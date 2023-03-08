// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>
#include <liburing.h>
#include <netinet/in.h>
#include <poll.h>
#include <gperftools/profiler.h>

#include <boost/context/fiber.hpp>
#include <boost/intrusive/list.hpp>
#include <queue>

#include "base/init.h"
#include "base/logging.h"
#include "base/mpmc_bounded_queue.h"
#include "base/pthread_utils.h"
#include "examples/fiber.h"

ABSL_FLAG(int16_t, port, 8081, "Echo server port");
ABSL_FLAG(uint32_t, size, 512, "Message size");
ABSL_FLAG(uint16_t, threads, 0,
          "Number of connection threads. 0 to run as single threaded. "
          "Otherwise, the main thread accepts connections and worker threads are handling TCP "
          "connections");

namespace ctx = boost::context;
using namespace std;

namespace {

struct signal_state {
  struct Item {
    std::function<void(int)> cb;
  };

  Item signal_map[_NSIG];
};

signal_state* get_signal_state() {
  static signal_state state;

  return &state;
}

void SigAction(int signal, siginfo_t*, void*) {
  signal_state* state = get_signal_state();
  DCHECK_LT(signal, _NSIG);

  auto& item = state->signal_map[signal];

  if (item.cb) {
    item.cb(signal);
  } else {
    LOG(ERROR) << "Tangling signal handler " << signal;
  }
}

void RegisterSignal(std::initializer_list<uint16_t> l, std::function<void(int)> cb) {
  auto* state = get_signal_state();

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));

  if (cb) {
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = &SigAction;

    for (uint16_t val : l) {
      CHECK(!state->signal_map[val].cb) << "Signal " << val << " was already registered";
      state->signal_map[val].cb = cb;
      CHECK_EQ(0, sigaction(val, &sa, NULL));
    }
  } else {
    sa.sa_handler = SIG_DFL;

    for (uint16_t val : l) {
      CHECK(state->signal_map[val].cb) << "Signal " << val << " was already registered";
      state->signal_map[val].cb = nullptr;
      CHECK_EQ(0, sigaction(val, &sa, NULL));
    }
  }
}

using IoResult = int;
using namespace example;

struct CqeResult {
  int32_t res;
  uint32_t flags;
};

struct ResumeablePoint {
  detail::FiberInterface* cntx = nullptr;
  union {
    CqeResult* cqe_res;
    uint32_t next;
  };

  ResumeablePoint() : cqe_res(nullptr) {
  }
};

struct Worker {
  unique_ptr<base::mpmc_bounded_queue<int>> queue;
  pthread_t pid;

  Worker() : queue(new base::mpmc_bounded_queue<int>(32)){};
};

std::vector<Worker> workers;

thread_local std::vector<ResumeablePoint> suspended_list;
thread_local uint32_t next_rp_id = UINT32_MAX;

bool loop_exit = false;

uint32_t NextRp() {
  ResumeablePoint* rp = &suspended_list[next_rp_id];
  CHECK_LT(rp->next, suspended_list.size());
  uint32_t res = next_rp_id;
  next_rp_id = rp->next;

  return res;
}

CqeResult SuspendMyself(io_uring_sqe* sqe) {
  DVLOG(1) << "SuspendMyself";

  CqeResult cqe_result;
  uint32_t myindex = NextRp();
  sqe->user_data = myindex;

  auto& rp = suspended_list[myindex];
  rp.cqe_res = &cqe_result;

  detail::FiberInterface* fi = detail::FiberActive();
  rp.cntx = fi;

  fi->scheduler()->Preempt();

  return cqe_result;
}

// return 0 if succeeded.
int Recv(int fd, uint8_t* buf, size_t len, io_uring* ring) {
  DCHECK_GE(fd, 0);

  CqeResult cqe_result;
  size_t offs = 0;
  while (len > 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, fd, buf + offs, len, 0);
    cqe_result = SuspendMyself(sqe);

    if (cqe_result.res > 0) {
      CHECK_LE(uint32_t(cqe_result.res), len);
      len -= cqe_result.res;
      offs += cqe_result.res;
      continue;
    }

    if (cqe_result.res == 0) {
      return ECONNABORTED;
    }

    if (-cqe_result.res == EAGAIN) {
      continue;
    }

    return -cqe_result.res;
  }

  return 0;
}

int Send(int fd, const uint8_t* buf, size_t len, io_uring* ring) {
  CqeResult cqe_result;

  size_t offs = 0;
  while (len > 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, fd, buf + offs, len, MSG_NOSIGNAL);
    cqe_result = SuspendMyself(sqe);

    if (cqe_result.res >= 0) {
      CHECK_LE(uint32_t(cqe_result.res), len);
      len -= cqe_result.res;
      offs += cqe_result.res;
      continue;
    }

    return -cqe_result.res;
  }

  return 0;
}

void HandleSocket(io_uring* ring, int fd) {
  VLOG(1) << "Handle socket " << fd;
  uint32_t size = absl::GetFlag(FLAGS_size);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[size]);

  while (true) {
    DVLOG(1) << "Trying to recv from socket " << fd;
    int res = Recv(fd, buf.get(), size, ring);

    if (res > 0) {  // error
      break;
    }

    DVLOG(1) << "After recv from socket " << fd;
    res = Send(fd, buf.get(), size, ring);
    if (res > 0) {
      break;
    }
  }

  close(fd);
}

void AcceptFiber(io_uring* ring, int listen_fd) {
  LOG(INFO) << "AcceptFiber";

  CqeResult cqe_result;
  unsigned next_worker = 0;

  while (true) {
    int res = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      VLOG(1) << "Accepted fd " << res;
      if (!workers.empty()) {
        CHECK(workers[next_worker].queue->try_enqueue(res));
        next_worker = (next_worker + 1) % workers.size();
      } else {
        Fiber("conn", [ring, res] { HandleSocket(ring, res); }).Detach();
      }
    } else if (errno == EAGAIN) {
      io_uring_sqe* sqe = io_uring_get_sqe(ring);
      io_uring_prep_poll_add(sqe, listen_fd, POLLIN);

      // switch back to the main loop.
      DVLOG(1) << "Resuming to loop from AcceptFiber";
      cqe_result = SuspendMyself(sqe);

      DVLOG(1) << "Resuming AcceptFiber after loop";

      if ((cqe_result.res & (POLLERR | POLLHUP)) != 0) {
        LOG(ERROR) << "aborted " << cqe_result.res;
        loop_exit = true;
        break;
      }
    } else {
      LOG(ERROR) << "Error " << strerror(errno);
      loop_exit = true;
      break;
    }
  }
};

void RunEventLoop(int worker_id, io_uring* ring, detail::Scheduler* sched) {
  VLOG(1) << "RunEventLoop";
  DCHECK(!sched->HasReady());

  io_uring_cqe* cqe = nullptr;
  unsigned spins = 0;
  do {
    if (worker_id >= 0) {
      int fd = -1;
      while (workers[worker_id].queue->try_dequeue(fd)) {
        Fiber("conn", [ring, fd] { HandleSocket(ring, fd); }).Detach();

        DVLOG(1) << "After HandleSocket fd " << fd;
      }
    }
    int num_submitted = io_uring_submit(ring);
    CHECK_GE(num_submitted, 0);
    uint32_t head = 0;
    unsigned i = 0;
    io_uring_for_each_cqe(ring, head, cqe) {
      uint32_t index = cqe->user_data;
      CHECK_LT(index, suspended_list.size());

      auto* cqe_res = suspended_list[index].cqe_res;
      cqe_res->res = cqe->res;
      cqe_res->flags = cqe->flags;
      suspended_list[index].next = next_rp_id;
      next_rp_id = index;

      sched->MarkReady(suspended_list[index].cntx);
      ++i;
    }
    io_uring_cq_advance(ring, i);

    sched->ProcessSleep();

    if (sched->HasReady()) {
      do {
        detail::FiberInterface* fi = sched->PopReady();
        auto fc = fi->SwitchTo();
        DCHECK(!fc);
      } while (sched->HasReady());
      continue;
    }

    if (spins++ < 20)
      continue;

    struct __kernel_timespec ts {
      .tv_sec = 0, .tv_nsec = 100000
    };
    io_uring_wait_cqe_timeout(ring, &cqe, &ts);
    spins = 0;
    // io_uring_wait_cqe_timeout(ring, &cqe, nullptr);
  } while (!loop_exit);

  VLOG(1) << "Exit RunEventLoop";
}

void SetupIORing(io_uring* ring) {
  io_uring_params params;
  memset(&params, 0, sizeof(params));
  CHECK_EQ(0, io_uring_queue_init_params(512, ring, &params));
  int res = io_uring_register_ring_fd(ring);
  VLOG_IF(1, res < 0) << "io_uring_register_ring_fd failed: " << -res;

  suspended_list.resize(1024);
  for (size_t i = 0; i < suspended_list.size(); ++i) {
    suspended_list[i].next = i + 1;
  }
  next_rp_id = 0;
}

int SetupListener(uint16_t port) {
  int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  CHECK_GE(listen_fd, 0);

  const int val = 1;
  CHECK_EQ(0, setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)));

  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  int res = bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
  CHECK_EQ(0, res);
  res = listen(listen_fd, 32);
  CHECK_EQ(0, res);

  return listen_fd;
}

void WorkerThread(unsigned index) {
  io_uring ring;

  SetupIORing(&ring);
  SetCustomScheduler([&](detail::Scheduler* sched) { RunEventLoop(index, &ring, sched); });

  ctx::fiber_context fc = detail::FiberActive()->scheduler()->Preempt();

  DCHECK(!fc);

  VLOG(1) << "Exiting worker " << index << " " << bool(fc);
}

}  // namespace

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);
  // ProfilerEnable();

  int listen_fd = -1;

  RegisterSignal({SIGINT, SIGTERM}, [&](int signal) {
    LOG(INFO) << "Exiting on signal " << strsignal(signal);
    shutdown(listen_fd, SHUT_RDWR);
  });

  // setup iouring
  io_uring ring;

  SetupIORing(&ring);

  // setup listening socket
  uint16_t port = absl::GetFlag(FLAGS_port);

  listen_fd = SetupListener(port);

  SetCustomScheduler([&ring](detail::Scheduler* sched) { RunEventLoop(-1, &ring, sched); });

  uint16_t num_workers = absl::GetFlag(FLAGS_threads);
  workers.resize(num_workers);
  LOG(INFO) << "Num threads " << num_workers;

  cpu_set_t cps;
  CPU_ZERO(&cps);

  for (size_t i = 0; i < num_workers; ++i) {
    string nm = absl::StrCat("worker", i);
    pthread_t tid = base::StartThread(nm.c_str(), [i] { WorkerThread(i); });

    CPU_SET(i, &cps);
    pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cps);
    CPU_CLR(i, &cps);

    workers[i].pid = tid;
  }

  Fiber accept_fb("accept", [&] { AcceptFiber(&ring, listen_fd); });
  accept_fb.Join();

  loop_exit = true;
  // detail::FiberActive()->scheduler()->DestroyTerminated();
  for (size_t i = 0; i < num_workers; ++i) {
    pthread_join(workers[i].pid, nullptr);
  }
  io_uring_queue_exit(&ring);
  // terminated_queue.clear();  // TODO: there is a resource leak here.

  return 0;
}
