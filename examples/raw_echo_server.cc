// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <liburing.h>
#include <netinet/in.h>
#include <poll.h>

#include <boost/context/continuation.hpp>

#include "base/init.h"
#include "base/logging.h"

ABSL_FLAG(int16_t, port, 8081, "Echo server port");
ABSL_FLAG(uint32_t, size, 512, "Message size");

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

struct CqeResult {
  int32_t res;
  uint32_t flags;
};

struct ResumeablePoint {
  ctx::continuation cont;
  union {
    CqeResult* cqe_res;
    uint32_t next;
  };

  ResumeablePoint() : cqe_res(nullptr) {
  }
};

struct Socket {
  int fd;
  ctx::continuation cont;
};

std::vector<ResumeablePoint> resume_points;
std::vector<Socket> sockets;

uint32_t next_rp_id = UINT32_MAX;
bool loop_exit = false;

uint32_t NextRp() {
  ResumeablePoint* rp = &resume_points[next_rp_id];
  CHECK_LT(rp->next, resume_points.size());
  uint32_t res = next_rp_id;
  next_rp_id = rp->next;

  return res;
}

int Recv(int fd, uint8_t* buf, size_t len, io_uring* ring, ctx::continuation* caller) {
  CqeResult cqe_result;
  size_t offs = 0;
  while (len > 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, fd, buf + offs, len, 0);
    uint32_t myindex = NextRp();
    sqe->user_data = myindex;

    auto& rp = resume_points[myindex];
    rp.cqe_res = &cqe_result;
    *caller = caller->resume_with([&](ctx::continuation&& myself) {
      rp.cont = std::move(myself);
      return ctx::continuation{};  // the caller won't have a continuation to switch back to.
    });

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

int Send(int fd, const uint8_t* buf, size_t len, io_uring* ring, ctx::continuation* caller) {
  CqeResult cqe_result;

  size_t offs = 0;
  while (len > 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, fd, buf + offs, len, MSG_NOSIGNAL);
    uint32_t myindex = NextRp();
    sqe->user_data = myindex;

    auto& rp = resume_points[myindex];
    rp.cqe_res = &cqe_result;
    *caller = caller->resume_with([&](ctx::continuation&& myself) {
      rp.cont = std::move(myself);
      return ctx::continuation{};  // the caller won't have a continuation to switch back to.
    });

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

ctx::continuation HandleSocket(io_uring* ring, int fd, ctx::continuation&& caller) {
  uint32_t size = absl::GetFlag(FLAGS_size);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[size]);

  while (true) {
    int res = Recv(fd, buf.get(), size, ring, &caller);

    if (res > 0) {
      break;
    }

    res = Send(fd, buf.get(), size, ring, &caller);
    if (res > 0) {
      break;
    }
  }

  close(fd);
  return std::move(caller);
}

ctx::continuation AcceptFiber(io_uring* ring, int listen_fd, ctx::continuation&& caller) {
  CqeResult cqe_result;

  while (true) {
    int res = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      VLOG(1) << "Accepted fd " << res;
      sockets.emplace_back();
      sockets.back().fd = res;
      ctx::continuation cont = ctx::callcc(
          [&](ctx::continuation&& caller) { return HandleSocket(ring, res, std::move(caller)); });
      CHECK(!cont);
    } else if (errno == EAGAIN) {
      io_uring_sqe* sqe = io_uring_get_sqe(ring);
      io_uring_prep_poll_add(sqe, listen_fd, POLLIN);

      uint32_t myindex = NextRp();
      sqe->user_data = myindex;

      auto& rp = resume_points[myindex];
      rp.cqe_res = &cqe_result;
      caller = caller.resume_with([&](ctx::continuation&& myself) {
        rp.cont = std::move(myself);
        return ctx::continuation{};
      });

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

  return std::move(caller);
};

}  // namespace

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  RegisterSignal({SIGINT, SIGTERM}, [](int signal) {
    LOG(INFO) << "Exiting on signal " << strsignal(signal);
    loop_exit = true;
  });

  // setup iouring
  io_uring ring;
  io_uring_params params;
  memset(&params, 0, sizeof(params));
  CHECK_EQ(0, io_uring_queue_init_params(512, &ring, &params));
  int res = io_uring_register_ring_fd(&ring);
  VLOG_IF(1, res < 0) << "io_uring_register_ring_fd failed: " << -res;

  resume_points.resize(1024);
  for (size_t i = 0; i < resume_points.size(); ++i) {
    resume_points[i].next = i + 1;
  }
  next_rp_id = 0;

  // setup listening socket
  uint16_t port = absl::GetFlag(FLAGS_port);

  int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  CHECK_GE(listen_fd, 0);

  const int val = 1;
  CHECK_EQ(0, setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)));

  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  res = bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
  CHECK_EQ(0, res);
  res = listen(listen_fd, 32);
  CHECK_EQ(0, res);

  ctx::continuation accept_cont = ctx::callcc(
      [&](ctx::continuation&& caller) { return AcceptFiber(&ring, listen_fd, std::move(caller)); });

  io_uring_cqe* cqe = nullptr;

  vector<ctx::continuation> active;

  while (!loop_exit) {
    int num_submitted = io_uring_submit(&ring);
    CHECK_GE(num_submitted, 0);
    uint32_t head = 0;
    unsigned i = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      uint32_t index = cqe->user_data;
      CHECK_LT(index, resume_points.size());

      auto* cqe_res = resume_points[index].cqe_res;
      cqe_res->res = cqe->res;
      cqe_res->flags = cqe->flags;
      resume_points[index].next = next_rp_id;
      next_rp_id = index;

      // switch to that fiber immediately. we probably do not want to do that when
      // we have a scheduler.
      active.push_back(std::move(resume_points[index].cont));
      ++i;
    }
    io_uring_cq_advance(&ring, i);

    if (i) {
      for (auto& cont : active) {
        std::move(cont).resume();
      }
      active.clear();
      continue;
    }

    io_uring_wait_cqe_nr(&ring, &cqe, 1);
  }

  close(listen_fd);
  io_uring_queue_exit(&ring);

  return 0;
}