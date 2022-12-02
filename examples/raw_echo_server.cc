// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <liburing.h>
#include <netinet/in.h>
#include <poll.h>

#include <boost/context/fiber.hpp>
#include <boost/intrusive/list.hpp>
#include <queue>

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

typedef boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink> >
    FiberContextHook;

class BaseFiberCtx {
 public:
  string name;

  virtual ~BaseFiberCtx();

  FiberContextHook list_hook{};  // 16 bytes

  void StartMain();

  void Resume();
  void Suspend();
  void SetReady();

  bool IsDefined() const {
    return bool(c_);
  }

 protected:
  void Terminate();

  // holds its own fiber_context when it's not active.
  // the difference between fiber_context and continuation is that continuation is launched
  // straight away via callcc and fiber is created without switching to it.
  // TODO: I still do not know how continuation_fcontext and fiber_fcontext achieve this
  // difference because their code looks very similar except some renaming.
  ctx::fiber_context c_;  // 8 bytes
};

thread_local BaseFiberCtx* active_fb_ctx = nullptr;
thread_local BaseFiberCtx* main_ctx = nullptr;

typedef boost::intrusive::list<
    BaseFiberCtx,
    boost::intrusive::member_hook<BaseFiberCtx, FiberContextHook, &BaseFiberCtx::list_hook>,
    boost::intrusive::constant_time_size<false> >
    ContextQueue;

thread_local ContextQueue ready_queue;
thread_local ContextQueue terminated_queue;

constexpr size_t kSizeOfCtx = sizeof(BaseFiberCtx);  // because of the virtual +8 bytes.

template <typename Fn> class FiberCtx : public BaseFiberCtx {
 public:
  template <typename StackAlloc>
  FiberCtx(const boost::context::preallocated& palloc, StackAlloc&& salloc, Fn&& fn)
      : fn_(std::forward<Fn>(fn)) {
    c_ =
        ctx::fiber_context(std::allocator_arg, palloc, std::forward<StackAlloc>(salloc),
                           [this](ctx::fiber_context&& caller) { return run_(std::move(caller)); });
  }

 private:
  ctx::fiber_context run_(ctx::fiber_context&& c) {
    {
      // fn and tpl must be destroyed before calling terminate()
      auto fn = std::move(fn_);
      fn();
    }

    Terminate();

    // Only main_cntx reaches this point because all the rest should switch to main_ctx and never
    // switch back.
    DCHECK(this == main_ctx);
    DCHECK(ready_queue.empty());
    DCHECK(c);
    return std::move(c);
  }

  Fn fn_;
};

template <typename StackAlloc, typename Fn>
static FiberCtx<Fn>* MakeFiberCtx(StackAlloc&& salloc, Fn&& fn) {
  ctx::stack_context sctx = salloc.allocate();
  // reserve space for control structure
  uintptr_t storage =
      (reinterpret_cast<uintptr_t>(sctx.sp) - sizeof(FiberCtx<Fn>)) & ~static_cast<uintptr_t>(0xff);
  uintptr_t stack_bottom = reinterpret_cast<uintptr_t>(sctx.sp) - static_cast<uintptr_t>(sctx.size);
  const std::size_t size = storage - stack_bottom;
  void* st_ptr = reinterpret_cast<void*>(storage);

  // placement new of context on top of fiber's stack
  FiberCtx<Fn>* fctx =
      new (st_ptr) FiberCtx<Fn>{ctx::preallocated{st_ptr, size, sctx},
                                std::forward<StackAlloc>(salloc), std::forward<Fn>(fn)};
  return fctx;
}

BaseFiberCtx::~BaseFiberCtx() {
  DVLOG(1) << "~BaseFiberCtx";
}

void BaseFiberCtx::Suspend() {
  main_ctx->Resume();
}

void BaseFiberCtx::Terminate() {
  DCHECK(this == active_fb_ctx);
  DCHECK(!list_hook.is_linked());


  if (this != main_ctx) {
    terminated_queue.push_back(*this);
    main_ctx->Resume();
  }
}

void BaseFiberCtx::Resume() {
  BaseFiberCtx* prev = this;

  std::swap(active_fb_ctx, prev);

  // pass pointer to the context that resumes `this`
  std::move(c_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->c_);

    prev->c_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

void BaseFiberCtx::StartMain() {
  DCHECK(!active_fb_ctx);
  active_fb_ctx = this;

  std::move(c_).resume();
}

void BaseFiberCtx::SetReady() {
  DCHECK(!list_hook.is_linked());
  ready_queue.push_back(*this);
}

template <typename StackAlloc, typename Fn>
BaseFiberCtx* LaunchFiber(std::allocator_arg_t, StackAlloc&& salloc, Fn&& fn) {
  auto* base_ctx = MakeFiberCtx(std::forward<StackAlloc>(salloc), std::forward<Fn>(fn));
  base_ctx->SetReady();
  return base_ctx;
}

template <typename Fn> BaseFiberCtx* LaunchFiber(Fn&& fn) {
  return LaunchFiber(std::allocator_arg, ctx::fixedsize_stack(), std::forward<Fn>(fn));
}

struct ResumeablePoint {
  BaseFiberCtx* cntx = nullptr;
  union {
    CqeResult* cqe_res;
    uint32_t next;
  };

  ResumeablePoint() : cqe_res(nullptr) {
  }
};

std::vector<ResumeablePoint> suspended_list;

uint32_t next_rp_id = UINT32_MAX;
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
  rp.cntx = active_fb_ctx;

  DCHECK(main_ctx);
  main_ctx->Resume();

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

    DCHECK(main_ctx);

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
  DVLOG(1) << "Handle socket";
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
  VLOG(1) << "AcceptFiber";

  CqeResult cqe_result;

  while (true) {
    int res = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      VLOG(1) << "Accepted fd " << res;
      // sockets.emplace_back();
      // sockets.back().fd = res;

      // new fiber
      BaseFiberCtx* cntx = LaunchFiber([ring, res] { HandleSocket(ring, res); });
      cntx->name = "handlesock";

      DVLOG(1) << "After HandleSocket fd " << res;
    } else if (errno == EAGAIN) {
      io_uring_sqe* sqe = io_uring_get_sqe(ring);
      io_uring_prep_poll_add(sqe, listen_fd, POLLIN);

      // switch back to the main loop.
      DVLOG(1) << "Resuming to main from AcceptFiber";
      cqe_result = SuspendMyself(sqe);

      DCHECK(main_ctx);
      DVLOG(1) << "Resuming AcceptFiber after main";

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

void RunReadyFbs() {
  while (!ready_queue.empty()) {
    BaseFiberCtx* cntx = &ready_queue.front();
    ready_queue.pop_front();

    cntx->Resume();
  }
}

void RunEventLoop(io_uring* ring) {
  VLOG(1) << "RunEventLoop";

  io_uring_cqe* cqe = nullptr;
  RunReadyFbs();
  while (!loop_exit) {
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

      suspended_list[index].cntx->SetReady();
      ++i;
    }
    io_uring_cq_advance(ring, i);

    if (i) {
      RunReadyFbs();
      continue;
    }

    io_uring_wait_cqe_nr(ring, &cqe, 1);
  }
}

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

  suspended_list.resize(1024);
  for (size_t i = 0; i < suspended_list.size(); ++i) {
    suspended_list[i].next = i + 1;
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

  main_ctx = MakeFiberCtx(ctx::fixedsize_stack(), [&ring] { RunEventLoop(&ring); });
  main_ctx->name = "main";
  DCHECK(main_ctx->IsDefined());

  DCHECK(ready_queue.empty());
  BaseFiberCtx* cntx = LaunchFiber([&] { AcceptFiber(&ring, listen_fd); });
  cntx->name = "accept";
  DCHECK(!ready_queue.empty());

  main_ctx->StartMain();  // blocking

  close(listen_fd);
  io_uring_queue_exit(&ring);
  terminated_queue.clear();  // TODO: there is a resource leak here.

  return 0;
}
