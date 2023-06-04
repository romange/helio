// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <ares.h>
#include <netdb.h>

#include "base/logging.h"
#include "util/fibers/epoll_proactor.h"
#include "util/fibers/proactor_base.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif

namespace util {
namespace fb2 {

using namespace std;

namespace {

struct DnsResolveCallbackArgs {
  char* dest_ip = nullptr;
  std::error_code ec;
  bool done = false;  // Should we use optional<ec> above instead of the additional bool field?
};

struct AresChannelState {
  ProactorBase* proactor;
  detail::FiberInterface* fiber_ctx = nullptr;

  // I assume here that a single socket is opened during DNS resolution.
  ares_socket_t channel_sock = ARES_SOCKET_BAD;
  bool has_reads = false;
  bool has_writes = false;
  unsigned arm_index;
};

inline bool HasReads(uint32_t mask) {
  return mask & ProactorBase::EPOLL_IN;
}

inline bool HasWrites(uint32_t mask) {
  return mask & ProactorBase::EPOLL_OUT;
}

void UpdateSocketsCallback(void* arg, ares_socket_t socket_fd, int readable, int writable) {
  VLOG(1) << "sfd: " << socket_fd << " " << readable << "/" << writable;
  AresChannelState* state = (AresChannelState*)arg;

  uint32_t mask = 0;

  if (readable)
    mask |= ProactorBase::EPOLL_IN;
  if (writable)
    mask |= ProactorBase::EPOLL_OUT;

  if (state->channel_sock == ARES_SOCKET_BAD) {
    state->channel_sock = socket_fd;

    // TODO: to unify epoll management under a unified interface in ProactorBase.
    if (state->proactor->GetKind() == ProactorBase::EPOLL) {
      EpollProactor* epoll = (EpollProactor*)state->proactor;
      auto cb = [state](uint32_t event_mask, int err, EpollProactor* me) {
        state->has_writes = HasWrites(event_mask);
        state->has_reads = HasReads(event_mask);
        if (state->fiber_ctx) {
          detail::FiberActive()->ActivateOther(state->fiber_ctx);
        }
      };
      state->arm_index = epoll->Arm(socket_fd, move(cb), mask);
    } else {
      CHECK_EQ(state->proactor->GetKind(), ProactorBase::IOURING);
#ifdef __linux__
      UringProactor* uring = (UringProactor*)state->proactor;
      auto cb = [state](uint32_t event_mask) {
        state->has_writes = HasWrites(event_mask);
        state->has_reads = HasReads(event_mask);
        if (state->fiber_ctx) {
          detail::FiberActive()->ActivateOther(state->fiber_ctx);
        }
      };
      state->arm_index = uring->EpollAdd(socket_fd, move(cb), mask);
#endif
    }
  } else {
    CHECK_EQ(state->channel_sock, socket_fd);
    CHECK_EQ(mask, 0u);
    if (state->proactor->GetKind() == ProactorBase::EPOLL) {
      EpollProactor* epoll = (EpollProactor*)state->proactor;
      epoll->Disarm(socket_fd, state->arm_index);
    } else {
      CHECK_EQ(state->proactor->GetKind(), ProactorBase::IOURING);
#ifdef __linux__
      UringProactor* uring = (UringProactor*)state->proactor;
      uring->EpollDel(state->arm_index);
#endif
    }
  }
}

void DnsResolveCallback(void* ares_arg, int status, int timeouts, hostent* hostent) {
  VLOG(1) << "DnsResolve: " << status;

  CHECK(ares_arg != nullptr);

  auto* cb_args = static_cast<DnsResolveCallbackArgs*>(ares_arg);
  auto set_error = [&]() {
    cb_args->ec = make_error_code(errc::address_not_available);
    cb_args->done = true;
  };

  if (status != ARES_SUCCESS || hostent == nullptr) {
    return set_error();
  }

  if (hostent->h_addrtype != AF_INET) {
    // We currently only support IPv4
    return set_error();
  }

  char** addr = hostent->h_addr_list;
  if (addr == nullptr) {
    return set_error();
  }

  ares_inet_ntop(AF_INET, *addr, cb_args->dest_ip, INET_ADDRSTRLEN);
  cb_args->done = true;
}

void ProcessChannel(ares_channel channel, AresChannelState* state, DnsResolveCallbackArgs* args) {
  auto* myself = detail::FiberActive();
  state->fiber_ctx = myself;

  while (!args->done) {
    myself->Suspend();

    int write_sock = state->has_writes ? state->channel_sock : ARES_SOCKET_BAD;
    int read_sock = state->has_reads ? state->channel_sock : ARES_SOCKET_BAD;
    ares_process_fd(channel, read_sock, write_sock);
  }
  state->fiber_ctx = nullptr;
}

}  // namespace

error_code DnsResolve(string host, uint32_t wait_ms, char dest_ip[], ProactorBase* proactor) {
  DCHECK(ProactorBase::me() == proactor) << "must call from the proactor thread";

  VLOG(1) << "DnsResolveStart";

  AresChannelState state;
  state.proactor = proactor;

  ares_options options = {};
  options.sock_state_cb = &UpdateSocketsCallback;
  options.sock_state_cb_data = &state;

  ares_channel channel;
  CHECK_EQ(ares_init_options(&channel, &options, ARES_OPT_SOCK_STATE_CB), ARES_SUCCESS);

  DnsResolveCallbackArgs cb_args;
  cb_args.dest_ip = dest_ip;
  ares_gethostbyname(channel, host.c_str(), AF_INET, &DnsResolveCallback, &cb_args);

  ProcessChannel(channel, &state, &cb_args);
  ares_destroy(channel);

  return cb_args.ec;
}

}  // namespace fb2
}  // namespace util
