// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <ares.h>
#include <netdb.h>
#include <sys/socket.h>

#include "base/logging.h"
#include "base/stl_util.h"
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

struct AresSocketState {
  unsigned mask;
  unsigned arm_index;
  bool removed = false;  // Used to indicate that the socket was removed, without modifying the map
};

struct AresChannelState {
  ProactorBase* proactor;
  detail::FiberInterface* fiber_ctx = nullptr;

  absl::flat_hash_map<ares_socket_t, AresSocketState> sockets_state;
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

  if (mask == 0) {
    auto it = state->sockets_state.find(socket_fd);
    CHECK(it != state->sockets_state.end());
    // TODO: to unify epoll management under a unified interface in ProactorBase.
    if (state->proactor->GetKind() == ProactorBase::EPOLL) {
      EpollProactor* epoll = (EpollProactor*)state->proactor;
      epoll->Disarm(socket_fd, it->second.arm_index);
    } else {
      CHECK_EQ(state->proactor->GetKind(), ProactorBase::IOURING);
#ifdef __linux__
      UringProactor* uring = (UringProactor*)state->proactor;
      uring->EpollDel(it->second.arm_index);
#endif
    }
    it->second.removed = true;
  } else {
    auto [it, inserted] = state->sockets_state.try_emplace(socket_fd);

    if (inserted || it->second.removed) {
      AresSocketState& socket_state = it->second;
      socket_state.mask = mask;
      socket_state.removed = false;

      if (state->proactor->GetKind() == ProactorBase::EPOLL) {
        EpollProactor* epoll = (EpollProactor*)state->proactor;
        auto cb = [state](uint32_t event_mask, int err, EpollProactor* me) {
          if (state->fiber_ctx) {
            ActivateSameThread(detail::FiberActive(), state->fiber_ctx);
          }
        };
        socket_state.arm_index = epoll->Arm(socket_fd, std::move(cb), mask);
      } else {
        CHECK_EQ(state->proactor->GetKind(), ProactorBase::IOURING);
#ifdef __linux__
        UringProactor* uring = (UringProactor*)state->proactor;
        auto cb = [state](uint32_t event_mask) {
          VLOG(2) << "ArmCb: " << event_mask << " " << state->fiber_ctx << " "
                  << state->sockets_state.size();
          if (state->fiber_ctx) {
            ActivateSameThread(detail::FiberActive(), state->fiber_ctx);
          }
        };
        VLOG(1) << "EpollAdd " << socket_fd << ", mask: " << mask;
        socket_state.arm_index = uring->EpollAdd(socket_fd, std::move(cb), mask);
#endif
      }
    } else {
      VLOG(1) << "Skipped updating the state for " << socket_fd;
    }
  }
}

void DnsResolveCallback(void* ares_arg, int status, int timeouts, struct ares_addrinfo* res) {
  auto* cb_args = static_cast<DnsResolveCallbackArgs*>(ares_arg);
  cb_args->done = true;
  VLOG(1) << "DnsResolveCallback: " << status << " " << timeouts << " " << res->nodes;

  if (status != ARES_SUCCESS || res->nodes == nullptr) {
    cb_args->ec = make_error_code(errc::address_not_available);
    return;
  }

  auto* node = res->nodes;

  // If IpV4 is available, prefer it
  if (node->ai_family == AF_INET6 && node->ai_next != nullptr &&
      node->ai_next->ai_family == AF_INET)
    node = node->ai_next;

  switch (node->ai_family) {
    case AF_INET: {
      auto* addr_in = reinterpret_cast<sockaddr_in*>(node->ai_addr);
      ares_inet_ntop(AF_INET, &addr_in->sin_addr, cb_args->dest_ip, INET6_ADDRSTRLEN);
      break;
    }
    case AF_INET6: {
      auto* addr_in6 = reinterpret_cast<sockaddr_in6*>(node->ai_addr);
      ares_inet_ntop(AF_INET6, &addr_in6->sin6_addr, cb_args->dest_ip, INET6_ADDRSTRLEN);
      break;
    }
    default:
      cb_args->ec = make_error_code(errc::invalid_argument);
  }
}

void ProcessChannel(ares_channel channel, AresChannelState* state, DnsResolveCallbackArgs* args) {
  auto* myself = detail::FiberActive();

  while (!args->done) {
    // It's important to set and reset fiber_ctx close to Suspend, to avoid the case
    // where EPOLL callbacks wake up the fiber in the wrong place.
    // ares_process_fd calls helio code that in turn can suspend a fiber as well.
    state->fiber_ctx = myself;
    myself->Suspend();
    state->fiber_ctx = nullptr;

    for (const auto& [socket_fd, socket_state] : state->sockets_state) {
      int read_fd = HasReads(socket_state.mask) ? socket_fd : ARES_SOCKET_BAD;
      int write_fd = HasWrites(socket_state.mask) ? socket_fd : ARES_SOCKET_BAD;
      VLOG(2) << "ares_process_fd: " << read_fd << " " << write_fd;
      ares_process_fd(channel, read_fd, write_fd);
    }
  }
}

}  // namespace

error_code DnsResolve(const string& host, uint32_t wait_ms, char dest_ip[],
                      ProactorBase* proactor) {
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

  // Same hints as for  hostentares_gethostbyname
  ares_addrinfo_hints hints{ARES_AI_CANONNAME, AF_UNSPEC, 0, 0};
  ares_getaddrinfo(channel, host.c_str(), nullptr, &hints, &DnsResolveCallback, &cb_args);

  ProcessChannel(channel, &state, &cb_args);
  ares_destroy(channel);

  return cb_args.ec;
}

}  // namespace fb2
}  // namespace util
