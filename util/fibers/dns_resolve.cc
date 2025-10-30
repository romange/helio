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
  fb2::detail::FiberInterface* cntx = nullptr;
  std::error_code ec;
  bool done = false;  // Should we use optional<ec> above instead of the additional bool field?
};

struct AresSocketState {
  unsigned mask;
  bool removed = false;  // Used to indicate that the socket was removed, without modifying the map
  ptrdiff_t arm_index;
};

struct AresChannelState {
  ProactorBase* proactor;
  fb2::CondVarAny cond;

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
          state->cond.notify_one();
        };
        socket_state.arm_index = epoll->Arm(socket_fd, std::move(cb), mask);
      } else {
        CHECK_EQ(state->proactor->GetKind(), ProactorBase::IOURING);
#ifdef __linux__
        UringProactor* uring = (UringProactor*)state->proactor;
        auto cb = [state](uint32_t event_mask) {
          VLOG(2) << "ArmCb: " << event_mask << " " << state->sockets_state.size();
          state->cond.notify_one();
        };

        // multishot epoll supported from 5.13 kernel onwards. we do not use it for now
        // to keep the compatibility with older kernels.
        socket_state.arm_index = uring->EpollAdd(socket_fd, std::move(cb), mask, false);
        DVLOG(1) << "EpollAdd " << socket_fd << ", mask: " << mask
                 << " index: " << socket_state.arm_index;
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
  VLOG(1) << "DnsResolveCallback: " << status << " " << timeouts << " " << (res ? res->name : "");

  if (cb_args->cntx) {
    fb2::detail::FiberActive()->ActivateOther(cb_args->cntx);
  }

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
  while (!args->done) {
    // It's important to set and reset fiber_ctx close to Suspend, to avoid the case
    // where EPOLL callbacks wake up the fiber in the wrong place.
    // ares_process_fd calls helio code that in turn can suspend a fiber as well.
    timeval timeout;
    timeval* timeout_result = ares_timeout(channel, nullptr, &timeout);

    fb2::NoOpLock lk;
    if (timeout_result) {
      uint64_t ms = timeout_result->tv_sec * 1000 + timeout_result->tv_usec / 1000;
      VLOG(2) << "blocking on timeout " << ms << "ms";
      cv_status st = state->cond.wait_for(lk, chrono::milliseconds(ms));
      if (st == cv_status::timeout) {
        VLOG_IF(1, !state->sockets_state.empty())
            << "Timed out on waiting for fd: " << state->sockets_state.begin()->first;
        // according to https://c-ares.org/docs/ares_process_fd.html
        // in case of timeouts we pass ARES_SOCKET_BAD to ares_process_fd.
        ares_process_fd(channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
        continue;
      }
    } else {
      state->cond.wait(lk);
    }

    for (const auto& [socket_fd, socket_state] : state->sockets_state) {
      int read_fd = HasReads(socket_state.mask) ? socket_fd : ARES_SOCKET_BAD;
      int write_fd = HasWrites(socket_state.mask) ? socket_fd : ARES_SOCKET_BAD;
      VLOG(2) << "ares_process_fd: " << read_fd << " " << write_fd;
      ares_process_fd(channel, read_fd, write_fd);
    }
  }
}

ares_channel* g_channel = nullptr;

}  // namespace

void InitDnsResolver(uint32_t timeout_ms) {
  CHECK(!g_channel) << "Already initialized";
  constexpr int kOptions = ARES_OPT_LOOKUPS | ARES_OPT_TIMEOUTMS | ARES_OPT_EVENT_THREAD;

  ares_options options = {};
  char lookups[] = "fb";
  options.lookups = lookups;  // hosts file first, then DNS
  options.timeout = timeout_ms;

  g_channel = new ares_channel;
  CHECK_EQ(ares_init_options(g_channel, &options, kOptions), ARES_SUCCESS);
}

// TODO: Replace DnsResolve with much simpler DnsResolve2.
error_code DnsResolve(const string& host, char dest_ip[]) {
  CHECK(g_channel) << "Must call InitDnsResolver before DnsResolve2";
  ares_channel* channel = g_channel;

  ares_addrinfo_hints hints{ARES_AI_CANONNAME, AF_UNSPEC, 0, 0};

  DnsResolveCallbackArgs cb_args;
  cb_args.dest_ip = dest_ip;
  cb_args.cntx = fb2::detail::FiberActive();
  ares_getaddrinfo(*channel, host.c_str(), NULL, &hints, DnsResolveCallback, &cb_args);
  fb2::detail::FiberActive()->Suspend();

  return cb_args.ec;
}

error_code DnsResolve(const string& host, uint32_t wait_ms, char dest_ip[],
                      ProactorBase* proactor) {
  DCHECK(ProactorBase::me() == proactor) << "must call from the proactor thread";
  VLOG(1) << "DnsResolveStart " << host;

  AresChannelState state;
  state.proactor = proactor;

  ares_options options = {};
  options.sock_state_cb = &UpdateSocketsCallback;
  options.sock_state_cb_data = &state;
  char lookups[] = "fb";
  options.lookups = lookups;  // hosts file first, then DNS

  // set timeout
  options.timeout = wait_ms;
  // TODO: use options.qcache_max_ttl once we be able to reuse cares channel.

  ares_channel channel;
  constexpr int kOptions = ARES_OPT_SOCK_STATE_CB | ARES_OPT_LOOKUPS | ARES_OPT_TIMEOUTMS;
  CHECK_EQ(ares_init_options(&channel, &options, kOptions), ARES_SUCCESS);

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
