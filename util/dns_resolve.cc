// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/dns_resolve.h"

#include <ares.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "base/logging.h"
#include "util/fibers/fibers_ext.h"

namespace util {
namespace {

using namespace std;

struct DnsResolveCallbackArgs {
  char* dest_ip = nullptr;
  std::error_code ec;
  bool done = false;  // Should we use optional<ec> above instead of the additional bool field?
};

void UpdateSocketsCallback(void* arg, ares_socket_t socket_fd, int readable, int writable) {
  int epoll_fd = (int64_t)arg;

  struct epoll_event ev;
  ev.data.fd = socket_fd;
  ev.events = 0;
  if (readable) {
    ev.events |= EPOLLIN;
  }
  if (writable) {
    ev.events |= EPOLLOUT;
  }

  if (ev.events != 0) {
    int ctl_result = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev);
    if (ctl_result < 0) {
      CHECK_EQ(errno, EEXIST);
      CHECK_EQ(epoll_ctl(epoll_fd, EPOLL_CTL_MOD, socket_fd, &ev), 0);
    }
  } else {
    CHECK_EQ(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, &ev), 0);
  }
}

void DnsResolveCallback(void* ares_arg, int status, int timeouts, hostent* hostent) {
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

void ProcessChannel(ares_channel channel, int epoll_fd, DnsResolveCallbackArgs& args) {
  while (!args.done) {
    std::array<epoll_event, 16> events = {};
    // TODO: Use fibers scheduler here.
    int nfds = epoll_wait(epoll_fd, events.data(), events.size(), -1);
    CHECK_NE(nfds, 0) << "epoll_wait() must return non-zero with infinite timeout";

    for (const auto& event : events) {
      ares_socket_t read_sock = ARES_SOCKET_BAD;
      if (event.events & EPOLLIN) {
        read_sock = event.data.fd;
      }

      ares_socket_t write_sock = ARES_SOCKET_BAD;
      if (event.events & EPOLLOUT) {
        read_sock = event.data.fd;
      }

      ares_process_fd(channel, read_sock, write_sock);
    }
  }
}

}  // namespace

error_code DnsResolve(const char* dns, uint32_t wait_ms, char dest_ip[]) {
  int epoll_fd = epoll_create1(0);
  CHECK_GE(epoll_fd, 0);

  ares_options options = {};
  options.sock_state_cb = &UpdateSocketsCallback;
  options.sock_state_cb_data = (void*)(int64_t)epoll_fd;

  ares_channel channel;
  CHECK_EQ(ares_init_options(&channel, &options, ARES_OPT_SOCK_STATE_CB), ARES_SUCCESS);

  DnsResolveCallbackArgs cb_args;
  cb_args.dest_ip = dest_ip;
  ares_gethostbyname(channel, dns, AF_INET, &DnsResolveCallback, &cb_args);

  ProcessChannel(channel, epoll_fd, cb_args);
  ares_destroy(channel);

  close(epoll_fd);

  return cb_args.ec;
}

}  // namespace util
