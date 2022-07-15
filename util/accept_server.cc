// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/accept_server.h"

#include <signal.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/listener_interface.h"
#include "util/proactor_pool.h"

namespace util {

using namespace boost;
using namespace std;

AcceptServer::AcceptServer(ProactorPool* pool, bool break_on_int) : pool_(pool), ref_bc_(0) {
  if (break_on_int) {
    ProactorBase* proactor = pool_->GetNextProactor();
    proactor->RegisterSignal({SIGINT, SIGTERM}, [this](int signal) {
      LOG(INFO) << "Exiting on signal " << strsignal(signal);
      if (on_break_hook_) {
        on_break_hook_();
      }
      BreakListeners();
    });
  }
}

AcceptServer::~AcceptServer() {
  list_interface_.clear();
}

void AcceptServer::Run() {
  if (!list_interface_.empty()) {
    ref_bc_.Add(list_interface_.size());

    for (auto& lw : list_interface_) {
      ProactorBase* proactor = lw->socket()->proactor();
      proactor->Dispatch([li = lw.get(), this] {
        li->RunAcceptLoop();
        ref_bc_.Dec();
      });
    }
  }
  was_run_ = true;
}

// If wait is false - does not wait for the server to stop.
// Then you need to run Wait() to wait for proper shutdown.
void AcceptServer::Stop(bool wait) {
  VLOG(1) << "AcceptServer::Stop";

  BreakListeners();
  if (wait)
    Wait();
}

void AcceptServer::Wait() {
  VLOG(1) << "AcceptServer::Wait";
  if (was_run_) {
    ref_bc_.Wait();
    VLOG(1) << "AcceptServer::Wait completed";
  } else {
    CHECK(list_interface_.empty()) << "Must Call AcceptServer::Run() after adding listeners";
  }
}

// Returns the port number to which the listener was bound.
unsigned short AcceptServer::AddListener(unsigned short port, ListenerInterface* lii) {
  error_code ec = AddListener(nullptr, port, lii);
  CHECK(!ec) << "Could not open port " << port << " " << ec << "/" << ec.message();

  auto ep = lii->socket()->LocalEndpoint();
  return ep.port();
}

error_code AcceptServer::AddListener(const char* bind_addr, uint16_t port,
                                     ListenerInterface* listener) {
  CHECK(listener && !listener->socket());

  char str_port[16];
  struct addrinfo hints, *servinfo;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE; /* Tuned for binding, see man getaddrinfo. */

  absl::numbers_internal::FastIntToBuffer(port, str_port);
  int res = getaddrinfo(bind_addr, str_port, &hints, &servinfo);
  if (res != 0) {
    const char* errmsg = gai_strerror(res);
    LOG(ERROR) << "Error resolving address " << bind_addr << ": " << errmsg;
    return make_error_code(errc::address_not_available);
  }
  CHECK(servinfo);

  ProactorBase* next = pool_->GetNextProactor();

  unique_ptr<LinuxSocketBase> fs{next->CreateSocket()};

  error_code ec;
  bool success = false;
  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    ec = fs->Create();
    if (ec)
      break;
    ec = listener->ConfigureServerSocket(fs->native_handle());
    if (ec)
      break;

    ec = fs->Listen(p->ai_addr, p->ai_addrlen, backlog_);
    if (!ec) {
      success = true;
      break;
    }
  }

  freeaddrinfo(servinfo);

  if (success) {
    listener->RegisterPool(pool_);
    listener->sock_ = std::move(fs);
    list_interface_.emplace_back(listener);
  }

  return ec;
}

error_code AcceptServer::AddUDSListener(const char* path, ListenerInterface* listener) {
  CHECK(listener && !listener->socket());

  ProactorBase* next = pool_->GetNextProactor();
  unique_ptr<LinuxSocketBase> fs{next->CreateSocket()};

  error_code ec = next->Await([&] {
    error_code ec = fs->Create(AF_UNIX);
    if (ec)
      return ec;

    ec = listener->ConfigureServerSocket(fs->native_handle());
    if (ec)
      return ec;

    return fs->ListenUDS(path, backlog_);
  });

  if (!ec) {
    listener->RegisterPool(pool_);
    listener->sock_ = std::move(fs);
    list_interface_.emplace_back(listener);
  }

  return ec;
}

void AcceptServer::BreakListeners() {
  for (auto& lw : list_interface_) {
    ProactorBase* proactor = lw->socket()->proactor();
    proactor->Dispatch([sock = lw->socket()] { sock->Shutdown(SHUT_RDWR); });
  }
  VLOG(1) << "AcceptServer::BreakListeners finished";
}

}  // namespace util
