// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/accept_server.h"

#include <signal.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"

#include "util/listener_interface.h"
#include "util/fiber_socket_base.h"
#include "util/proactor_pool.h"

namespace util {

using namespace boost;
using namespace std;

AcceptServer::AcceptServer(ProactorPool* pool, bool break_on_int)
    : pool_(pool), ref_bc_(0) {
  if (break_on_int) {
    ProactorBase* proactor = pool_->GetNextProactor();
    proactor->RegisterSignal({SIGINT, SIGTERM}, [this](int signal) {
      LOG(INFO) << "Exiting on signal " << signal;
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
      ProactorBase* proactor = lw->sock_->proactor();
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
  CHECK(lii && !lii->sock_);

  // We can not allow dynamic listener additions because listeners_ might reallocate.
  CHECK(!was_run_);

  ProactorBase* next = pool_->GetNextProactor();

  // TODO: to think about FiberSocket creation.
  std::unique_ptr<LinuxSocketBase> fs{next->CreateSocket()};
  uint32_t sock_opt_mask = lii->GetSockOptMask();
  auto ec = fs->Listen(port, backlog_, sock_opt_mask);
  CHECK(!ec) << "Could not open port " << port << " " << ec << "/" << ec.message();

  auto ep = fs->LocalEndpoint();
  lii->RegisterPool(pool_);

  lii->sock_ = std::move(fs);

  list_interface_.emplace_back(lii);

  return ep.port();
}

void AcceptServer::BreakListeners() {
  for (auto& lw : list_interface_) {
    ProactorBase* proactor = lw->sock_->proactor();
    proactor->Dispatch([sock = lw->sock_.get()] { sock->Shutdown(SHUT_RDWR); });
  }
  VLOG(1) << "AcceptServer::BreakListeners finished";
}

}  // namespace util
