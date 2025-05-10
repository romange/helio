// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/accept_server.h"

#include <absl/strings/numbers.h>
#include <netdb.h>
#include <signal.h>

#include <system_error>

#include "base/logging.h"
#include "io/io.h"
#include "util/fiber_socket_base.h"
#include "util/listener_interface.h"
#include "util/proactor_pool.h"

namespace util {

using namespace boost;
using namespace std;

static io::Result<struct addrinfo*> CreateServerSocket(struct addrinfo* servinfo, uint16_t backlog,
                                                       ListenerInterface* listener,
                                                       FiberSocketBase* fs) {
  int family_pref[2] = {AF_INET, AF_INET6};
  error_code ec;
  // Try ip4 first
  for (unsigned j = 0; j < 2; ++j) {
    for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
      if (p->ai_family != family_pref[j])
        continue;
      auto ec = fs->Create(p->ai_family);
      if (ec)
        continue;

      ec = listener->ConfigureServerSocket(fs->native_handle());
      if (ec)
        break;

      ec = fs->Bind(p->ai_addr, p->ai_addrlen);
      if (ec)
        break;
      ec = fs->Listen(backlog);
      if (ec)
        break;
      return p;
    }
    (void)fs->Close();
  };
  return nonstd::make_unexpected(ec);
}

AcceptServer::AcceptServer(ProactorPool* pool, PMR_NS::memory_resource* mr, bool break_on_int)
    : pool_(pool), mr_(mr), ref_bc_(0), break_on_int_(break_on_int) {
  if (break_on_int) {
    ProactorBase* proactor = pool_->GetNextProactor();
    ProactorBase::RegisterSignal({SIGINT, SIGTERM}, proactor, [this](int signal) {
      LOG(INFO) << "Exiting on signal " << strsignal(signal);
      if (on_break_hook_) {
        on_break_hook_();
      }
      BreakListeners();
    });
  }
}

AcceptServer::~AcceptServer() {
  if (break_on_int_) {
    ProactorBase::ClearSignal({SIGINT, SIGTERM}, true);
  }
  list_interface_.clear();
}

void AcceptServer::Run() {
  VLOG(1) << "AcceptServer::Run";

  if (!list_interface_.empty()) {
    ref_bc_->Add(list_interface_.size());

    for (auto& lw : list_interface_) {
      ProactorBase* proactor = lw->socket()->proactor();

      // We must capture ref_bc_ by value because once it is decremented, AcceptServer
      // instance can be destroyed before Dec returnes.
      proactor->Dispatch([li = lw.get(), bc = ref_bc_]() mutable {
        li->RunAcceptLoop();
        bc->Dec();
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
    ref_bc_->Wait();
    VLOG(1) << "AcceptServer::Wait completed";
    was_run_ = false;
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
  CHECK(!was_run_);

  char str_port[absl::numbers_internal::kFastToBufferSize];
  struct addrinfo hints, *servinfo;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
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

  unique_ptr<FiberSocketBase> fs{next->CreateSocket()};
  DCHECK(fs);

  io::Result<struct addrinfo*> listen_res =
      next->Await([&] { return CreateServerSocket(servinfo, backlog_, listener, fs.get()); });
  int res_family = listen_res ? (*listen_res)->ai_family : AF_UNSPEC;
  freeaddrinfo(servinfo);

  if (!listen_res) {
    return listen_res.error();
  }

  DCHECK(fs->IsOpen());

  const char* safe_bind = bind_addr ? bind_addr : "";
  VLOG(1) << "AddListener [" << fs->native_handle() << "] family: " << res_family << " "
          << safe_bind << ":" << port;

  listener->InitByAcceptServer(pool_, mr_);
  listener->sock_ = std::move(fs);
  list_interface_.emplace_back(listener);

  return {};
}

error_code AcceptServer::AddUDSListener(const char* path, mode_t permissions,
                                        ListenerInterface* listener) {
  CHECK(listener && !listener->socket());
  CHECK(!was_run_);

  ProactorBase* next = pool_->GetNextProactor();
  unique_ptr<FiberSocketBase> fs{next->CreateSocket()};

  error_code ec = next->Await([&] {
    error_code ec = fs->Create(AF_UNIX);
    if (ec)
      return ec;

    ec = listener->ConfigureServerSocket(fs->native_handle());
    if (ec)
      return ec;

    return fs->ListenUDS(path, permissions, backlog_);
  });

  if (!ec) {
    listener->InitByAcceptServer(pool_, mr_);
    listener->sock_ = std::move(fs);
    list_interface_.emplace_back(listener);
  }

  return ec;
}

void AcceptServer::BreakListeners() {
  for (auto& lw : list_interface_) {
    lw->StopAccepting();
  }
  VLOG(1) << "AcceptServer::BreakListeners finished";
}
}  // namespace util
