// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/listener_interface.h"

#include <signal.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/accept_server.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_pool.h"

#define VSOCK(verbosity, sock) VLOG(verbosity) << "sock[" << native_handle(sock) << "] "
#define DVSOCK(verbosity, sock) DVLOG(verbosity) << "sock[" << native_handle(sock) << "] "

namespace util {

using namespace boost;
using namespace std;

namespace {

auto native_handle(const LinuxSocketBase& sock) {
  return sock.native_handle();
}

auto native_handle(const Connection& conn) {
  const LinuxSocketBase* ls = static_cast<const LinuxSocketBase*>(conn.socket());
  return ls->native_handle();
}

using ListType =
    intrusive::slist<Connection, Connection::member_hook_t, intrusive::constant_time_size<true>,
                     intrusive::cache_last<false>>;

}  // namespace

struct ListenerInterface::TLConnList {
  ListType list;
  fibers::condition_variable_any empty_cv;

  void Link(Connection* c) {
    list.push_front(*c);
    DVLOG(3) << "List size " << list.size();
  }

  void Unlink(Connection* c) {
    auto it = list.iterator_to(*c);
    list.erase(it);
    DVLOG(3) << "List size " << list.size();
    if (list.empty()) {
      empty_cv.notify_one();
    }
  }

  void AwaitEmpty() {
    DVLOG(1) << "AwaitEmpty: List size: " << list.size();

    fibers_ext::Await(empty_cv, [this] { return this->list.empty(); });
  }
};

thread_local unordered_map<ListenerInterface*, ListenerInterface::TLConnList*>
    ListenerInterface::conn_list;

// Runs in a dedicated fiber for each listener.
void ListenerInterface::RunAcceptLoop() {
  auto& fiber_props = this_fiber::properties<FiberProps>();
  fiber_props.set_name("AcceptLoop");

  auto ep = sock_->LocalEndpoint();
  VSOCK(0, *sock_) << "AcceptServer - listening on port " << ep.port();

  PreAcceptLoop(sock_->proactor());

  pool_->Await([this](auto*) { conn_list.emplace(this, new TLConnList{}); });

  while (true) {
    FiberSocketBase::AcceptResult res = sock_->Accept();
    if (!res.has_value()) {
      FiberSocketBase::error_code ec = res.error();
      if (ec != errc::connection_aborted) {
        LOG(ERROR) << "Error calling accept " << ec << "/" << ec.message();
      }
      break;
    }

    unique_ptr<LinuxSocketBase> peer{static_cast<LinuxSocketBase*>(res.value())};

    VSOCK(2, *peer) << "Accepted " << peer->RemoteEndpoint();

    // Most probably next is in another thread.
    ProactorBase* next = PickConnectionProactor(peer.get());

    peer->SetProactor(next);
    Connection* conn = NewConnection(next);
    conn->SetSocket(peer.release());

    // Run cb in its Proactor thread.
    next->Dispatch([this, conn] { RunSingleConnection(conn); });
  }

  PreShutdown();

  atomic_uint32_t cur_conn_cnt{0};

  pool_->AwaitFiberOnAll([&](auto* pb) {
    auto* clist = conn_list.find(this)->second;

    cur_conn_cnt.fetch_add(clist->list.size(), memory_order_relaxed);
    for (auto& conn : clist->list) {
      conn.Shutdown();
      DVSOCK(1, conn) << "Shutdown";
    }
  });

  VLOG(1) << "Waiting for " << cur_conn_cnt << " connections to close";

  pool_->AwaitFiberOnAll([&](auto* pb) {
    auto it = conn_list.find(this);
    it->second->AwaitEmpty();
    delete it->second;
    conn_list.erase(this);
  });

  PostShutdown();

  LOG(INFO) << "Listener stopped for port " << ep.port();
}

ListenerInterface::~ListenerInterface() {
  VLOG(1) << "Destroying ListenerInterface " << this;
}

void ListenerInterface::RunSingleConnection(Connection* conn) {
  VSOCK(2, *conn) << "Running connection ";

  unique_ptr<Connection> guard(conn);
  auto* clist = conn_list.find(this)->second;
  clist->Link(conn);

  try {
    conn->HandleRequests();
    VSOCK(2, *conn) << "After HandleRequests";

  } catch (std::exception& e) {
    LOG(ERROR) << "Uncaught exception " << e.what();
  }

  clist->Unlink(conn);
}

void ListenerInterface::RegisterPool(ProactorPool* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);

  pool_ = pool;
}

ProactorBase* ListenerInterface::PickConnectionProactor(LinuxSocketBase* sock) {
  return pool_->GetNextProactor();
}

void ListenerInterface::TraverseConnections(TraverseCB cb) {
  pool_->AwaitFiberOnAll([&](auto* pb) {
    auto it = conn_list.find(this);

    for (auto& conn : it->second->list) {
      cb(&conn);
    }
  });
}

void ListenerInterface::Migrate(Connection* conn, ProactorBase* dest) {
  ProactorBase* src = conn->socket()->proactor();
  CHECK(src->InMyThread());

  if (src == dest)
    return;

  auto* clist = conn_list.find(this)->second;
  clist->Unlink(conn);

  src->Migrate(dest);

  DCHECK(dest->InMyThread());  // We are running in the updated thread.
  conn->socket()->SetProactor(dest);

  clist = conn_list.find(this)->second;
  clist->Link(conn);
}

void Connection::Shutdown() {
  auto ec = socket_->Shutdown(SHUT_RDWR);
  VLOG_IF(1, ec) << "Error during shutdown " << ec.message();

  OnShutdown();
}

}  // namespace util
