// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/listener_interface.h"

#include <signal.h>
#include <sys/resource.h>

// #include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/accept_server.h"
#ifndef USE_FB2
#include "util/fiber_sched_algo.h"
#endif

#include "util/proactor_pool.h"

#define VSOCK(verbosity, sock) VLOG(verbosity) << "sock[" << native_handle(sock) << "] "
#define DVSOCK(verbosity, sock) DVLOG(verbosity) << "sock[" << native_handle(sock) << "] "

namespace util {

using namespace boost;
using namespace std;

#ifndef USE_FB2
using fibers_ext::Await;
#endif

namespace {

auto native_handle(const FiberSocketBase& sock) {
  return sock.native_handle();
}

auto native_handle(const Connection& conn) {
  return conn.socket()->native_handle();
}

using ListType =
    intrusive::slist<Connection, Connection::member_hook_t, intrusive::constant_time_size<true>,
                     intrusive::cache_last<false>>;

}  // namespace

struct ListenerInterface::TLConnList {
  ListType list;
#ifdef USE_FB2
  fb2::CondVarAny empty_cv;
#else
  fibers::condition_variable_any empty_cv;
#endif

  void Link(Connection* c) {
    DCHECK(!c->hook_.is_linked());

    list.push_front(*c);
    DVLOG(3) << "List size " << list.size();
  }

  void Unlink(Connection* c) {
    DCHECK(c->hook_.is_linked());
    DCHECK(!list.empty());

    auto it = ListType::s_iterator_to(*c);
    list.erase(it);

    DVLOG(2) << "Unlink conn, new list size: " << list.size();
    if (list.empty()) {
      empty_cv.notify_one();
    }
  }

  void AwaitEmpty() {
    if (list.empty())
      return;

    DVLOG(1) << "AwaitEmpty: List size: " << list.size();

    Await(empty_cv, [this] { return this->list.empty(); });
    DVLOG(1) << "AwaitEmpty finished ";
  }
};

thread_local unordered_map<ListenerInterface*, ListenerInterface::TLConnList*>
    ListenerInterface::conn_list;

// Runs in a dedicated fiber for each listener.
void ListenerInterface::RunAcceptLoop() {
  ThisFiber::SetName("AcceptLoop");
  FiberSocketBase::endpoint_type ep;

  if (!sock_->IsUDS() && !sock_->IsDirect()) {
    ep = sock_->LocalEndpoint();
    VSOCK(0, *sock_) << "AcceptServer - listening on port " << ep.port();
  }

  PreAcceptLoop(sock_->proactor());

  pool_->Await([this](auto*) {
    DVLOG(1) << "Emplacing " << this;
    conn_list.emplace(this, new TLConnList{});
  });

  while (true) {
    FiberSocketBase::AcceptResult res = sock_->Accept();
    if (!res.has_value()) {
      FiberSocketBase::error_code ec = res.error();
      if (ec != errc::connection_aborted) {
        LOG(ERROR) << "Error calling accept " << ec << "/" << ec.message();
      }
      break;
    }

    unique_ptr<FiberSocketBase> peer{res.value()};

    VSOCK(2, *peer) << "Accepted " << peer->RemoteEndpoint();

    uint32_t prev_connections = open_connections_.fetch_add(1, std::memory_order_relaxed);
    if (prev_connections >= max_clients_) {
      peer->SetProactor(sock_->proactor());
      OnMaxConnectionsReached(peer.get());
      (void)peer->Close();
      open_connections_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }

    // Most probably next is in another thread.
    fb2::ProactorBase* next = PickConnectionProactor(peer.get());

    peer->SetProactor(next);
    Connection* conn = NewConnection(next);
    conn->SetSocket(peer.release());
    conn->owner_ = this;

    // Run cb in its Proactor thread.
    next->Dispatch([this, conn] { RunSingleConnection(conn); });
  }

  sock_->Shutdown(SHUT_RDWR);
  PreShutdown();

#ifdef USE_FB2
#else
  // I am not a fan of "almost works" solutions. Here, I wait for Dispatch() calls
  // to start running and decrease the probability we continue without the up to date conn_list.
  // Since we are during the shutdown phase, waiting for 10ms is "probably" enough to let all the
  // callback to unwind and run.
  fibers_ext::SleepFor(10ms);
#endif
  atomic_uint32_t cur_conn_cnt{0};

  pool_->AwaitFiberOnAll([&](auto* pb) {
    auto it = conn_list.find(this);
    DCHECK(it != conn_list.end());
    auto* clist = it->second;

    cur_conn_cnt.fetch_add(clist->list.size(), memory_order_relaxed);

    // This is correctly atomic because Shutdown() does not block the fiber or yields.
    // However once we implement Shutdown via io_uring, this code becomes unsafe.
    for (auto& conn : clist->list) {
      conn.Shutdown();
      DVSOCK(1, conn) << "Shutdown";
    }
  });

  VLOG(1) << "Listener - " << ep.port() << " waiting for " << cur_conn_cnt
          << " connections to close";

  pool_->AwaitFiberOnAll([this](auto* pb) {
    auto it = conn_list.find(this);
    DCHECK(it != conn_list.end());

    it->second->AwaitEmpty();
    delete it->second;
    conn_list.erase(this);
  });
  VLOG(1) << "Listener - " << ep.port() << " connections closed";

  PostShutdown();
  error_code ec = sock_->Close();
  LOG_IF(WARNING, ec) << "Socket close failed: " << ec.message();
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
  OnConnectionStart(conn);

  try {
    conn->HandleRequests();
    VSOCK(1, *conn) << "After HandleRequests";

  } catch (std::exception& e) {
    LOG(ERROR) << "Uncaught exception " << e.what();
  }

  // Our connection could migrate, hence we should find it again
  OnConnectionClose(conn);
  LOG_IF(ERROR, conn->socket()->Close());

  clist = conn_list.find(this)->second;
  clist->Unlink(conn);

  guard.reset();
  open_connections_.fetch_sub(1, std::memory_order_relaxed);
}

void ListenerInterface::RegisterPool(ProactorPool* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);

  pool_ = pool;
}

error_code ListenerInterface::ConfigureServerSocket(int fd) {
  error_code ec;
  const int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0)
    ec.assign(errno, system_category());
  return ec;
}

fb2::ProactorBase* ListenerInterface::PickConnectionProactor(FiberSocketBase* sock) {
  return pool_->GetNextProactor();
}

void ListenerInterface::TraverseConnections(TraverseCB cb) {
  pool_->Await([&](unsigned index, auto* pb) {
    auto it = conn_list.find(this);
    FiberAtomicGuard fg;

    for (auto& conn : it->second->list) {
      cb(index, &conn);
    }
  });
}

void ListenerInterface::Migrate(Connection* conn, fb2::ProactorBase* dest) {
  fb2::ProactorBase* src_proactor = conn->socket()->proactor();
  CHECK(src_proactor->InMyThread());
  CHECK(conn->owner() == this);

  if (src_proactor == dest)
    return;

  VLOG(1) << "Migrating from " << src_proactor->thread_id() << " to " << dest->thread_id();

  conn->OnPreMigrateThread();
  auto* clist = conn_list.find(this)->second;
  clist->Unlink(conn);

  src_proactor->Migrate(dest);

  DCHECK(dest->InMyThread());  // We are running in the updated thread.
  conn->socket()->SetProactor(dest);

  auto* clist2 = conn_list.find(this)->second;
  DCHECK(clist2 != clist);

  clist2->Link(conn);
  conn->OnPostMigrateThread();
}

void ListenerInterface::SetMaxClients(uint32_t max_clients) {
  max_clients_ = max_clients;

  // We usually have 2 open files per proactor and ~10 more open files
  // for the rest. Taking 32 as a reasonable maximum bound.
  const uint32_t kResidualOpenFiles = 32;
  uint32_t wanted_limit = 2 * pool_->size() + max_clients + kResidualOpenFiles;

  struct rlimit lim;
  if (getrlimit(RLIMIT_NOFILE, &lim) == -1) {
    LOG(ERROR) << "Error in getrlimit: " << strerror(errno);
    return;
  }

  if (lim.rlim_cur < wanted_limit) {
    lim.rlim_cur = wanted_limit;
    if (setrlimit(RLIMIT_NOFILE, &lim) == -1) {
      LOG(WARNING) << "Couldn't increase the open files limit to " << lim.rlim_cur
                   << ". setrlimit: " << strerror(errno);
    }
  }
}

uint32_t ListenerInterface::GetMaxClients() const {
  return max_clients_;
}

void Connection::Shutdown() {
  auto ec = socket_->Shutdown(SHUT_RDWR);
  VLOG_IF(1, ec) << "Error during shutdown " << ec.message();

  OnShutdown();
}

}  // namespace util
