// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/listener_interface.h"

#include <signal.h>
#include <sys/resource.h>

#include "base/logging.h"
#include "util/accept_server.h"
#include "util/proactor_pool.h"

#define VSOCK(verbosity, sock) VLOG(verbosity) << "sock[" << native_handle(sock) << "] "
#define DVSOCK(verbosity, sock) DVLOG(verbosity) << "sock[" << native_handle(sock) << "] "

namespace util {

using namespace boost;
using namespace std;

namespace {

auto native_handle(const FiberSocketBase& sock) {
  return sock.native_handle();
}

auto native_handle(const Connection& conn) {
  return conn.socket()->native_handle();
}

// We set intrusive::constant_time_size<true> because we use ListType::size()
// for introspection purposes.
using ListType =
    intrusive::list<Connection, Connection::member_hook_t, intrusive::constant_time_size<true>>;

}  // namespace

thread_local ListenerInterface::ListenerConnMap ListenerInterface::conn_list;

struct ListenerInterface::TLConnList {
  ListType list;
  fb2::CondVarAny empty_cv;
  int pool_index = -1;

  void Link(Connection* c);

  void Unlink(Connection* c, ListenerInterface* l);

  void AwaitEmpty() {
    if (list.empty())
      return;

    DVLOG(1) << "AwaitEmpty: List size: " << list.size();

    Await(empty_cv, [this] { return this->list.empty(); });
    DVLOG(1) << "AwaitEmpty finished ";
  }
};

void ListenerInterface::TLConnList::Link(Connection* c) {
  DCHECK(!c->hook_.is_linked());
  list.push_back(*c);
  CHECK_EQ(c->socket()->proactor()->GetPoolIndex(), this->pool_index);

  DVLOG(3) << "List size " << list.size();
}

void ListenerInterface::TLConnList::Unlink(Connection* c, ListenerInterface* l) {
  DCHECK(c->hook_.is_linked());

  auto it = ListType::s_iterator_to(*c);
  util::ProactorBase* conn_proactor = c->socket()->proactor();
  CHECK_EQ(pool_index, conn_proactor->GetPoolIndex());

  DCHECK(!list.empty());
  list.erase(it);

  DVLOG(2) << "Unlink conn, new list size: " << list.size();
  if (list.empty()) {
    empty_cv.notify_one();
  }
}

auto __attribute__((noinline)) ListenerInterface::GetSafeTlsConnMap() -> ListenerConnMap* {
  // Prevents conn_list being cached when a function being migrated between threads.
  // See: https://stackoverflow.com/a/75622732 and also https://godbolt.org/z/acrozfMW8
  // for why we need this.

  asm volatile("");
  return &ListenerInterface::conn_list;
}

// Runs in a dedicated fiber for each listener.
void ListenerInterface::RunAcceptLoop() {
  ThisFiber::SetName("AcceptLoop");
  FiberSocketBase::endpoint_type ep;

  if (!sock_->IsUDS()) {
    ep = sock_->LocalEndpoint();
    VSOCK(0, *sock_) << "AcceptServer - listening on port " << ep.port();
  }

  PreAcceptLoop(sock_->proactor());

  pool_->Await([this](unsigned index, auto*) {
    DVLOG(1) << "Emplacing listener " << this;
    auto res = conn_list.emplace(this, new TLConnList{});
    CHECK(res.second);
    res.first->second->pool_index = index;
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

    uint32_t prev_connections = open_connections_.fetch_add(1, std::memory_order_acquire);
    if (prev_connections >= max_clients_) {
      // In general, we do not handle max client limit correctly for case where we experience
      // connection storms. The reason is that we immediately close the just-accepted socket,
      // which causes clients just to retry again and again, and increases the load on the server.
      // What we need to do is to start throttling the accept queue by NOT handling
      // the accept requests. Then the accept queue overflows and the linux OS
      // starts blocking connection requests by dropping SYN packets, effectively stalling
      // the connecting clients.
      // Here is the great article explaining the dynamics of the connection storms:
      // https://veithen.io/2014/01/01/how-tcp-backlog-works-in-linux.html
      // There is this one as well: https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
      peer->SetProactor(sock_->proactor());
      OnMaxConnectionsReached(peer.get());
      (void)peer->Close();
      open_connections_.fetch_sub(1, std::memory_order_release);
      continue;
    }

    // Most probably next is in another thread.
    fb2::ProactorBase* next = PickConnectionProactor(peer.get());

    Connection* conn = NewConnection(next);
    conn->listener_ = this;
    conn->SetSocket(peer.release());

    // Run cb in its Proactor thread.
    next->Dispatch([this, conn] {
      conn->socket()->SetProactor(fb2::ProactorBase::me());
      RunSingleConnection(conn);
    });
  }

  error_code ec = sock_->Shutdown(SHUT_RDWR);
  PreShutdown();

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
  ec = sock_->Close();
  LOG_IF(WARNING, ec) << "Socket close failed: " << ec.message();
  LOG(INFO) << "Listener stopped for port " << ep.port();
}

ListenerInterface::~ListenerInterface() {
  VLOG(1) << "Destroying ListenerInterface " << this;
}

void ListenerInterface::RunSingleConnection(Connection* conn) {
  VSOCK(2, *conn) << "Running connection ";

  unique_ptr<Connection> guard(conn);

  ListenerConnMap* conn_map = GetSafeTlsConnMap();
  TLConnList* clist = conn_map->find(this)->second;
  clist->Link(conn);
  OnConnectionStart(conn);

  try {
    conn->HandleRequests();
    VSOCK(1, *conn) << "After HandleRequests";

  } catch (std::exception& e) {
    LOG(ERROR) << "Uncaught exception " << e.what();
  }

  OnConnectionClose(conn);
  CHECK(conn->socket() != nullptr);

  VSOCK(1, *conn) << "Closing connection";
  LOG_IF(ERROR, conn->socket()->Close());

  // Our connection could migrate, hence we should find it again
  conn_map = GetSafeTlsConnMap();

  auto it = conn_map->find(this);

  // If the listener was destroyed (when we are in the middle of migration)
  // we do not need to unlink the connection.
  if (it != conn_map->end()) {
    clist = it->second;

    clist->Unlink(conn, this);
  }
  guard.reset();
  open_connections_.fetch_sub(1, std::memory_order_release);
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
  pool_->Await([&](unsigned index, auto* pb) { TraverseConnectionsOnThread(cb); });
}

void ListenerInterface::TraverseConnectionsOnThread(TraverseCB cb) {
  DCHECK(ProactorBase::IsProactorThread());
  unsigned index = ProactorBase::me()->GetPoolIndex();

  FiberAtomicGuard fg;
  auto it = conn_list.find(this);
  for (auto& conn : it->second->list) {
    cb(index, &conn);
  }
}

void ListenerInterface::Migrate(Connection* conn, fb2::ProactorBase* dest) {
  fb2::ProactorBase* src_proactor = conn->socket()->proactor();
  DCHECK(src_proactor->InMyThread());
  CHECK(conn->listener() == this);
  DCHECK_EQ(src_proactor, ProactorBase::me());

  if (src_proactor == dest)
    return;

  // We are careful to avoid accessing tls data structures if possible and use stack variables
  // instead.
  unsigned src_index = src_proactor->GetPoolIndex();

  ListenerConnMap* src_conn_map = GetSafeTlsConnMap();
  VLOG(1) << "Migrating from " << src_proactor->sys_tid() << "(" << src_index << ") "
          << src_conn_map << " to " << dest->sys_tid();

  conn->OnPreMigrateThread();

  TLConnList* src_clist = src_conn_map->find(this)->second;
  src_clist->Unlink(conn, this);
  conn->socket()->SetProactor(nullptr);
  src_proactor->Migrate(dest);

  CHECK(dest->InMyThread());  // We are running in the updated thread.
  conn->socket()->SetProactor(dest);

  ListenerConnMap* dest_conn_map = GetSafeTlsConnMap();
  int dest_index = dest->GetPoolIndex();
  VLOG(1) << "Migrated from " << src_index << " to " << dest_index << " " << dest_conn_map;

  CHECK_NE(dest_conn_map, src_conn_map);
  auto it = dest_conn_map->find(this);

  // If the listener is being destroyed but the connection is in the middle of thread migration,
  // we do not need to link it again.
  if (it != dest_conn_map->end()) {
    TLConnList* dst_clist = it->second;
    CHECK(dst_clist != src_clist);
    CHECK_EQ(dst_clist->pool_index, dest_index);
    dst_clist->Link(conn);
  }
  conn->OnPostMigrateThread();
}

void ListenerInterface::SetMaxClients(uint32_t max_clients) {
  max_clients_ = max_clients;

  // We usually have 2 open files per proactor and ~10 more open files
  // for the rest. Taking 150 as a reasonable upper bound.
  const uint32_t kResidualOpenFiles = 150;
  uint32_t wanted_limit = max_clients + kResidualOpenFiles;

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
  auto ec = CHECK_NOTNULL(socket_)->Shutdown(SHUT_RDWR);
  VLOG_IF(1, ec) << "Error during shutdown " << ec.message();

  OnShutdown();
}

}  // namespace util
