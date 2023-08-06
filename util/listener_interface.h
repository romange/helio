// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "util/fiber_socket_base.h"

namespace util {

class ProactorPool;
class Connection;
class AcceptServer;

/**
 * @brief Abstracts away connections implementation and their life-cycle.
 *
 */
class ListenerInterface {
 public:
  using ProactorBase = fb2::ProactorBase;

  virtual ~ListenerInterface();

  void RegisterPool(ProactorPool* pool);

  //! Creates a dedicated handler for a new connection.
  //! Called per new accepted connection
  virtual Connection* NewConnection(fb2::ProactorBase* pb) = 0;

  //! Hook to be notified when listener interface start listening and accepting sockets.
  //! Called once.
  virtual void PreAcceptLoop(fb2::ProactorBase* pb) {
  }

  // Called by AcceptServer when shutting down start and before all connections are closed.
  virtual void PreShutdown() {
  }

  // Called by AcceptServer when shutting down finalized and after all connections are closed.
  virtual void PostShutdown() {
  }

  // Is called once when a server socket for this listener is configured and before
  // bind is called.
  virtual std::error_code ConfigureServerSocket(int fd);

  virtual fb2::ProactorBase* PickConnectionProactor(FiberSocketBase* sock);

  // This callback should not preempt because we traverse the list of connections
  // without locking it.
  // TraverseCB accepts the thread index and the connection pointer.
  using TraverseCB = std::function<void(unsigned, Connection*)>;

  // traverses all client connections in all threads. cb must be thread safe.
  // cb should not keep Connection* pointers beyond the run of this function because
  // Connection* are valid only during the call to cb.
  void TraverseConnections(TraverseCB cb);

  // Must be called from the connection fiber (that runs HandleRequests() function).
  // Moves the calling fiber from its thread to to dest proactor thread.
  // Updates socket_ and listener interface bookeepings.
  void Migrate(Connection* conn, fb2::ProactorBase* dest);

  FiberSocketBase* socket() {
    return sock_.get();
  }

  // Set the max clients value. Attempts to increase the rlimit for max open connections
  // if it is too low.
  void SetMaxClients(uint32_t max_clients);
  uint32_t GetMaxClients() const;

 protected:
  ProactorPool* pool() {
    return pool_;
  }

  virtual void OnConnectionStart(Connection* conn) {
  }

  virtual void OnConnectionClose(Connection* conn) {
  }

  // Called when a connection is rejected because max connections number was reached.
  // Override to send an error message to the socket.
  virtual void OnMaxConnectionsReached(FiberSocketBase* sock) {
  }

 private:
  void RunAcceptLoop();

  void RunSingleConnection(Connection* conn);

  struct TLConnList;  // threadlocal connection list. contains connections for that thread.

  static thread_local std::unordered_map<ListenerInterface*, TLConnList*> conn_list;

  std::unique_ptr<FiberSocketBase> sock_;
  // Number of max connections. Unlimited by default.
  uint32_t max_clients_{UINT32_MAX};
  // Number of current open connections. Incremented in RunAcceptLoop, decremented at the end of
  // RunSingleConnection.
  std::atomic_uint32_t open_connections_{0};

  ProactorPool* pool_ = nullptr;
  friend class AcceptServer;
};

}  // namespace util
