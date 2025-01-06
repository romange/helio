// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "base/pmr/memory_resource.h"
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

  void InitByAcceptServer(ProactorPool* pool, PMR_NS::memory_resource* mr);

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

  // traverses all client connection in current thread. cb must adhere to rules from
  // `TraverseConnections`. Specifically, cb should not fiber block so that the underlying
  // connection list won't change during the traversal.
  Connection* TraverseConnectionsOnThread(TraverseCB cb, uint32_t limit, Connection* from);

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

  void SetConnFiberStackSize(uint32_t size) {
    conn_fiber_stack_size_ = size;
  }

  // Stops accepting the new sockets. Any incomming connection is immediately closed.
  // The listener continues runnning.
  void pause_accepting() {
    pause_accepting_ = true;
  }

  void resume_accepting() {
    pause_accepting_ = false;
  }

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
  struct TLConnList;  // threadlocal connection list. contains connections for that thread.
  using ListenerConnMap = std::unordered_map<ListenerInterface*, TLConnList*>;

  void RunAcceptLoop();

  void RunSingleConnection(Connection* conn);

  static ListenerConnMap* GetSafeTlsConnMap();

  static thread_local ListenerConnMap listener_map;

  std::unique_ptr<FiberSocketBase> sock_;
  // Number of max connections. Unlimited by default.
  uint32_t max_clients_{UINT32_MAX};

  uint32_t conn_fiber_stack_size_ = 64 * 1024;

  // Number of current open connections. Incremented in RunAcceptLoop, decremented at the end of
  // RunSingleConnection.
  std::atomic_uint32_t open_connections_{0};

  ProactorPool* pool_ = nullptr;
  PMR_NS::memory_resource* mr_;
  bool pause_accepting_ = false;

  // we want to prevent from migrations running in parallel to traversals using
  // the following rules:
  // 1. Multiple traversals can run in parallel.
  // 2. Multiple migrations  can run in parallel.
  // 3. Traversals are not common, therefore to prevent starvation in frequent migration
  //    scenarios, traversals must be able to proceed.
  //
  // We divide this 64 bit integer into 32 high-bit migration counter and 32 low-bit
  // traversal counter. Any of the parties can progress only if the other is 0.
  // The code however is not symmetric, see cc file for more details.
  std::atomic_uint64_t migrate_traversal_state_{0};
  constexpr static uint64_t kMigrateVal = 1ULL << 32;
  constexpr static uint64_t kTraverseVal = 1ULL;
  friend class AcceptServer;
};

}  // namespace util
