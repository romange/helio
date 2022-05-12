// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>
#include <memory>

#include "util/fiber_socket_base.h"
#include <unordered_map>

namespace util {

class ProactorPool;
class Connection;
class ProactorBase;
class AcceptServer;

/**
 * @brief Abstracts away connections implementation and their life-cycle.
 *
 */
class ListenerInterface {
 public:
  virtual ~ListenerInterface();

  void RegisterPool(ProactorPool* pool);

  //! Creates a dedicated handler for a new connection.
  //! Called per new accepted connection
  virtual Connection* NewConnection(ProactorBase* pb) = 0;

  //! Hook to be notified when listener interface start listening and accepting sockets.
  //! Called once.
  virtual void PreAcceptLoop(ProactorBase* pb) {
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

  virtual ProactorBase* PickConnectionProactor(LinuxSocketBase* sock);

  using TraverseCB = std::function<void(Connection*)>;

  // traverses all client connections in all threads. cb must be thread safe.
  // cb should not keep Connection* pointers beyond the run of this function because
  // Connection* are valid only during the call to cb.
  void TraverseConnections(TraverseCB cb);

  // Must be called from the connection fiber (that runs HandleRequests() function).
  // Moves the calling fiber from its thread to to dest proactor thread.
  // Updates socket_ and listener interface bookeepings.
  void Migrate(Connection* conn, ProactorBase* dest);

  LinuxSocketBase* socket() {
    return sock_.get();
  }

 protected:
  ProactorPool* pool() {
    return pool_;
  }

 private:
  void RunAcceptLoop();

  void RunSingleConnection(Connection* conn);

  struct TLConnList;  // threadlocal connection list. contains connections for that thread.

  static thread_local std::unordered_map<ListenerInterface*, TLConnList*> conn_list;

  std::unique_ptr<LinuxSocketBase> sock_;

  ProactorPool* pool_ = nullptr;
  friend class AcceptServer;
};

}  // namespace util
