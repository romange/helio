// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>
#include <memory>

#include "util/fiber_socket_base.h"

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

  virtual uint32_t GetSockOptMask() const {
    return 1 << SO_REUSEADDR;
  }

  virtual ProactorBase* PickConnectionProactor(LinuxSocketBase* sock);

 protected:
  ProactorPool* pool() {
    return pool_;
  }

  FiberSocketBase* socket() {
    return sock_.get();
  }

 private:
  struct SafeConnList;

  void RunAcceptLoop();

  static void RunSingleConnection(Connection* conn, SafeConnList* list);

  std::unique_ptr<LinuxSocketBase> sock_;

  ProactorPool* pool_ = nullptr;
  friend class AcceptServer;
};

}  // namespace util
