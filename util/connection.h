// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/intrusive/list.hpp>
#include <functional>
#include <memory>

#include "util/fiber_socket_base.h"
#include "util/fibers/proactor_base.h"

namespace util {

class ListenerInterface;

class Connection {
  using connection_hook_t = ::boost::intrusive::list_member_hook<
      ::boost::intrusive::link_mode<::boost::intrusive::safe_link>>;

  connection_hook_t hook_;

 public:
  using member_hook_t =
      ::boost::intrusive::member_hook<Connection, connection_hook_t, &Connection::hook_>;

  virtual ~Connection() {
  }

  void SetSocket(FiberSocketBase* s) {
    socket_.reset(s);
  }

  FiberSocketBase* socket() {
    return socket_.get();
  }

  const FiberSocketBase* socket() const {
    return socket_.get();
  }

  FiberSocketBase* ReleaseSocket() {
    return socket_.release();
  }

  // Calls shutdown(SHUT_RDWR) on a socket and then
  // calls OnShutdown().
  void Shutdown();

  util::ProactorBase* DEBUG_proactor = nullptr;
 protected:
  // The main loop for a connection. Runs in the same proactor thread as of socket_.
  virtual void HandleRequests() = 0;

  virtual void OnShutdown() {
  }

  virtual void OnPreMigrateThread() {
  }
  virtual void OnPostMigrateThread() {
  }

  [[deprecated]]
  ListenerInterface* owner() const {
    return listener_;
  }

  ListenerInterface* listener() const {
    return listener_;
  }

  std::unique_ptr<FiberSocketBase> socket_;

 private:
  ListenerInterface* listener_ = nullptr;
  friend class ListenerInterface;
};

}  // namespace util
