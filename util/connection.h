// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/intrusive/list.hpp>
#include <functional>
#include <memory>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "util/fiber_socket_base.h"
#include "util/fibers/proactor_base.h"

namespace util {

class ListenerInterface;

/**
 * @brief A connection object that represents a client tcp connection, managed by a listener.
 *
 * We use boost::intrusive_ref_counter to be able to access the object from multiple fibers
 * in the same thread. Specifically, we may shutdown a connection during the shutdown of
 * ListenerInterface::RunAcceptLoop,
 * while the connection is going through a closing sequence itself.
 */
class Connection : public boost::intrusive_ref_counter<Connection, boost::thread_unsafe_counter> {
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

 protected:
  // The main loop for a connection. Runs in the same proactor thread as of socket_.
  virtual void HandleRequests() = 0;

  virtual void OnShutdown() {
  }

  virtual void OnPreMigrateThread() {
  }
  virtual void OnPostMigrateThread() {
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
