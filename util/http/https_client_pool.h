// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>
#include <memory>
#include <string>

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace fb2 {
class ProactorBase;
}  // namespace fb2

namespace http {

class Client;

// thread-local pool that manages a set of http(s) connections.
class ClientPool {
  ClientPool(const ClientPool&) = delete;
  ClientPool& operator=(const ClientPool&) = delete;
 public:
  class HandleGuard {
   public:
    HandleGuard(ClientPool* pool = nullptr) : pool_(pool) {
    }

    void operator()(Client* client);

   private:
    ClientPool* pool_;
  };

  using ClientHandle = std::unique_ptr<Client, HandleGuard>;

  ClientPool(const std::string& domain, SSL_CTX* ssl_ctx, fb2::ProactorBase* pb);

  ~ClientPool();

  /*! @brief Returns https client connection from the pool.
   *
   * Must be called withing IoContext thread. Once ClientHandle destructs,
   * the connection returns to the pool. GetHandle() might block the calling fiber for
   * connect_msec_ millis in case it creates a new connection.
   * Note that all allocated handles must be destroyed before destroying their parent pool.
   */
  ClientHandle GetHandle();

  void set_connect_timeout(unsigned msec) {
    connect_msec_ = msec;
  }

  //! Sets number of retries for https client handles.
  void set_retry_count(uint32_t cnt) {
    retry_cnt_ = cnt;
  }

  //! Number of existing handles created by this pool.
  unsigned handles_count() const {
    return existing_handles_;
  }

  const std::string& domain() const {
    return domain_;
  }

 private:
  std::string domain_;
  SSL_CTX* ssl_cntx_;
  fb2::ProactorBase& proactor_;

  unsigned connect_msec_ = 1000, retry_cnt_ = 1;
  int existing_handles_ = 0;

  std::deque<Client*> available_handles_;  // Using queue to allow round-robin access.
};

}  // namespace http
}  // namespace util
