// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/proactor_pool.h"

namespace util {
namespace epoll {

class EpollPool : public ProactorPool {
 public:
  //! Constructs io_context pool with number of threads equal to 'pool_size'.
  //! pool_size = 0 chooses automatically pool size equal to number of cores in
  //! the system.
  explicit EpollPool(std::size_t pool_size = 0);

  ~EpollPool();

 private:
  ProactorBase* CreateProactor() final;
  void InitInThread(unsigned index) final;
};


}  // namespace epoll
}  // namespace util
