// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/proactor_pool.h"

namespace util {
namespace epoll {

class EvPool : public ProactorPool {
 public:
  //! Constructs io_context pool with number of threads equal to 'pool_size'.
  //! pool_size = 0 chooses automatically pool size equal to number of cores in
  //! the system.
  explicit EvPool(std::size_t pool_size = 0);

  ~EvPool();

 private:
  ProactorBase* CreateProactor() final;
  void InitInThread(unsigned index) final;
};


}  // namespace epoll
}  // namespace util
