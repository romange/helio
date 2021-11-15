// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/proactor_pool.h"
namespace util {
namespace uring {

class UringPool : public ProactorPool {
 public:
  //! Constructs io_context pool with number of threads equal to 'pool_size'.
  //! pool_size = 0 chooses automatically pool size equal to number of cores in
  //! the system.
  explicit UringPool(size_t ring_depth = 256, std::size_t pool_size = 0)
      : ProactorPool(pool_size), ring_depth_(ring_depth) {
  }

  ~UringPool();

 protected:
  ProactorBase* CreateProactor() final;
  void InitInThread(unsigned index) final;

 private:
  unsigned ring_depth_;
};

}  // namespace uring
}  // namespace util
