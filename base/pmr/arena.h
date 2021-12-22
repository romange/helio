// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <vector>

namespace base {

// Used to allocate small blobs with size upto 4GB.
class PmrArena {
 public:
  PmrArena(std::pmr::memory_resource* mr = std::pmr::get_default_resource());
  ~PmrArena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  size_t MemoryUsage() const {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*);
  }

  void Swap(PmrArena& other);

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(uint32_t block_bytes);

  std::pmr::memory_resource* mr_;

  // Allocation state
  char* alloc_ptr_ = nullptr;
  size_t alloc_bytes_remaining_ = 0;

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_ = 0;

  struct Block {
    char* ptr;
    uint32_t sz;
  } __attribute__((packed));

  static_assert(sizeof(Block) == 12);

  // Array of the allocated memory blocks
  std::pmr::vector<Block> blocks_;

  // No copying allowed
  PmrArena(const PmrArena&) = delete;
  void operator=(const PmrArena&) = delete;
};

inline char* PmrArena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace base
