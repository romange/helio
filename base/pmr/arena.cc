// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/pmr/arena.h"

#include <cassert>

namespace base {

static const int kBlockSize = 8192;
using namespace std;

PmrArena::PmrArena(pmr::memory_resource* mr) : mr_(mr), blocks_(mr) {
}

PmrArena::~PmrArena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    mr_->deallocate(blocks_[i].ptr, blocks_[i].sz);
  }
}

char* PmrArena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* PmrArena::AllocateAligned(size_t bytes) {
  const int align = sizeof(void*);     // We'll align to pointer size
  assert((align & (align - 1)) == 0);  // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* PmrArena::AllocateNewBlock(uint32_t block_bytes) {
  char* result = reinterpret_cast<char*>(mr_->allocate(block_bytes));
  blocks_memory_ += block_bytes;
  blocks_.push_back(Block{result, block_bytes});
  return result;
}

void PmrArena::Swap(PmrArena& other) {
  swap(other.alloc_ptr_, alloc_ptr_);
  swap(other.alloc_bytes_remaining_, alloc_bytes_remaining_);

  swap(other.blocks_, blocks_);
  swap(other.blocks_memory_, blocks_memory_);
  std::swap(other.mr_, mr_);
}

}  // namespace base
