// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/cuckoo_map.h"

#include "base/logging.h"

#define ADDITIONAL_CHECKS 0

namespace {

inline uint8 random_bit(uint32& bit_index) {
  return (bit_index++) & 1;
}

inline uint8 random_index(uint32& indx) {
  return (indx++) & 3;
}

static bool IsPrime(uint64 val) {
  if (val < 4)
    return true;
  if (val % 2 == 0 || val % 3 == 0)
    return false;

  for (uint32 i = 5; i * i <= val; i += 2) {
    if (val % i == 0)
      return false;
  }
  return true;
}

inline constexpr bool is_power_2(uint64 v) {
  return ((v - 1) & v) == 0;
}

constexpr unsigned kMinBucketCount = 16;

}  // namespace

namespace base {

uint64 GetPrimeNotLessThan(uint64 value) {
  if (value % 2 == 0)
    ++value;
  for (; value < kuint64max; value += 2) {
    if (IsPrime(value)) {
      return value;
    }
  }
  LOG(FATAL) << "Should not happen.";
  return 1;
}

constexpr CuckooMapTable::dense_id CuckooMapTable::npos;

/* State transition for BucketState:
   None -> visited: checked whether this bucket was already checked for fullness.
                    Also, makes sure that the traversal graph looks like tree:
                    every node has exactly one parent (or incoming edge).
   visited -> added to stack.
   popped out the stack and explored -> mark as explored and added it's children edges.
   explored -> unshiftable when all its children are explored
*/
class CuckooMapTable::BucketState {
  static constexpr uint8 kExploredBitVal = (1 << kBucketLength);
  static constexpr uint8 kVisitedBitVal = (kExploredBitVal << 1);  // 32
  static constexpr uint8 kFullBitVal = kVisitedBitVal << 1;
  static constexpr uint8 kChildrenMask = kExploredBitVal - 1;

  // contains the state of the bucket: which children might lead to the successful
  // shift chain and what's the state of this bucket in the traversal.
  uint8 mask_;

 public:
  BucketState() : mask_(0) {
    static_assert(kBucketLength <= 32, "Need more bits to encode the state");
    static_assert(kChildrenMask == 15, "");
  }

  // Marks the node as explored which means it was added to the stack.
  // children_mask contains those keys whose other buckets also full,
  // are not unshiftable and might lead to successful shift chain.
  // Note that explored node is either currently on stack or unshiftable.
  void set_explored(uint8 children_mask) {
    DCHECK_LT(children_mask, kExploredBitVal);
    mask_ |= (kExploredBitVal | children_mask);
  }

  // Returns true if keys of this bucket were not checked yet.
  bool unexplored() const {
    return !(mask_ & kExploredBitVal);
  }

  // This bucket was visited during the current DFS.
  void set_visited() {
    mask_ |= kVisitedBitVal;
  }
  bool visited() const {
    return mask_ & kVisitedBitVal;
  }
  void clear_visited() {
    mask_ &= ~kVisitedBitVal;
  }

  void set_full() {
    mask_ |= kFullBitVal;
  }
  bool full() const {
    return mask_ & kFullBitVal;
  }

  // number of unexplored children in the explored bucket.
  uint8 num_children() const {
    return absl::popcount(children_mask());
  }

  // returns first unexplored child.
  uint8 first_child() const {
    DCHECK_GT(num_children(), 0);
    return absl::countr_zero(children_mask());
  }

  uint8 children_mask() const {
    return mask_ & kChildrenMask;
  }

  // removes child from the set of children we need to explore.
  // Usuall called during backtracking.
  void remove_child(uint8 child) {
    mask_ &= ~uint8(1 << child);
  }

  void add_child(uint8 child) {
    mask_ |= uint8(1 << child);
  }

  // If bucket was explored and number of its children is 0, it means
  // that it's not possible to shift anything from this bucket.
  bool unshiftable() const {
    return (mask_ & ~kFullBitVal) == kExploredBitVal;
  }

  void reset_but_preserve_unshiftable() {
    if (!unshiftable())
      mask_ &= kFullBitVal;
    else
      mask_ = kFullBitVal | kExploredBitVal;
  }

  void reset() {
    mask_ &= kFullBitVal;
  }
  uint8 raw_mask() const {
    return mask_;
  }
};

// Allocates space for at least the given number of key-values.
CuckooMapTable::CuckooMapTable(const uint32 value_size, uint64 capacity) : value_size_(value_size) {
  static_assert(sizeof(Bucket) == 8 * kBucketLength, "Wrong bucket size");
  Init(std::max<BucketId>(kMinBucketCount, BucketFromId(capacity)));
}

CuckooMapTable::~CuckooMapTable() {
}

inline uint32 CuckooMapTable::CheckEmpty(const Bucket& bucket) const {
  // I trust compiler to do loop unrolling in opt mode.
  uint32 result = 0;
  for (uint8 i = 0; i < kBucketLength; ++i) {
    result |= ((bucket.key[i] == empty_value_) << i);
  }
  return result;
}

inline CuckooMapTable::BucketId CuckooMapTable::NextBucketId(BucketId current, key_type key) const {
  BucketIdPair id_pair = HashToIdPair(key);
  DCHECK(id_pair.id[0] == current || id_pair.id[1] == current);
  DCHECK_NE(id_pair.id[0], id_pair.id[1]);

  return (current == id_pair.id[0]) ? id_pair.id[1] : id_pair.id[0];
}

CuckooMapTable::BucketIdPair CuckooMapTable::HashToIdPair(const key_type v) const {
  BucketId a = hash1(v);
  __builtin_prefetch(buf_.get() + size_t(a) * bucket_size_, 0, 1);

  BucketId b = hash2(v);
  // __builtin_prefetch(buf_.get() + b * bucket_size_, 0, 1);
  // VLOG(2) << "HashToIdPair: " << v << " " << a_r << " " << b << " " << bucket_count_;
  // handle the case when 2 hash functions returns the same value.
  if (__builtin_expect(b == a, 0)) {
    b = (b + 1) % bucket_count_;
  }

  return BucketIdPair(a, b);
}

inline CuckooMapTable::dense_id CuckooMapTable::FindInBucket(const BucketIdPair& id_pair,
                                                             const key_type k) const {
  const key_type* a = GetBucketById(id_pair.id[0])->key;
  for (uint8 i = 0; i < kBucketLength; ++i) {
    if (a[i] == k)
      return ToDenseId(id_pair.id[0], i);
  }
  a = GetBucketById(id_pair.id[1])->key;
  for (uint8 i = 0; i < kBucketLength; ++i) {
    if (a[i] == k)
      return ToDenseId(id_pair.id[1], i);
  }
  return npos;
}

std::pair<CuckooMapTable::dense_id, bool> CuckooMapTable::Insert(key_type k, const uint8* data) {
  DCHECK(empty_value_set_);
  DCHECK_NE(empty_value_, k);

  std::pair<CuckooMapTable::dense_id, bool> result;
  BucketIdPair id_pair = HashToIdPair(k);
  result.first = FindInBucket(id_pair, k);
  if (result.first != npos) {
    return result;
  }
  VLOG(3) << "insert not found: " << k;
  result.second = true;
  ++inserts_since_last_grow_;

  // Setup key/value to roll.
  pending_key_ = k;
  memcpy(pending_ptr_, data, value_size_);
  uint32 iteration = 0;
  while (true) {
    VLOG(2) << "insert iteration: " << iteration << ", pending = " << pending_key_;
    result.first = RollPending(shifts_limit_, id_pair);
    if (result.first != npos) {
      break;
    }
    Grow();
    ++iteration;
    // After Grow() bucket_count_ has changed and after RollPending call pending_key has changed.
    id_pair = HashToIdPair(pending_key_);
  }
  ++size_;
  if (iteration != 0) {
    // We need to fix dense_id for k because after first iteration pending_key_ != k so
    // RollPending returns the dense id of the last inserted key.
    result.first = find(k);
  }
  return result;
}

inline int CuckooMapTable::InsertIntoBucket(const uint32 empty_mask, Bucket* bucket) {
  int i = absl::countr_zero(empty_mask);
  bucket->key[i] = pending_key_;
  memcpy(bucket->data + i * value_size_, pending_ptr_, value_size_);
  return i;
}

inline void CuckooMapTable::SwapPending(Bucket* bucket, uint8 index) {
  VLOG(3) << "Swapping " << pending_key_ << " and " << bucket->key[index]
          << " which now will be pending";
  std::swap(pending_key_, bucket->key[index]);
  uint8* next_ptr =
      (pending_ptr_ == tmp_value_.get()) ? pending_ptr_ + value_size_ : tmp_value_.get();
  uint8* value_ptr = bucket->data + value_size_ * index;
  memcpy(next_ptr, value_ptr, value_size_);
  memcpy(value_ptr, pending_ptr_, value_size_);
  pending_ptr_ = next_ptr;
}

void CuckooMapTable::SetEmptyValues() {
  uint8* ptr = buf_.get();
  for (BucketId i = 0; i < bucket_count_; ++i, ptr += bucket_size_) {
    Bucket* bucket = reinterpret_cast<Bucket*>(ptr);
    std::fill(bucket->key, bucket->key + kBucketLength, empty_value_);
  }
}

void CuckooMapTable::Init(BucketId bucket_capacity) {
  VLOG(1) << "Init: " << bucket_capacity;

  static_assert(is_power_2(kBucketLength), "BucketSizeMustBePowerOf2");
  bucket_size_ = kBucketLength * (sizeof(uint64) + value_size_);
  bucket_size_ = (1 + (bucket_size_ - 1) / 4) * 4;  // align by 4 for faster access.

  growth_ = 1.2f;
  tmp_value_.reset(new uint8[value_size_ * 2]);
  pending_ptr_ = tmp_value_.get();
  SetBucketCount(GetPrimeNotLessThan(bucket_capacity));
  DoAllocate();
}

void CuckooMapTable::Reserve(size_t bigger_capacity) {
  if (Capacity() >= bigger_capacity)
    return;

  if (empty()) {
    Init(BucketFromId(bigger_capacity));
  } else {
    Grow(BucketFromId(bigger_capacity));
  }
}

CuckooMapTable::dense_id CuckooMapTable::RollPending(uint32 shifts_limit,
                                                     const BucketIdPair& bucket_pair) {
  DCHECK(bucket_pair == HashToIdPair(pending_key_));

  // Check both possible buckets for an empty slot.
  Bucket* buckets[2];

  uint32 empty[2];
  for (unsigned j = 0; j < 2; ++j) {
    buckets[j] = GetBucketById(bucket_pair.id[j]);
    empty[j] = CheckEmpty(*buckets[j]);
    if (empty[j]) {
      int index = InsertIntoBucket(empty[j], buckets[j]);
      VLOG(2) << "Inserting " << pending_key_ << " into bucket " << j << "/" << bucket_pair.id[j]
              << " index= " << index << " with mask " << empty[j];
      // There is an empty slot in the first bucket.
      return ToDenseId(bucket_pair.id[j], InsertIntoBucket(empty[j], buckets[j]));
    }
  }

  BucketId bid = bucket_pair.id[0];
  Bucket* cur_bucket = buckets[0];
  if (random_bit(random_bit_indx_)) {
    bid = bucket_pair.id[1];
    cur_bucket = buckets[1];
  }

  BucketId start_bucket_id = bid;
  static_assert(kBucketLength == 4, "Other length is not supported yet");
  uint8 start_index = random_index(random_bit_indx_);
  uint8 shift_index = start_index;
  for (uint32 j = 0; j < shifts_limit; ++j) {
    SwapPending(cur_bucket, shift_index);
    // Choose the alternative index for the pending value.
    BucketId next_bid = NextBucketId(bid, pending_key_);
    cur_bucket = GetBucketById(next_bid);
    VLOG(3) << "Loop: " << j << ", " << uint32(shift_index) << ", from " << bid << " to next "
            << next_bid;
    bid = next_bid;
    const uint32 empty = CheckEmpty(*cur_bucket);
    if (empty) {
      sum_shifts_ += (j + 1);
      InsertIntoBucket(empty, cur_bucket);
      return ToDenseId(start_bucket_id, start_index);
    }

    shift_index = random_index(random_bit_indx_);
    if (bid == start_bucket_id && shift_index == start_index) {
      // We made exact cycle and returned to the same place we started from.
      // Lets change the shift index to something else.
      shift_index = (shift_index + 1) % kBucketLength;
    }
  }
  // We failed to insert.
  return npos;
}

// There is an empty slot in the second bucket.
void CuckooMapTable::Grow(size_t low_bucket_bound) {
  // Keep the old metadata
  std::unique_ptr<uint8[]> old_buf(buf_.release());
  const size_t old_bucket_count = bucket_count_;

  // Enlarge the container.  Repeat as long as we cannot reinsert everything.
  double avg_shift_count =
      inserts_since_last_grow_ == 0 ? 0 : double(sum_shifts_) / inserts_since_last_grow_;
  VLOG(1) << "PreGrow: "
          << "size_/capacity: " << size_ << "/" << Capacity() << ", utilization: " << Utilization()
          << ", inserts since last grow: " << inserts_since_last_grow_
          << ", avg_shift_count: " << avg_shift_count;

  uint32 iteration = 0;
  std::vector<uint8> save_pending_value(pending_ptr_, pending_ptr_ + value_size_);
  key_type save_pending_key = pending_key_;
  while (true) {
    BucketId new_bucket_count = static_cast<BucketId>(bucket_count_ * growth_ + 1.0f);
    if (new_bucket_count < low_bucket_bound)
      new_bucket_count = low_bucket_bound;

    new_bucket_count = GetPrimeNotLessThan(new_bucket_count);
    CHECK_LT(bucket_count_, new_bucket_count);

    SetBucketCount(new_bucket_count);
    DoAllocate();

    VLOG(1) << "Growing iteration: " << iteration << ", new_capacity " << Capacity()
            << ", new utilization " << Utilization() << " allocated: " << BytesAllocated();
    // Copy old the existing data to the new storage.
    bool success =
        CopyBuffer(old_buf.get(), old_bucket_count, [this](const BucketIdPair& pair) -> bool {
          // Provide more shifts than usual in order to have higher chance to succeed.
          // Do we need it?!! Needs profiling.
          return RollPending(shifts_limit_ * 2, pair) != npos;
        });
    if (success)
      break;
    iteration++;
  }
  inserts_since_last_grow_ = 0;
  sum_shifts_ = 0;
  pending_key_ = save_pending_key;
  memcpy(pending_ptr_, save_pending_value.data(), value_size_);
}

bool CuckooMapTable::CopyBuffer(const uint8* bucket_array, uint32 count,
                                std::function<bool(const BucketIdPair&)> insert_func) {
  const uint8* ptr = bucket_array;
  for (uint32 i = 0; i < count; ++i, ptr += bucket_size_) {
    const Bucket* bucket = reinterpret_cast<const Bucket*>(ptr);
    for (uint8 j = 0; j < kBucketLength; ++j) {
      pending_key_ = bucket->key[j];
      if (pending_key_ == empty_value_)
        continue;
      BucketIdPair id_pair = HashToIdPair(pending_key_);
      memcpy(pending_ptr_, bucket->data + j * value_size_, value_size_);
      if (!insert_func(id_pair))
        return false;
    }
  }
  return true;
}

void CuckooMapTable::SetBucketCount(size_t bucket_cnt) {
  VLOG(1) << "SetBucketCount " << bucket_cnt;

  bucket_count_ = bucket_cnt;
  shifts_limit_ = absl::countr_zero(bucket_count_) * 2;

  divide_s_ = libdivide::libdivide_u64_gen(bucket_count_);
  divide_s_alg_ = libdivide::libdivide_u64_get_algorithm(&divide_s_);

  VLOG(1) << "Shifts limit: " << shifts_limit_;
}

void CuckooMapTable::DoAllocate() {
  size_t sz = bucket_count_ * bucket_size_;
  buf_.reset(new (std::nothrow) uint8[sz]);
  CHECK(buf_) << "Could not allocate " << sz << " bytes";
  SetEmptyValues();
}

bool CuckooMapTable::Compact(double ratio) {
  CHECK_GT(ratio, 1.001);
  if (bucket_count_ < 128) {
    // Do not bother compacting it.
    return true;
  }
  BucketId prev_reserved_buckets = bucket_count_;
  std::unique_ptr<uint8[]> old_buf(buf_.release());
  BucketId bucket_count_from_size =
      std::max<size_t>(kMinBucketCount, 1 + BucketFromId(size() * ratio));

  BucketId desired_bucket_size = GetPrimeNotLessThan(bucket_count_from_size);
  VLOG(1) << "Current bucket size " << bucket_count_ << ", current utilization " << Utilization()
          << " desired bucket size: " << desired_bucket_size;
  const int kMaxCompactTries = 10;
  for (int i = 0; i < kMaxCompactTries; ++i) {
    SetBucketCount(desired_bucket_size);
    VLOG(1) << "Trying bucket size " << bucket_count_
            << " with expected utilization: " << Utilization();
    DoAllocate();
    std::vector<key_type> problematic_keys;
    std::vector<uint8> problematic_values;
    bool res = CopyBuffer(old_buf.get(), prev_reserved_buckets,
                          [this, &problematic_keys, &problematic_values](const BucketIdPair& pair) {
                            dense_id pos = RollPending(shifts_limit_ * 2, pair);
                            if (pos == npos) {
                              problematic_keys.push_back(pending_key_);
                              size_t sz = problematic_values.size();
                              problematic_values.resize(sz + value_size_);
                              memcpy(&problematic_values[sz], pending_ptr_, value_size_);
                            }
                            return true;
                          });
    CHECK(res);
    if (InsertProblematicKeys(problematic_keys, problematic_values.data())) {
      compaction_info_.shrink_to_fit();
      compaction_info_.clear();
      return true;
    }
    desired_bucket_size = GetPrimeNotLessThan(desired_bucket_size + 2);
  }
  return false;
}

bool CuckooMapTable::InsertProblematicKeys(const std::vector<key_type>& keys, const uint8* values) {
  if (keys.empty())
    return true;
  VLOG(1) << "Got " << keys.size() << " problematic keys during compaction to " << bucket_count_
          << " buckets";
  compaction_info_.assign(bucket_count_, BucketState());
  for (key_type k : keys) {
    pending_key_ = k;
    memcpy(pending_ptr_, values, value_size_);
    BucketIdPair id_pair = HashToIdPair(pending_key_);
    if (!ShiftExhaustive(id_pair.id[0])) {
      return false;
    }
    values += value_size_;
  }
  return true;
}

bool CuckooMapTable::ShiftExhaustive(BucketId bid) {
  CHECK_LT(bid, compaction_info_.size());
  compaction_stack_.push_back(bid);
  compaction_info_[bid].set_visited();
  uint32 visited_count = 1;
  key_type start_key = pending_key_;
  VLOG(2) << "ShiftExhaustive: starting from bid " << bid << " and pending_key_ " << pending_key_;
  while (!compaction_stack_.empty()) {
    BucketId parent = compaction_stack_.back();
    VLOG(3) << "Peeking at " << parent;
#if ADDITIONAL_CHECKS || !defined NDEBUG
    {
      BucketIdPair key = HashToIdPair(pending_key_);
      CHECK(key.id[0] == parent || key.id[1] == parent) << parent << ", " << pending_key_;
    }
#endif
    BucketState& state = compaction_info_[parent];
    Bucket* parent_bucket = GetBucketById(parent);
    if (state.unexplored()) {
      if (Explore(parent))
        break;
      visited_count += state.num_children();
    }
    if (state.num_children() == 0) {
      --visited_count;
      CHECK(state.visited());
      state.clear_visited();
      CHECK(state.unshiftable());
      // no children to traverse.
      VLOG(3) << "Popping " << parent;
      compaction_stack_.pop_back();
      continue;
    }
    uint8 child_index = state.first_child();

    // Swapping is needed for both advancing and backtracking.
    SwapPending(parent_bucket, child_index);

    // Special condition.
    if (parent == bid && pending_key_ == start_key) {
      VLOG(2) << "Reached the starting bucket, backtracking and removing explored child index "
              << uint32(child_index);
      state.remove_child(child_index);
      continue;
    }
    // we use pending_key_ because it was just swapped with
    // parent_bucket->key[child_index].
    // Note that when we advancing next_id is indeed the next bucket id
    // we want to explore.
    // But when we are backtracking it's the bid of the parent of the 'parent'
    // or another second bucket_id from the original pending_key_ (if stack size = 1)
    // In case of advancing the next_state will be unexplored.
    // In case of backtracking (in both sub-cases) it will be explored: either
    // in the stack as a grandparent or unshiftable.
    BucketId next_id = NextBucketId(parent, pending_key_);
    BucketState next_state = compaction_info_[next_id];
    if (next_state.unexplored()) {
      VLOG(3) << "Pushing child index " << uint32(child_index) << ", bid " << next_id << " and key "
              << pending_key_;
      // It's advancing, so we swapped key/value in hope that we find a shift path.
      compaction_stack_.push_back(next_id);
    } else {
      VLOG(3) << "Removing child index " << uint32(child_index) << " from bid " << parent;
      // We are backtracking. And Our swap (when advancing) did not lead to good path
      // so we fixed it now by swapping again. Now we also remove the child index from the set
      // of parents' children.
      // Lets check our invariants.
      // key_type child_key = parent_bucket->key[child_index];
      // BucketId child_id = NextBucketId(parent, child_key);
      /*CHECK(compaction_info_[child_id].unshiftable()) << "Transition from " << parent
          << " to " << child_id << " through key " << child_key;*/
      state.remove_child(child_index);
    }
  }

  // We traversed the whole graph of full buckets and did not succeed to find
  // a place to insert pending_key_.
  if (compaction_stack_.empty())
    return false;

  // We found a successful path. Now, we should clean all the states except
  // unshiftables. That means - all visited nodes.
  // Unfortunately we do not have them together. So we use the heuristic below.
  if (visited_count < compaction_info_.size() / 6) {
    // All visited nodes are children of the nodes in the stack.
    // We traverse from end to begin so that we won't remove children of the states
    // earlier in the stack.
    for (int32 i = compaction_stack_.size() - 1; i >= 0; --i) {
      BucketId id = compaction_stack_[i];
      BucketState& bstate = compaction_info_[id];
      Bucket* b = GetBucketById(id);
      while (bstate.num_children() > 0) {
        uint8 cindex = bstate.first_child();
        BucketId next = NextBucketId(id, b->key[cindex]);
        bstate.remove_child(cindex);
        if (i > 0 && next == compaction_stack_[i - 1])
          continue;
        VLOG(3) << "Unmark " << next;
        compaction_info_[next].reset_but_preserve_unshiftable();
      }
      bstate.reset_but_preserve_unshiftable();
    }
  } else {
    // LOG(INFO) << "Full Sweep: " << visited_count << " " << compaction_stack_.size()
    //           << " " << compaction_info_.size();
    for (BucketState& bs : compaction_info_) {
      bs.reset_but_preserve_unshiftable();
    }
  }
  compaction_stack_.clear();
  return true;
}

bool CuckooMapTable::Explore(BucketId parent) {
  BucketState& state = compaction_info_[parent];
  Bucket* parent_bucket = GetBucketById(parent);
  uint mask = 0;
  for (uint8 k = 0; k < kBucketLength; ++k) {
    key_type key = parent_bucket->key[k];
    DCHECK_NE(key, pending_key_);
    DCHECK_NE(key, empty_value_);
    BucketId next_id = NextBucketId(parent, key);
    BucketState& next_state = compaction_info_[next_id];
    /*if (++nextid_call_count % 1000000 == 0) {
      LOG(INFO) << "nextid_call_count " << nextid_call_count;
    }*/
    if (next_state.visited() || next_state.unshiftable())
      continue;
    VLOG(3) << "Adding child " << next_id << " to " << parent << " through " << key;
    if (!next_state.full()) {
      // Visited  - verified that the bucket is full.
      Bucket* next_bucket = GetBucketById(next_id);
      const uint32 empty = CheckEmpty(*next_bucket);
      if (empty) {
        key_type pending = pending_key_;
        SwapPending(parent_bucket, k);
        InsertIntoBucket(empty, next_bucket);
        VLOG(2) << "Found successful bucket " << next_id << " with mask " << empty
                << ", also stored " << pending << " in " << parent;
        if (mask) {
          // Set this mask so that all marked next states would be cleared when we
          // unwind the stack.
          state.set_explored(mask);
        }
        return true;
      }
      next_state.set_full();
    } else {
      /*if (++save_count % 10000 == 0) {
        LOG(INFO) << "Saved " << save_count;
      }*/
    }
    next_state.set_visited();
    mask |= (1 << k);
  }
  // We add here only those nodes that were not visited before.
  // That's enough in order to find a shift path.
  // Please note that we in fact traverse the sub-graph which is a tree consisting
  // only of full buckets.
  state.set_explored(mask);
  VLOG(3) << "Explored " << parent << ", with mask " << mask;
  return false;
}

}  // namespace base
