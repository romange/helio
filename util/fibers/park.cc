// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/park.h"

#include "base/logging.h"
#include "util/fibers/detail/fiber_interface_impl.h"

namespace util {
namespace fb2 {

void ParkHandle::Park() noexcept {
  DCHECK(fiber_ == detail::FiberActive());
  DCHECK(!detail::IsFiberAtomicSection());
  fiber_->Suspend();
}

void WakeList::Wake() noexcept {
  ParkNode* node = std::exchange(head_, nullptr);
  if (!node)
    return;

  detail::FiberInterface* active = detail::FiberActive();
  do {
    // Clear the link so the node can be published again after Park() returns.
    ParkNode* next = std::exchange(node->next_, nullptr);
    detail::FiberInterface* fiber = node->fiber_;
    DCHECK(fiber != active) << "A fiber cannot wake its own node";

    // `node` may be destroyed once `fiber` runs.
    active->ActivateOther(fiber);
    node = next;
  } while (node);
}

void LocalityParkQueue::Push(LocalityParkNode* node) noexcept {
  DCHECK(node->next_ == nullptr);
  DCHECK(node->fiber_ == detail::FiberActive());

  detail::Scheduler* sched = node->fiber_->scheduler();
  uint64_t now = base::CycleClock::Now();
  for (Lane& lane : lanes_) {
    if (lane.sched == sched) {
      lane.tail->next_ = node;
      lane.tail->next_push_cycles_ = now;
      lane.tail = node;
      return;
    }
  }
  lanes_.push_back(Lane{sched, node, node, now});
}

LocalityParkNode* LocalityParkQueue::Pop() noexcept {
  if (lanes_.empty())
    return nullptr;
  bypasses_ = 0;
  return PopLane(OldestLane());
}

LocalityParkNode* LocalityParkQueue::PopNear() noexcept {
  if (lanes_.empty())
    return nullptr;

  detail::Scheduler* sched = detail::FiberActive()->scheduler();
  unsigned oldest = 0;
  unsigned near = lanes_.size();
  for (unsigned i = 0; i < lanes_.size(); ++i) {
    if (lanes_[i].head_push_cycles < lanes_[oldest].head_push_cycles)
      oldest = i;
    if (lanes_[i].sched == sched)
      near = i;
  }

  if (near < lanes_.size() && near != oldest && bypasses_ < max_bypass_) {
    uint64_t waited = base::CycleClock::Now() - lanes_[oldest].head_push_cycles;
    if (waited < base::CycleClock::FromUsec(max_wait_usec_)) {
      ++bypasses_;
      return PopLane(near);
    }
  }
  bypasses_ = 0;
  return PopLane(oldest);
}

void LocalityParkQueue::WakeAll(WakeList* wakes) noexcept {
  for (const Lane& lane : lanes_) {
    if (wakes->head_)
      lane.tail->next_ = wakes->head_;
    wakes->head_ = lane.head;
  }
  lanes_.clear();
  bypasses_ = 0;
}

unsigned LocalityParkQueue::OldestLane() const noexcept {
  unsigned oldest = 0;
  for (unsigned i = 1; i < lanes_.size(); ++i) {
    if (lanes_[i].head_push_cycles < lanes_[oldest].head_push_cycles)
      oldest = i;
  }
  return oldest;
}

LocalityParkNode* LocalityParkQueue::PopLane(unsigned index) noexcept {
  Lane& lane = lanes_[index];
  LocalityParkNode* node = lane.head;
  if (node == lane.tail) {
    lane = lanes_.back();
    lanes_.pop_back();
  } else {
    lane.head = static_cast<LocalityParkNode*>(node->next_);
    lane.head_push_cycles = node->next_push_cycles_;
    node->next_ = nullptr;
  }
  return node;
}

}  // namespace fb2
}  // namespace util
