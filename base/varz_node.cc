// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/varz_node.h"
#include "absl/strings/str_cat.h"

namespace base {

using namespace std;
using absl::StrAppend;

folly::RWSpinLock VarzListNode::g_varz_lock;

VarzListNode::VarzListNode(const char* name) : name_(name), prev_(nullptr) {
  folly::RWSpinLock::WriteHolder guard(g_varz_lock);

  next_ = global_list();
  if (next_) {
    next_->prev_ = this;
  }
  global_list() = this;
}

VarzListNode::~VarzListNode() {
  folly::RWSpinLock::WriteHolder guard(g_varz_lock);
  if (global_list() == this) {
    global_list() = next_;
  } else {
    if (next_) {
      next_->prev_ = prev_;
    }
    if (prev_) {
      prev_->next_ = next_;
    }
  }
}

string VarzListNode::Format(const AnyValue& av) {
  string result;

  switch (av.type) {
    case VarzValue::STRING:
      StrAppend(&result, "\"", av.str, "\"");
      break;
    case VarzValue::NUM:
    case VarzValue::TIME:
      StrAppend(&result, av.num);
      break;
    case VarzValue::DOUBLE:
      StrAppend(&result, av.dbl);
      break;
    case VarzValue::MAP:
      result.append("{ ");
      for (const auto& k_v : av.key_value_array) {
        StrAppend(&result, "\"", k_v.first, "\": ", Format(k_v.second), ",");
      }
      result.back() = ' ';
      result.append("}");
      break;
  }
  return result;
}

VarzListNode*& VarzListNode::global_list() {
  static VarzListNode* varz_global_list = nullptr;
  return varz_global_list;
}

void VarzListNode::Iterate(std::function<void(const char*, AnyValue&&)> f) {
  folly::RWSpinLock::ReadHolder guard(g_varz_lock);

  for (VarzListNode* node = global_list(); node != nullptr; node = node->next_) {
    if (node->name_ != nullptr) {
      f(node->name_, node->GetData());
    }
  }
}

}  // namespace base
