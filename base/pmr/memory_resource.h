#pragma once
/// A workaround for libc++ versions with only experimental memory_resource support.

#include <version>

#if defined(__clang__) && !defined(__cpp_lib_memory_resource)
#include <experimental/memory_resource>
namespace PMR_NS = std::experimental::pmr;
#else
#include <memory_resource>
namespace PMR_NS = std::pmr;
#endif
