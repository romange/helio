//
// Copyright (C) 1999 and onwards Google, Inc.
//
//
// These are weird things we need to do to get this compiling on
// random systems (and on SWIG).

#ifndef BASE_PORT_H_
#define BASE_PORT_H_

#include <limits.h>         // So we can set the bounds of our types
#include <string.h>         // for memcpy()
#include <stdlib.h>         // for free()

#if defined(__APPLE__)
#include <unistd.h>         // for getpagesize() on mac
#elif defined(OS_CYGWIN)
#include <malloc.h>         // for memalign()
#endif

#include "base/integral_types.h"

// Must happens before inttypes.h inclusion */
#if defined(__APPLE__)
/* From MacOSX's inttypes.h:
 * "C++ implementations should define these macros only when
 *  __STDC_FORMAT_MACROS is defined before <inttypes.h> is included." */
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif  /* __STDC_FORMAT_MACROS */
#endif  /* __APPLE__ */

/* Default for most OSes */
/* We use SIGPWR since that seems unlikely to be used for other reasons. */
#define GOOGLE_OBSCURE_SIGNAL  SIGPWR

#if defined OS_LINUX || defined OS_CYGWIN

// _BIG_ENDIAN
// #include <endian.h>

#if defined(__cplusplus)
#include <cstddef>              // For _GLIBCXX macros
#endif

#if !defined(HAVE_TLS) && defined(_GLIBCXX_HAVE_TLS) && defined(__x86_64__)
#define HAVE_TLS 1
#endif

#elif defined OS_FREEBSD

// _BIG_ENDIAN
#include <machine/endian.h>

#elif defined __APPLE__

// BIG_ENDIAN
#include <machine/endian.h>  // NOLINT(build/include)
/* Let's try and follow the Linux convention */
#define __BYTE_ORDER  BYTE_ORDER
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BIG_ENDIAN BIG_ENDIAN

#endif

// The following guarenty declaration of the byte swap functions, and
// define __BYTE_ORDER for MSVC
#ifdef _MSC_VER
#include <stdlib.h>  // NOLINT(build/include)
#define __BYTE_ORDER __LITTLE_ENDIAN
#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__APPLE__)
// Mac OS X / Darwin features
#include <libkern/OSByteOrder.h>
#define bswap_16(x) OSSwapInt16(x)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)

#elif defined(__GLIBC__)
#include <byteswap.h>  // IWYU pragma: export

#else

static inline uint16 bswap_16(uint16 x) {
  return ((x & 0xFF) << 8) | ((x & 0xFF00) >> 8);
}
#define bswap_16(x) bswap_16(x)
static inline uint32 bswap_32(uint32 x) {
  return (((x & 0xFF) << 24) |
          ((x & 0xFF00) << 8) |
          ((x & 0xFF0000) >> 8) |
          ((x & 0xFF000000) >> 24));
}
#define bswap_32(x) bswap_32(x)
static inline uint64 bswap_64(uint64 x) {
  return (((x & GG_ULONGLONG(0xFF)) << 56) |
          ((x & GG_ULONGLONG(0xFF00)) << 40) |
          ((x & GG_ULONGLONG(0xFF0000)) << 24) |
          ((x & GG_ULONGLONG(0xFF000000)) << 8) |
          ((x & GG_ULONGLONG(0xFF00000000)) >> 8) |
          ((x & GG_ULONGLONG(0xFF0000000000)) >> 24) |
          ((x & GG_ULONGLONG(0xFF000000000000)) >> 40) |
          ((x & GG_ULONGLONG(0xFF00000000000000)) >> 56));
}
#define bswap_64(x) bswap_64(x)

#endif


// define the macros IS_LITTLE_ENDIAN or IS_BIG_ENDIAN
// using the above endian definitions from endian.h if
// endian.h was included
#ifdef __BYTE_ORDER
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
#define IS_BIG_ENDIAN
#endif

#else

#if defined(__LITTLE_ENDIAN__)
#define IS_LITTLE_ENDIAN
#elif defined(__BIG_ENDIAN__)
#define IS_BIG_ENDIAN
#endif

// there is also PDP endian ...

#endif  // __BYTE_ORDER

// Define the OS's path separator
#ifdef __cplusplus  // C won't merge duplicate const variables at link time
// Some headers provide a macro for this (GCC's system.h), remove it so that we
// can use our own.

// GCC-specific features

#if (defined(__GNUC__) || defined(__APPLE__) || defined(__clang__)) && !defined(SWIG)
#define MUST_USE_RESULT __attribute__ ((warn_unused_result))

//
// Tell the compiler to do printf format string checking if the
// compiler supports it; see the 'format' attribute in
// <http://gcc.gnu.org/onlinedocs/gcc-4.3.0/gcc/Function-Attributes.html>.
//
// N.B.: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
#define PRINTF_ATTRIBUTE(string_index, first_to_check) \
    __attribute__((__format__ (__printf__, string_index, first_to_check)))
#define SCANF_ATTRIBUTE(string_index, first_to_check) \
    __attribute__((__format__ (__scanf__, string_index, first_to_check)))

//
// Prevent the compiler from padding a structure to natural alignment
//
#define PACKED __attribute__ ((packed))

// Cache line alignment
#if defined(__i386__) || defined(__x86_64__)
#define CACHELINE_SIZE 64
#elif defined(__powerpc64__)
// TODO(user) This is the L1 D-cache line size of our Power7 machines.
// Need to check if this is appropriate for other PowerPC64 systems.
#define CACHELINE_SIZE 128
#elif defined(__arm__)
// Cache line sizes for ARM: These values are not strictly correct since
// cache line sizes depend on implementations, not architectures.  There
// are even implementations with cache line sizes configurable at boot
// time.
#if defined(__ARM_ARCH_5T__)
#define CACHELINE_SIZE 32
#elif defined(__ARM_ARCH_7A__)
#define CACHELINE_SIZE 64
#endif
#endif

// This is a NOP if CACHELINE_SIZE is not defined.
#ifdef CACHELINE_SIZE
#define CACHELINE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#else
#define CACHELINE_ALIGNED
#endif

//
// Prevent the compiler from complaining about or optimizing away variables
// that appear unused
// (careful, others e.g. third_party/libxml/xmlversion.h also define this)
#undef ATTRIBUTE_UNUSED
#define ATTRIBUTE_UNUSED __attribute__ ((unused))

//
// For functions we want to force inline or not inline.
// Introduced in gcc 3.1.
#undef ATTRIBUTE_ALWAYS_INLINE
#undef ATTRIBUTE_NOINLINE

#define ATTRIBUTE_ALWAYS_INLINE  inline __attribute__ ((always_inline))
#define HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))
#define HAVE_ATTRIBUTE_NOINLINE 1

// For weak functions
#undef ATTRIBUTE_WEAK
#define ATTRIBUTE_WEAK __attribute__ ((weak))
#define HAVE_ATTRIBUTE_WEAK 1

//
// Tell the compiler that some function parameters should be non-null pointers.
// Note: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
#define ATTRIBUTE_NONNULL(arg_index) __attribute__((nonnull(arg_index)))

//
// Tell the compiler that a given function never returns
//
#define ATTRIBUTE_NORETURN __attribute__((noreturn))

// Tell AddressSanitizer (or other memory testing tools) to ignore a given
// function. Useful for cases when a function reads random locations on stack,
// calls _exit from a cloned subprocess, deliberately accesses buffer
// out of bounds or does other scary things with memory.
#ifdef ADDRESS_SANITIZER
#define ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS \
    __attribute__((no_address_safety_analysis))
#else
#define ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS
#endif


#ifndef HAVE_ATTRIBUTE_SECTION  // may have been pre-set to 0, e.g. for Darwin
#define HAVE_ATTRIBUTE_SECTION 1
#endif

#if HAVE_ATTRIBUTE_SECTION  // define section support for the case of GCC

//
// Tell the compiler/linker to put a given function into a section and define
// "__start_ ## name" and "__stop_ ## name" symbols to bracket the section.
// Sections can not span more than none compilation unit.
// This functionality is supported by GNU linker.
// Any function with ATTRIBUTE_SECTION must not be inlined, or it will
// be placed into whatever section its caller is placed into.
//
#ifndef ATTRIBUTE_SECTION
#define ATTRIBUTE_SECTION(name) \
  __attribute__ ((section (#name))) __attribute__ ((noinline))
#endif

//
// Weak section declaration to be used as a global declaration
// for ATTRIBUTE_SECTION_START|STOP(name) to compile and link
// even without functions with ATTRIBUTE_SECTION(name).
// DEFINE_ATTRIBUTE_SECTION should be in the exactly one file; it's
// a no-op on ELF but not on Mach-O.
//
#ifndef DECLARE_ATTRIBUTE_SECTION_VARS
#define DECLARE_ATTRIBUTE_SECTION_VARS(name) \
  extern char __start_##name[] ATTRIBUTE_WEAK; \
  extern char __stop_##name[] ATTRIBUTE_WEAK
#endif
#ifndef DEFINE_ATTRIBUTE_SECTION_VARS
#define INIT_ATTRIBUTE_SECTION_VARS(name)
#define DEFINE_ATTRIBUTE_SECTION_VARS(name)
#endif

//
// Return void* pointers to start/end of a section of code with
// functions having ATTRIBUTE_SECTION(name).
// Returns 0 if no such functions exits.
// One must DECLARE_ATTRIBUTE_SECTION_VARS(name) for this to compile and link.
//
#define ATTRIBUTE_SECTION_START(name) (reinterpret_cast<void*>(__start_##name))
#define ATTRIBUTE_SECTION_STOP(name) (reinterpret_cast<void*>(__stop_##name))

#endif  // HAVE_ATTRIBUTE_SECTION

//
// The legacy prod71 libc does not provide the stack alignment required for use
// of SSE intrinsics.  In order to properly use the intrinsics you need to use
// a trampoline function which aligns the stack prior to calling your code,
// or as of crosstool v10 with gcc 4.2.0 there is an attribute which asks
// gcc to do this for you.
//
// It has also been discovered that crosstool up to and including v10 does not
// provide proper alignment for pthread_once() functions in x86-64 code either.
// Unfortunately gcc does not provide force_align_arg_pointer as an option in
// x86-64 code, so this requires us to always have a trampoline.
//
// For an example of using this see util/hash/adler32*

#if defined(__i386__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC __attribute__((force_align_arg_pointer))
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#elif defined(__i386__) || defined(__x86_64__)
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (1)
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#else
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#endif



#if defined(__GNUC__)
// Defined behavior on some of the uarchs:
// PREFETCH_HINT_T0:
//   prefetch to all levels of the hierarchy (except on p4: prefetch to L2)
// PREFETCH_HINT_NTA:
//   p4: fetch to L2, but limit to 1 way (out of the 8 ways)
//   core: skip L2, go directly to L1
//   k8 rev E and later: skip L2, can go to either of the 2-ways in L1
enum PrefetchHint {
  PREFETCH_HINT_T0 = 3,  // More temporal locality
  PREFETCH_HINT_T1 = 2,
  PREFETCH_HINT_T2 = 1,  // Less temporal locality
  PREFETCH_HINT_NTA = 0  // No temporal locality
};
#else
// prefetch is a no-op for this target. Feel free to add more sections above.
#endif

extern inline void prefetch(const char *x, int hint) {
#if defined(__llvm__)
  // In the gcc version of prefetch(), hint is only a constant _after_ inlining
  // (assumed to have been successful).  llvm views things differently, and
  // checks constant-ness _before_ inlining.  This leads to compilation errors
  // with using the other version of this code with llvm.
  //
  // One way round this is to use a switch statement to explicitly match
  // prefetch hint enumerations, and invoke __builtin_prefetch for each valid
  // value.  llvm's optimization removes the switch and unused case statements
  // after inlining, so that this boils down in the end to the same as for gcc;
  // that is, a single inlined prefetchX instruction.
  //
  // Note that this version of prefetch() cannot verify constant-ness of hint.
  // If client code calls prefetch() with a variable value for hint, it will
  // receive the full expansion of the switch below, perhaps also not inlined.
  // This should however not be a problem in the general case of well behaved
  // caller code that uses the supplied prefetch hint enumerations.
  switch (hint) {
    case PREFETCH_HINT_T0:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T0);
      break;
    case PREFETCH_HINT_T1:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T1);
      break;
    case PREFETCH_HINT_T2:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T2);
      break;
    case PREFETCH_HINT_NTA:
      __builtin_prefetch(x, 0, PREFETCH_HINT_NTA);
      break;
    default:
      __builtin_prefetch(x);
      break;
  }
#elif defined(__GNUC__)
 #if !defined(__i386) || defined(__SSE__)
  if (__builtin_constant_p(hint)) {
    __builtin_prefetch(x, 0, hint);
  } else {
    // Defaults to PREFETCH_HINT_T0
    __builtin_prefetch(x);
  }
#else
  // We want a __builtin_prefetch, but we build with the default -march=i386
  // where __builtin_prefetch quietly turns into nothing.
  // Once we crank up to -march=pentium3 or higher the __SSE__
  // clause above will kick in with the builtin.
  // -- mec 2006-06-06
  if (hint == PREFETCH_HINT_NTA)
    __asm__ __volatile__("prefetchnta (%0)" : : "r"(x));
 #endif
#else
  // You get no effect.  Feel free to add more sections above.
#endif
}

#ifdef __cplusplus
// prefetch intrinsic (bring data to L1 without polluting L2 cache)
extern inline void prefetch(const char *x) {
  return prefetch(x, 0);
}
#endif  // ifdef __cplusplus

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x
#endif

//
// Tell GCC that a function is hot or cold. GCC can use this information to
// improve static analysis, i.e. a conditional branch to a cold function
// is likely to be not-taken.
// This annotation is used for function declarations, e.g.:
//   int foo() ATTRIBUTE_HOT;
//
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#define ATTRIBUTE_HOT __attribute__ ((hot))
#define ATTRIBUTE_COLD __attribute__ ((cold))
#else
#define ATTRIBUTE_HOT
#define ATTRIBUTE_COLD
#endif

#define FTELLO ftello
#define FSEEKO fseeko

#if !defined(__cplusplus) && !defined(__APPLE__) && !defined(OS_CYGWIN)
// stdlib.h only declares this in C++, not in C, so we declare it here.
// Also make sure to avoid declaring it on platforms which don't support it.
extern int posix_memalign(void **memptr, size_t alignment, size_t size);
#endif

inline void *aligned_malloc(size_t size, int minimum_alignment) {
#if defined(__APPLE__)
  // mac lacks memalign(), posix_memalign(), however, according to
  // http://stackoverflow.com/questions/196329/osx-lacks-memalign
  // mac allocs are already 16-byte aligned.
  if (minimum_alignment <= 16)
    return malloc(size);
  // next, try to return page-aligned memory. perhaps overkill
  if (minimum_alignment <= getpagesize())
    return valloc(size);
  // give up
  return NULL;
#elif defined(OS_CYGWIN)
  return memalign(minimum_alignment, size);
#else  // !__APPLE__ && !OS_CYGWIN
  void *ptr = NULL;
  if (posix_memalign(&ptr, minimum_alignment, size) != 0)
    return NULL;
  else
    return ptr;
#endif
}

inline void aligned_free(void *aligned_memory) {
  free(aligned_memory);
}

#else   // not GCC

#define PRINTF_ATTRIBUTE(string_index, first_to_check)
#define SCANF_ATTRIBUTE(string_index, first_to_check)
#define PACKED
#define CACHELINE_ALIGNED
#define ATTRIBUTE_UNUSED
#define ATTRIBUTE_ALWAYS_INLINE
#define ATTRIBUTE_NOINLINE
#define ATTRIBUTE_HOT
#define ATTRIBUTE_COLD
#define ATTRIBUTE_WEAK
#define HAVE_ATTRIBUTE_WEAK 0
#define ATTRIBUTE_INITIAL_EXEC
#define ATTRIBUTE_NONNULL(arg_index)
#define ATTRIBUTE_NORETURN
#define HAVE_ATTRIBUTE_SECTION 0
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#define MUST_USE_RESULT
extern inline void prefetch(const char *x) {}
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x

// These should be redefined appropriately if better alternatives to
// ftell/fseek exist in the compiler
#define FTELLO ftell
#define FSEEKO fseek

#endif  // GCC


#if !HAVE_ATTRIBUTE_SECTION  // provide dummy definitions

#define ATTRIBUTE_SECTION(name)
#define INIT_ATTRIBUTE_SECTION_VARS(name)
#define DEFINE_ATTRIBUTE_SECTION_VARS(name)
#define DECLARE_ATTRIBUTE_SECTION_VARS(name)
#define ATTRIBUTE_SECTION_START(name) (reinterpret_cast<void*>(0))
#define ATTRIBUTE_SECTION_STOP(name) (reinterpret_cast<void*>(0))

#endif  // !HAVE_ATTRIBUTE_SECTION



// create macros in which the programmer should enclose all specializations
// for hash_maps and hash_sets. This is necessary since these classes are not
// STL standardized. Depending on the STL implementation they are in different
// namespaces. Right now the right namespace is passed by the Makefile
// Examples: gcc3: -DHASH_NAMESPACE=__gnu_cxx
//           icc:  -DHASH_NAMESPACE=std
//           gcc2: empty

#ifndef HASH_NAMESPACE
#  define HASH_NAMESPACE_DECLARATION_START
#  define HASH_NAMESPACE_DECLARATION_END
#else
#  define HASH_NAMESPACE_DECLARATION_START  namespace HASH_NAMESPACE {
#  define HASH_NAMESPACE_DECLARATION_END    }
#endif

// Our STL-like classes use __STD.
#if defined(__GNUC__) || defined(__APPLE__) || defined(_MSC_VER)
#define __STD std
#endif


// ROMAN: I disabled direct direct access with reinterpret_cast due to
// problems with unaligned access and simd optimizations.
// It seems that's the only alloweable solution. Modern compilers are smart enough
// to eliminate memcpy.
inline uint16 UNALIGNED_LOAD16(const void *p) {
  uint16 t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32 UNALIGNED_LOAD32(const void *p) {
  uint32 t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64 UNALIGNED_LOAD64(const void *p) {
  uint64 t;
  memcpy(&t, p, sizeof t);
  return t;
}


inline void UNALIGNED_STORE16(void *p, uint16 v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE32(void *p, uint32 v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE64(void *p, uint64 v) {
  memcpy(p, &v, sizeof v);
}

// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.

#if defined(__i386) || defined(ARCH_ATHLON) || defined(__x86_64__) || defined(_ARCH_PPC)

// x86 and x86-64 can perform unaligned loads/stores directly;
// modern PowerPC hardware can also do unaligned integer loads and stores;
// but note: the FPU still sends unaligned loads and stores to a trap handler!

// #define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16 *>(_p))
// #define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32 *>(_p))
// #define UNALIGNED_LOAD64(_p) (*reinterpret_cast<const uint64 *>(_p))

// #define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16 *>(_p) = (_val))
// #define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32 *>(_p) = (_val))

#elif defined(__arm__) && \
      !defined(__ARM_ARCH_5__) && \
      !defined(__ARM_ARCH_5T__) && \
      !defined(__ARM_ARCH_5TE__) && \
      !defined(__ARM_ARCH_5TEJ__) && \
      !defined(__ARM_ARCH_6__) && \
      !defined(__ARM_ARCH_6J__) && \
      !defined(__ARM_ARCH_6K__) && \
      !defined(__ARM_ARCH_6Z__) && \
      !defined(__ARM_ARCH_6ZK__) && \
      !defined(__ARM_ARCH_6T2__)

// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// “ARMv7 or higher”, so we have to filter away all ARMv5 and ARMv6
// sub-architectures. Newer gcc (>= 4.6) set an __ARM_FEATURE_ALIGNED #define,
// so in time, maybe we can move on to that.
//
// This is a mess, but there's not much we can do about it.

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16 *>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32 *>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16 *>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32 *>(_p) = (_val))


#else

#define NEED_ALIGNED_LOADS

// These functions are provided for architectures that don't support
// unaligned loads and stores.


#endif

#ifdef _LP64
#define UNALIGNED_LOADW(_p) UNALIGNED_LOAD64(_p)
#define UNALIGNED_STOREW(_p, _val) UNALIGNED_STORE64(_p, _val)
#else
#define UNALIGNED_LOADW(_p) UNALIGNED_LOAD32(_p)
#define UNALIGNED_STOREW(_p, _val) UNALIGNED_STORE32(_p, _val)
#endif

// NOTE(user): These are only exported to C++ because the macros they depend on
// use C++-only syntax. This #ifdef can be removed if/when the macros are fixed.

#if defined(__cplusplus)

inline void UnalignedCopy16(const void *src, void *dst) {
  UNALIGNED_STORE16(dst, UNALIGNED_LOAD16(src));
}

inline void UnalignedCopy32(const void *src, void *dst) {
  UNALIGNED_STORE32(dst, UNALIGNED_LOAD32(src));
}

inline void UnalignedCopy64(const void *src, void *dst) {
  if (sizeof(void *) == 8) {
    UNALIGNED_STORE64(dst, UNALIGNED_LOAD64(src));
  } else {
    const char *src_char = reinterpret_cast<const char *>(src);
    char *dst_char = reinterpret_cast<char *>(dst);

    UNALIGNED_STORE32(dst_char, UNALIGNED_LOAD32(src_char));
    UNALIGNED_STORE32(dst_char + 4, UNALIGNED_LOAD32(src_char + 4));
  }
}

#endif  // defined(__cpluscplus)

// printf macros for size_t, in the style of inttypes.h
#ifdef _LP64
#define __PRIS_PREFIX "z"
#else
#define __PRIS_PREFIX
#endif

// Use these macros after a % in a printf format string
// to get correct 32/64 bit behavior, like this:
// size_t size = records.size();
// printf("%" PRIuS "\n", size);

#define PRIdS __PRIS_PREFIX "d"
#define PRIxS __PRIS_PREFIX "x"
#define PRIuS __PRIS_PREFIX "u"
#define PRIXS __PRIS_PREFIX "X"
#define PRIoS __PRIS_PREFIX "o"

#define GPRIuPTHREAD "lu"
#define GPRIxPTHREAD "lx"
#ifdef OS_CYGWIN
#define PRINTABLE_PTHREAD(pthreadt) reinterpret_cast<uintptr_t>(pthreadt)
#else
#define PRINTABLE_PTHREAD(pthreadt) pthreadt
#endif


#define LANG_CXX11 1

// On some platforms, a "function pointer" points to a function descriptor
// rather than directly to the function itself.  Use FUNC_PTR_TO_CHAR_PTR(func)
// to get a char-pointer to the first instruction of the function func.
#if defined(__powerpc__) || defined(__ia64)
// use opd section for function descriptors on these platforms, the function
// address is the first word of the descriptor
enum { kPlatformUsesOPDSections = 1 };
#define FUNC_PTR_TO_CHAR_PTR(func) (reinterpret_cast<char**>(func)[0])
#else
enum { kPlatformUsesOPDSections = 0 };
#define FUNC_PTR_TO_CHAR_PTR(func)  (reinterpret_cast<char *>(func))
#endif

// Private implementation detail: __has_extension is useful to implement
// static_assert, and defining it for all toolchains avoids an extra level of
// nesting of #if/#ifdef/#ifndef.
#ifndef __has_extension
#define __has_extension(x) 0  // MSVC 10's preprocessor can't handle 'false'.
#endif


#endif  // __cplusplus

#endif  // BASE_PORT_H_
