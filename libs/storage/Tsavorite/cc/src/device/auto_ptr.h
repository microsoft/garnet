// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>

#include "alloc.h"
#include "lss_allocator.h"

#ifdef _WIN32
#include <intrin.h>
#pragma intrinsic(_BitScanReverse64)
#else
namespace FASTER {
/// Convert GCC's __builtin_clzl() to Microsoft's _BitScanReverse64().
inline uint8_t _BitScanReverse64(unsigned long* index, uint64_t mask) {
  bool found = mask > 0;
  *index = 63 - __builtin_clzl(mask);
  return found;
}
}
#endif

/// Wrappers for C++ std::unique_ptr<>.

namespace FASTER {
namespace core {

/// Round the specified size up to the next power of 2.
inline size_t next_power_of_two(size_t size) {
  assert(size > 0);
  // BSR returns the index k of the most-significant 1 bit. So 2^(k+1) > (size - 1) >= 2^k,
  // which means 2^(k+1) >= size > 2^k.
  unsigned long k;
  uint8_t found = _BitScanReverse64(&k, size - 1);
  return (uint64_t)1 << (found  * (k + 1));
}

/// Pad alignment to specified. Declared "constexpr" so that the calculation can be performed at
/// compile time, assuming parameters "size" and "alignment" are known then.
constexpr inline size_t pad_alignment(size_t size, size_t alignment) {
  assert(alignment > 0);
  // Function implemented only for powers of 2.
  assert((alignment & (alignment - 1)) == 0);
  size_t max_padding = alignment - 1;
  return (size + max_padding) & ~max_padding;
}

/// Pad alignment to specified type.
template <typename T>
constexpr inline size_t pad_alignment(size_t size) {
  return pad_alignment(size, alignof(T));
}

/// Defined in C++ 14; copying the definition here for older compilers.
template <typename T>
using remove_const_t = typename std::remove_const<T>::type;

/// alloc_aligned(): allocate a unique_ptr with a particular alignment.
template <typename T>
void unique_ptr_aligned_deleter(T* p) {
  auto q = const_cast<remove_const_t<T>*>(p);
  q->~T();
  aligned_free(q);
}

template <typename T>
struct AlignedDeleter {
  void operator()(T* p) const {
    unique_ptr_aligned_deleter(p);
  }
};

template <typename T>
using aligned_unique_ptr_t = std::unique_ptr<T, AlignedDeleter<T>>;
static_assert(sizeof(aligned_unique_ptr_t<void>) == 8, "sizeof(unique_aligned_ptr_t)");

template <typename T>
aligned_unique_ptr_t<T> make_aligned_unique_ptr(T* p) {
  return aligned_unique_ptr_t<T>(p, AlignedDeleter<T>());
}

template <typename T>
aligned_unique_ptr_t<T> alloc_aligned(size_t alignment, size_t size) {
  return make_aligned_unique_ptr<T>(reinterpret_cast<T*>(aligned_alloc(alignment, size)));
}

/// alloc_context(): allocate a small chunk of memory for a callback context.
template <typename T>
void unique_ptr_context_deleter(T* p) {
  auto q = const_cast<remove_const_t<T>*>(p);
  q->~T();
  lss_allocator.Free(q);
}

template <typename T>
struct ContextDeleter {
  void operator()(T* p) const {
    unique_ptr_context_deleter(p);
  }
};

template <typename T>
using context_unique_ptr_t = std::unique_ptr<T, ContextDeleter<T>>;
static_assert(sizeof(context_unique_ptr_t<void>) == 8, "sizeof(context_unique_ptr_t)");

template <typename T>
context_unique_ptr_t<T> make_context_unique_ptr(T* p) {
  return context_unique_ptr_t<T>(p, ContextDeleter<T>());
}

template <typename T>
context_unique_ptr_t<T> alloc_context(uint32_t size) {
  return make_context_unique_ptr<T>(reinterpret_cast<T*>(lss_allocator.Allocate(size)));
}

}
} // namespace FASTER::core
