// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdlib>

#ifdef _WIN32
#include <malloc.h>
#endif

namespace FASTER {
namespace core {

/// Windows and standard C++/Linux have incompatible implementations of aligned malloc(). (Windows
/// defines a corresponding aligned free(), while Linux relies on the ordinary free().)
inline void* aligned_alloc(size_t alignment, size_t size) {
#ifdef _WIN32
  return _aligned_malloc(size, alignment);
#else
  return ::aligned_alloc(alignment, size);
#endif
}

inline void aligned_free(void* ptr) {
#ifdef _WIN32
  _aligned_free(ptr);
#else
  ::free(ptr);
#endif
}

}
} // namespace FASTER::core

