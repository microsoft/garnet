// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#ifdef _DEBUG
#include <cstring>
#endif

#include "status.h"
#include "thread.h"

/// A fast allocator intended for mostly-FIFO workloads (e.g., allocating contexts for file-I/O
/// callbacks). Each thread allocates by bumping the tail of its current segment; when it fills a
/// segment, it malloc()s a new one. Any thread frees by decrementing the allocation's segment's
/// ref count; when a (filled) segment's ref count reaches 0, we free() it. So long as the workload
/// is mostly FIFO, we don't leak memory.

namespace FASTER {
namespace core {

/// Internal classes and structures.
namespace lss_memory {

/// Size of each segment (in bytes). (In experiments, a segment size of 16,000 worked well for
/// on Windows, while 8,000 worked well on Linux.)
#ifdef _WIN32
static constexpr uint32_t kSegmentSize = 16000;
#else
static constexpr uint32_t kSegmentSize = 8000;
#endif

/// Preserving Windows malloc() behavior, all LSS allocations are aligned to 16 bytes.
static constexpr uint32_t kBaseAlignment = 16;

/// Header, prepended to all allocated blocks; used to find the ref count variable, to decrement it
/// when the block is freed. (The allocation size isn't needed, since LSS allocations are
/// essentially stack allocations; but _DEBUG mode includes it for the benefit of the caller.)
#ifdef _DEBUG
struct alignas(8) Header {
  Header(uint32_t size_, uint32_t offset_)
    : offset{ offset_ }
    , size{ size_ } {
  }

  /// Offset from the head of the segment allocator's buffer to the memory block.
  uint32_t offset;

  /// Size of the memory block.
  uint32_t size;
};
static_assert(sizeof(Header) == 8, "Header is not 8 bytes!");
#else
struct Header {
  Header(uint16_t offset_)
    : offset{ offset_ } {
  }

  /// Offset from the head of the segment allocator's buffer to the memory block.
  uint16_t offset;
};
static_assert(sizeof(Header) == 2, "Header is not 2 bytes!");
#endif

class ThreadAllocator;

class SegmentState {
 public:
  SegmentState()
    : control{ 0 } {
  }

  SegmentState(uint64_t control_)
    : control{ control_ } {
  }

  SegmentState(uint32_t allocations_, uint32_t frees_)
    : frees{ frees_ }
    , allocations{ allocations_ } {
  }

  union {
      struct {
        /// Count of memory blocks freed inside this segment. Incremented on each free. Frees can
        /// take place on any thread.
        uint32_t frees;
        /// If this segment is sealed, then the count of memory blocks allocated inside this
        /// segment. Otherwise, zero.
        uint32_t allocations;
      };
      /// 64-bit control field, used so that threads can read the allocation count atomically at
      /// the same time they increment the free count atomically.
      std::atomic<uint64_t> control;
    };
};
static_assert(sizeof(SegmentState) == 8, "sizeof(SegmentState) != 8");
static_assert(kSegmentSize < UINT16_MAX / 2, "kSegmentSize too large for offset size!");

/// Allocation takes place inside segments. When a segment is no longer needed, we add it to the
/// garbage list.
class SegmentAllocator {
 public:
  /// Offset from the head of the class to the head of its buffer_ field.
#ifdef _DEBUG
  static constexpr uint32_t kBufferOffset = 8;
#else
  static constexpr uint32_t kBufferOffset = 14;
#endif

  /// Initialize the segment allocator and allocate the segment.
  SegmentAllocator()
    : state{} {
#ifdef _DEBUG
    // Debug LSS memory codes:
    //  - 0xBA - initialized, not allocated.
    std::memset(buffer, 0xBA, kSegmentSize);
#endif
  }

  /// Free the specified memory block. The block must be inside this segment! Returns true if the
  /// segment was freed; otherwise, returns false.
  void Free(void* bytes);

  /// Seal the segment--no more blocks will be allocated inside this segment. Returns true if the
  /// segment was freed; otherwise, returns false.
  void Seal(uint32_t blocks_allocated);

 private:
  /// Decrement the active references count, effectively freeing one allocation. Also frees the
  /// segment if (1) it is sealed and (2) its active references count is now zero. Returns true if
  /// the segment was freed; otherwise, returns false.
  void Free();

 public:
  /// Segment allocator state (8 bytes).
  SegmentState state;

  /// Padding, as needed, so that the first user allocation, at buffer_[sizeof(Header)] is 16-byte
  /// aligned.
  /// (In _DEBUG builds, sizeof(Header) == 8, so we require 0 bytes padding; in release builds,
  /// sizeof(Header) == 2, so we require 6 bytes padding.)
 private:
#ifdef _DEBUG
#else
  uint8_t padding_[6];
#endif

 public:
  /// This segment's memory. (First allocation's 8-byte Header starts at 8 (mod 16), so the
  /// allocation's contents will start at 0 (mod 16), as desired.)
  uint8_t buffer[kSegmentSize];
};

/// Allocator for a single thread. Allocates only; frees are directed by the global allocator
/// object directly to the relevant segment allocator.
class alignas(64) ThreadAllocator {
 public:
  static constexpr uint32_t kCacheLineSize = 64;

  /// Initialize the thread allocator. The real work happens lazily, when Allocate() is called for
  /// the first time.
  ThreadAllocator()
    : segment_allocator_{ nullptr }
    , segment_offset_{ 0 }
    , allocations_{ 0 } {
  }

  /// Allocate a memory block of the specified size < kSegmentSize. If allocation fails, returns
  /// nullptr.
  void* Allocate(uint32_t size);
  void* AllocateAligned(uint32_t size, uint32_t offset);

 private:
  inline uint32_t Reserve(uint32_t block_size) {
    assert(block_size <= kSegmentSize);
    ++allocations_;
    uint32_t result = segment_offset_;
    assert(result <= kSegmentSize);
    segment_offset_ += block_size;
    return result;
  }

  /// Segment inside which each thread's new allocations occur (pointer, 8 bytes).
  SegmentAllocator* segment_allocator_;

  /// Offset, into the active segment, of the next allocation.
  uint32_t segment_offset_;

  /// Number of blocks allocated inside the active segment.
  uint32_t allocations_;
};
static_assert(sizeof(ThreadAllocator) == 64, "sizeof(ThreadAllocator) != 64.");

} // namespace lss_memory

/// The LSS allocator allocates memory from a log-structured store, but does not perform garbage
/// collection. Memory is allocated from segments; each segment is freed only after all of its
/// allocations have been freed. This means that if a single allocation inside a segment is still
/// alive, the entire segment is still alive.
/// The LSS allocator works well in the case where memory usage is almost FIFO. In that case, all
/// of the segment's allocations will eventually be freed, so the segment will be freed. The LSS
/// allocator is intended to replace the (synchronous) function call stack, for asynchronous
/// continuations.
class LssAllocator {
 public:
  /// Maximum number of threads supported. For each possible thread, we reserve an 8-byte
  /// ThreadAllocator; so the memory required is 8 * (kMaxThreadCount) bytes. For each actual
  /// thread, we reserve a full SegmentAllocator, of size approximately kSegmentSize.
  static constexpr size_t kMaxThreadCount = Thread::kMaxNumThreads;

  /// Size of each segment (in bytes).
  static constexpr uint32_t kSegmentSize = lss_memory::kSegmentSize;

  /// Preserving Windows malloc() behavior, all LSS allocations are aligned to 16 bytes.
  static constexpr uint32_t kBaseAlignment = lss_memory::kBaseAlignment;

  /// Initialize the LSS allocator. The real work happens lazily, when a thread calls Allocate()
  /// for the first time.
  LssAllocator() {
    for(size_t idx = 0; idx < kMaxThreadCount; ++idx) {
      thread_allocators_[idx] = lss_memory::ThreadAllocator{};
    }
  }

  /// Allocate a memory block of the specified size. Note that size must be < kSegmentSize, since
  /// the allocation will take place inside a segment. The Allocate() code is ultimately single-
  /// threaded, since we maintain a separate ThreadAllocator per thread, each with its own
  /// SegmentAllocator. If allocation fails, returns nullptr.
  void* Allocate(uint32_t size);
  void* AllocateAligned(uint32_t size, uint32_t alignment);

  /// Free the specified memory block. The Free() code is thread-safe, since the Free() request is
  /// always directed to the SegmentAllocator() that originally allocated the code--regardless of
  /// what thread it is issued from.
  void Free(void* bytes);

 private:
  /// To reduce contention (and avoid needing atomic primitives in the allocation path), we
  /// maintain a unique allocator per thread.
  lss_memory::ThreadAllocator thread_allocators_[kMaxThreadCount];
};

/// The global LSS allocator instance.
extern LssAllocator lss_allocator;

}
} // namespace FASTER::core
