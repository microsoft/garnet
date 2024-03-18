// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <cassert>
#include <cstdlib>

#include "alloc.h"
#include "auto_ptr.h"
#include "lss_allocator.h"
#include "thread.h"

namespace FASTER {
namespace core {

#define thread_index_ Thread::id()

LssAllocator lss_allocator{};

namespace lss_memory {

static_assert(sizeof(Header) < kBaseAlignment, "Unexpected header size!");

void SegmentAllocator::Free(void* bytes) {
#ifdef _DEBUG
  Header* header = reinterpret_cast<Header*>(bytes) - 1;
  assert(header->offset < kSegmentSize);
  assert(header->offset + header->size <= kSegmentSize);
  //  - 0xDA - freed.
  ::memset(header + 1, 0xDA, header->size);
#endif
  Free();
}

void SegmentAllocator::Seal(uint32_t allocations) {
  SegmentState delta_state{ allocations, 1 };
  SegmentState old_state{ state.control.fetch_add(delta_state.control) };
  assert(old_state.allocations == 0);
  assert(old_state.frees < allocations);
  if(allocations == old_state.frees + 1) {
    // We were the last to free a block inside this segment, so we must free it.
    this->~SegmentAllocator();
    aligned_free(this);
  }
}

void SegmentAllocator::Free() {
  SegmentState delta_state{ 0, 1 };
  SegmentState old_state{ state.control.fetch_add(delta_state.control) };
  assert(old_state.allocations == 0 || old_state.frees < old_state.allocations);
  if(old_state.allocations == old_state.frees + 1) {
    // We were the last to free a block inside this segment, so we must free it.
    this->~SegmentAllocator();
    aligned_free(this);
  }
}

void* ThreadAllocator::Allocate(uint32_t size) {
  if(!segment_allocator_) {
    segment_allocator_ = reinterpret_cast<SegmentAllocator*>(aligned_alloc(kCacheLineSize,
                         sizeof(SegmentAllocator)));
    if(!segment_allocator_) {
      return nullptr;
    }
    new(segment_allocator_) SegmentAllocator{};
  }
  // Block is 16-byte aligned, after a 2-byte (8-byte in _DEBUG mode) header.
  uint32_t block_size = static_cast<uint32_t>(pad_alignment(size + sizeof(Header),
                        kBaseAlignment));
  uint32_t offset = Reserve(block_size);
  if(segment_offset_ <= kSegmentSize) {
    // The allocation succeeded inside the active segment.
    uint8_t* buffer = segment_allocator_->buffer;
#ifdef _DEBUG
    //  - 0xCA - allocated.
    ::memset(&buffer[offset], 0xCA, block_size);
#endif
    Header* header = reinterpret_cast<Header*>(&buffer[offset]);
#ifdef _DEBUG
    new(header) Header(size, offset);
#else
    new(header) Header(offset);
#endif
    return header + 1;
  } else {
    // We filled the active segment; seal it.
    segment_allocator_->Seal(allocations_);
    segment_allocator_ = nullptr;
    allocations_ = 0;
    segment_offset_ = 0;
    // Call self recursively, to allocate inside a new segment.
    return Allocate(size);
  }
}

void* ThreadAllocator::AllocateAligned(uint32_t size, uint32_t alignment) {
  if(!segment_allocator_) {
    segment_allocator_ = reinterpret_cast<SegmentAllocator*>(aligned_alloc(kCacheLineSize,
                         sizeof(SegmentAllocator)));
    if(!segment_allocator_) {
      return nullptr;
    }
    new(segment_allocator_) SegmentAllocator{};
  }
  // Alignment must be >= base alignment, and a power of 2.
  assert(alignment >= kBaseAlignment);
  assert((alignment & (alignment - 1)) == 0);
  // Block needs to be large enough to hold the user block, the header, and the align land fill.
  // Max align land fill size is (alignment - kBaseAlignment).
  uint32_t block_size = static_cast<uint32_t>(pad_alignment(
                          size + sizeof(Header) + alignment - kBaseAlignment,
                          kBaseAlignment));
  uint32_t block_offset = Reserve(block_size);
  if(segment_offset_ <= kSegmentSize) {
    // The allocation succeeded inside the active segment.
    uint8_t* buffer = segment_allocator_->buffer;
#ifdef _DEBUG
    //  - 0xEA - align land fill.
    ::memset(&buffer[block_offset], 0xEA, block_size);
#endif
    // Align the user block.
    uint32_t user_offset = static_cast<uint32_t>(pad_alignment(reinterpret_cast<size_t>(
                             &buffer[block_offset]) + sizeof(Header), alignment) -
                           reinterpret_cast<size_t>(&buffer[block_offset]) - sizeof(Header));
    assert(user_offset + sizeof(Header) + size <= block_size);
    uint32_t offset = block_offset + user_offset;
#ifdef _DEBUG
    //  - 0xCA - allocated.
    ::memset(&buffer[offset], 0xCA, size + sizeof(Header));
#endif
    Header* header = reinterpret_cast<Header*>(&buffer[offset]);
#ifdef _DEBUG
    new(header) Header(size, offset);
#else
    new(header) Header(offset);
#endif
    return header + 1;
  } else {
    // We filled the active segment; seal it.
    segment_allocator_->Seal(allocations_);
    segment_allocator_ = nullptr;
    allocations_ = 0;
    segment_offset_ = 0;
    // Call self recursively, to allocate inside a new segment.
    return AllocateAligned(size, alignment);
  }
}
} // namespace lss_memory

void* LssAllocator::Allocate(uint32_t size) {
  return thread_allocators_[thread_index_].Allocate(size);
}

void* LssAllocator::AllocateAligned(uint32_t size, uint32_t alignment) {
  return thread_allocators_[thread_index_].AllocateAligned(size, alignment);
}

void LssAllocator::Free(void* bytes) {
  lss_memory::Header* header = reinterpret_cast<lss_memory::Header*>(bytes) - 1;
  uint8_t* block = reinterpret_cast<uint8_t*>(header);
  uint32_t offset = header->offset + lss_memory::SegmentAllocator::kBufferOffset;
  lss_memory::SegmentAllocator* segment_allocator =
    reinterpret_cast<lss_memory::SegmentAllocator*>(block - offset);
  segment_allocator->Free(bytes);
}

#undef thread_index_

}
} // namespace FASTER::core
