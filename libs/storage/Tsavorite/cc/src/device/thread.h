// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

/// Turn this on to have Thread::current_num_threads_ keep a count of currently-active threads.
#undef COUNT_ACTIVE_THREADS

namespace FASTER {
namespace core {

/// Gives every thread a unique, numeric thread ID, and recycles IDs when threads exit.
class Thread {
 public:
  /// The number of entries in table. Currently, this is fixed at 96 and never changes or grows.
  /// If the table runs out of entries, then the current implementation will throw a
  /// std::runtime_error.
  static constexpr size_t kMaxNumThreads = 96;

 private:
  /// Encapsulates a thread ID, getting a free ID from the Thread class when the thread starts, and
  /// releasing it back to the Thread class, when the thread exits.
  class ThreadId {
   public:
    static constexpr uint32_t kInvalidId = UINT32_MAX;

    inline ThreadId();
    inline ThreadId(uint32_t id);
    inline ~ThreadId();

    inline uint32_t id() const {
      return id_;
    }

    inline void acquire_id() {
        id_ = Thread::ReserveEntry(thread_index_);
    }

    inline void release_id() {
        Thread::ReleaseEntry(id_);
        id_ = kInvalidId;
    }

   private:
    uint32_t id_;
  };

 public:
  /// Call static method Thread::id() to get the executing thread's ID.
  inline static uint32_t id() {
    return id_.id();
  }

  inline static void acquire_id() {
      id_.acquire_id();
  }

  inline static void release_id() {
      id_.release_id();
  }

 private:
  /// Methods ReserveEntry() and ReleaseEntry() do the real work.
  inline static uint32_t ReserveEntry(uint32_t start) {
#ifdef COUNT_ACTIVE_THREADS
    int32_t result = ++current_num_threads_;
    assert(result < kMaxNumThreads);
#endif
    uint32_t end = start + 2 * kMaxNumThreads;
    for(uint32_t id = start; id < end; ++id) {
      bool expected = false;
      if(id_used_[id % kMaxNumThreads].compare_exchange_strong(expected, true)) {
        return id % kMaxNumThreads;
      }
    }
    // Already have 64 active threads.
    throw std::runtime_error{ "Too many threads!" };
  }

  inline static void ReleaseEntry(uint32_t id) {
    assert(id != ThreadId::kInvalidId);
    assert(id_used_[id].load());
    id_used_[id] = false;
#ifdef COUNT_ACTIVE_THREADS
    int32_t result = --current_num_threads_;
#endif
  }

  /// The current thread's page_index.
  static thread_local ThreadId id_;
  static thread_local uint32_t thread_index_;

  /// Next thread index to consider.
  static std::atomic<uint32_t> next_index_;
  /// Which thread IDs have already been taken.
  static std::atomic<bool> id_used_[kMaxNumThreads];

#ifdef COUNT_ACTIVE_THREADS
  static std::atomic<int32_t> current_num_threads_;
#endif

  friend class ThreadId;
};

inline Thread::ThreadId::ThreadId(uint32_t id)
    : id_{ id } {
}

inline Thread::ThreadId::ThreadId()
  : id_{ kInvalidId } {
  id_ = Thread::ReserveEntry(next_index_++);
}

inline Thread::ThreadId::~ThreadId() {
  if (id_ != kInvalidId) Thread::ReleaseEntry(id_);
}

}
} // namespace FASTER::core
