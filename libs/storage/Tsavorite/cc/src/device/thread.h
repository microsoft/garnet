// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

/// Turn this on to have Thread::current_num_threads_ keep a count of currently-active threads.
#undef COUNT_ACTIVE_THREADS

namespace FASTER {
namespace core {

/// Gives every thread a unique, numeric thread ID, and recycles IDs when threads exit.
class Thread {
 public:
  /// Number of slots in the per-thread id table (and therefore in the LightEpoch entry table,
  /// which is sized to kTableSize = kMaxNumThreads). A slot is reserved only for the duration
  /// of a single epoch-protected device call: ReadAsync / WriteAsync / RemoveSegment each call
  /// acquire_id() -> run their epoch region -> release_id(), all synchronously on the same
  /// thread. Slots are therefore RECYCLED across calls (exactly like LightEpoch reuses its
  /// thread-table entries), so this bounds the number of threads that may be *concurrently*
  /// inside the device, NOT the total number of threads that ever call it.
  ///
  /// Historically this was 96, and ReserveEntry() threw std::runtime_error("Too many threads!")
  /// once every slot was momentarily held. Because that throw originates inside an extern "C"
  /// P/Invoke entry point it terminated the whole process (std::terminate) — observed in
  /// production as the server "stops issuing I/O" and failing to shut down cleanly when enough
  /// threads (the .NET thread pool grows under a slow disk, per-connection IO threads, etc.)
  /// were concurrently blocked inside a device op (e.g. parked in the io_submit EAGAIN backoff).
  /// Device.benchmark never hit it because it drives the device from a small fixed thread set.
  ///
  /// The native device must never abort the process, so ReserveEntry() now yield-spins until a
  /// slot frees instead of throwing (see below). 256 is a generous concurrency ceiling; the
  /// table is (kMaxNumThreads + 2) * 64 bytes per LightEpoch instance and one atomic<bool> per
  /// slot here. The hot path indexes table_[id] and never scans, so size does not affect
  /// steady-state throughput; only the infrequent drain/reclaim scans are O(kMaxNumThreads).
  static constexpr size_t kMaxNumThreads = 256;

 private:
  /// Encapsulates a thread ID, getting a free ID from the Thread class when the thread starts, and
  /// releasing it back to the Thread class, when the thread exits.
  class ThreadId {
   public:
    static constexpr uint32_t kInvalidId = UINT32_MAX;

    /// The default ctor used to reserve a slot eagerly (Thread::ReserveEntry), but the only
    /// instance — the thread_local Thread::id_ (thread_manual.cc) — is constructed with the
    /// explicit ThreadId(kInvalidId) ctor and reserves lazily via acquire_id(). The reserving
    /// default ctor is therefore dead; delete it so it can never be used (it would now yield-spin
    /// instead of throwing on a full table, and double-reserve relative to acquire_id()).
    ThreadId() = delete;
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
    // Probe the table for a free slot, starting at `start` and wrapping. Each pass tries
    // every slot once via CAS. If a whole pass finds nothing free, more than kMaxNumThreads
    // threads are *concurrently* holding a slot; yield to let those holders finish their
    // (short, bounded) epoch region and release, then re-probe. We must NEVER throw here:
    // ReserveEntry runs inside extern "C" device entry points, so an exception would unwind
    // across the P/Invoke boundary and terminate the process. This mirrors LightEpoch's
    // full-table behavior (which blocks on a semaphore) with a simpler yield-spin, which is
    // appropriate because a slot is guaranteed to free as soon as any in-flight device op
    // completes its acquire_id()/release_id() pair.
    for (uint32_t backoff = 0;; ++backoff) {
      uint32_t end = start + 2 * kMaxNumThreads;
      for (uint32_t id = start; id < end; ++id) {
        bool expected = false;
        if (id_used_[id % kMaxNumThreads].compare_exchange_strong(expected, true)) {
          return id % kMaxNumThreads;
        }
      }
      // Table momentarily saturated: every slot is held by a live epoch region. Yield so the
      // holders can make progress and release a slot, then re-probe. After a yield budget,
      // sleep briefly to avoid burning a core if saturation persists (matches the bounded
      // backoff used by the device submit path on a full ring).
      if (backoff < 64)
        std::this_thread::yield();
      else
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
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

inline Thread::ThreadId::~ThreadId() {
  if (id_ != kInvalidId) Thread::ReleaseEntry(id_);
}

}
} // namespace FASTER::core
