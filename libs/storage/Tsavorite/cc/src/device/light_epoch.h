// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>

#include "alloc.h"
#include "async.h"
#include "constants.h"
#include "phase.h"
#include "thread.h"
#include "utility.h"

namespace FASTER {
namespace core {

class LightEpoch {
 private:
  /// Entry in epoch table
  struct alignas(Constants::kCacheLineBytes) Entry {
    Entry()
      : local_current_epoch{ 0 }
      , reentrant{ 0 }
      , phase_finished{ Phase::REST } {
    }

    uint64_t local_current_epoch;
    uint32_t reentrant;
    std::atomic<Phase> phase_finished;
  };
  static_assert(sizeof(Entry) == 64, "sizeof(Entry) != 64");

  struct EpochAction {
    typedef void(*callback_t)(IAsyncContext*);

    static constexpr uint64_t kFree = UINT64_MAX;
    static constexpr uint64_t kLocked = UINT64_MAX - 1;

    EpochAction()
      : epoch{ kFree }
      , callback{ nullptr }
      , context{ nullptr } {
    }

    void Initialize() {
      callback = nullptr;
      context = nullptr;
      epoch = kFree;
    }

    bool IsFree() const {
      return epoch.load() == kFree;
    }

    bool TryPop(uint64_t expected_epoch) {
      bool retval = epoch.compare_exchange_strong(expected_epoch, kLocked);
      if(retval) {
        callback_t callback_ = callback;
        IAsyncContext* context_ = context;
        callback = nullptr;
        context = nullptr;
        // Release the lock.
        epoch.store(kFree);
        // Perform the action.
        callback_(context_);
      }
      return retval;
    }

    bool TryPush(uint64_t prior_epoch, callback_t new_callback, IAsyncContext* new_context) {
      uint64_t expected_epoch = kFree;
      bool retval = epoch.compare_exchange_strong(expected_epoch, kLocked);
      if(retval) {
        callback = new_callback;
        context = new_context;
        // Release the lock.
        epoch.store(prior_epoch);
      }
      return retval;
    }

    bool TrySwap(uint64_t expected_epoch, uint64_t prior_epoch, callback_t new_callback,
                 IAsyncContext* new_context) {
      bool retval = epoch.compare_exchange_strong(expected_epoch, kLocked);
      if(retval) {
        callback_t existing_callback = callback;
        IAsyncContext* existing_context = context;
        callback = new_callback;
        context = new_context;
        // Release the lock.
        epoch.store(prior_epoch);
        // Perform the action.
        existing_callback(existing_context);
      }
      return retval;
    }

    /// The epoch field is atomic--always read it first and write it last.
    std::atomic<uint64_t> epoch;

    void(*callback)(IAsyncContext* context);
    IAsyncContext* context;
  };

 public:
  /// Default invalid page_index entry.
  static constexpr uint32_t kInvalidIndex = 0;
  /// This thread is not protecting any epoch.
  static constexpr uint64_t kUnprotected = 0;

 private:
  /// Default number of entries in the entries table
  static constexpr uint32_t kTableSize = Thread::kMaxNumThreads;
  /// Default drainlist size
  static constexpr uint32_t kDrainListSize = 256;
  /// Epoch table
  Entry* table_;

  /// List of action, epoch pairs containing actions to performed when an epoch becomes
  /// safe to reclaim.
  EpochAction drain_list_[kDrainListSize];
  /// Count of drain actions
  std::atomic<uint32_t> drain_count_;

 public:
  /// Current system epoch (global state)
  std::atomic<uint64_t> current_epoch;
  /// Cached value of epoch that is safe to reclaim
  std::atomic<uint64_t> safe_to_reclaim_epoch;

  LightEpoch()
    : table_{ nullptr }
    , drain_count_{ 0 }
    , drain_list_{} {
    Initialize();
  }

  ~LightEpoch() {
    Uninitialize();
  }

 private:
  void Initialize() {
    // do cache-line alignment
    table_ = reinterpret_cast<Entry*>(aligned_alloc(Constants::kCacheLineBytes,
                                      (kTableSize + 2) * sizeof(Entry)));
    new(table_) Entry[kTableSize + 2];
    current_epoch = 1;
    safe_to_reclaim_epoch = 0;
    for(uint32_t idx = 0; idx < kDrainListSize; ++idx) {
      drain_list_[idx].Initialize();
    }
    drain_count_ = 0;
  }

  void Uninitialize() {
    aligned_free(table_);
    table_ = nullptr;
    current_epoch = 1;
    safe_to_reclaim_epoch = 0;
  }

 public:
  /// Enter the thread into the protected code region
  inline uint64_t Protect() {
    uint32_t entry = Thread::id();
    table_[entry].local_current_epoch = current_epoch.load();
    return table_[entry].local_current_epoch;
  }

  /// Enter the thread into the protected code region
  /// Process entries in drain list if possible
  inline uint64_t ProtectAndDrain() {
    uint32_t entry = Thread::id();
    table_[entry].local_current_epoch = current_epoch.load();
    if(drain_count_.load() > 0) {
      Drain(table_[entry].local_current_epoch);
    }
    return table_[entry].local_current_epoch;
  }

  uint64_t ReentrantProtect() {
    uint32_t entry = Thread::id();
    if(table_[entry].local_current_epoch != kUnprotected)
      return table_[entry].local_current_epoch;
    table_[entry].local_current_epoch = current_epoch.load();
    table_[entry].reentrant++;
    return table_[entry].local_current_epoch;
  }

  inline bool IsProtected() {
    uint32_t entry = Thread::id();
    return table_[entry].local_current_epoch != kUnprotected;
  }

  /// Exit the thread from the protected code region.
  void Unprotect() {
    table_[Thread::id()].local_current_epoch = kUnprotected;
    if (drain_count_.load() > 0) {
        SuspendDrain();
    }
  }

  void SuspendDrain()
  {
      while (drain_count_.load() > 0)
      {
          for (uint32_t index = 0; index < kTableSize; ++index) {
              uint64_t entry_epoch = table_[index].local_current_epoch;
              if (entry_epoch != kUnprotected) return;
          }
          ProtectAndDrain();
          table_[Thread::id()].local_current_epoch = kUnprotected; // unprotect without drain
	  }
  }

  void ReentrantUnprotect() {
    uint32_t entry = Thread::id();
    if(--(table_[entry].reentrant) == 0) {
      table_[entry].local_current_epoch = kUnprotected;
    }
  }

  void Drain(uint64_t nextEpoch) {
    ComputeNewSafeToReclaimEpoch(nextEpoch);
    for(uint32_t idx = 0; idx < kDrainListSize; ++idx) {
      uint64_t trigger_epoch = drain_list_[idx].epoch.load();
      if(trigger_epoch <= safe_to_reclaim_epoch) {
        if(drain_list_[idx].TryPop(trigger_epoch)) {
          if(--drain_count_ == 0) {
            break;
          }
        }
      }
    }
  }

  /// Increment the current epoch (global system state)
  uint64_t BumpCurrentEpoch() {
    uint64_t nextEpoch = ++current_epoch;
    if(drain_count_ > 0) {
      Drain(nextEpoch);
    }
    return nextEpoch;
  }

  /// Increment the current epoch (global system state) and register
  /// a trigger action for when older epoch becomes safe to reclaim
  uint64_t BumpCurrentEpoch(EpochAction::callback_t callback, IAsyncContext* context) {
    uint64_t prior_epoch = BumpCurrentEpoch() - 1;
    uint32_t i = 0, j = 0;
    while(true) {
      uint64_t trigger_epoch = drain_list_[i].epoch.load();
      if(trigger_epoch == EpochAction::kFree) {
        if(drain_list_[i].TryPush(prior_epoch, callback, context)) {
          ++drain_count_;
          break;
        }
      } else if(trigger_epoch <= safe_to_reclaim_epoch.load()) {
        if(drain_list_[i].TrySwap(trigger_epoch, prior_epoch, callback, context)) {
          break;
        }
      }
      if(++i == kDrainListSize) {
        i = 0;
        if(++j == 500) {
          j = 0;
          std::this_thread::sleep_for(std::chrono::seconds(1));
          fprintf(stderr, "Slowdown: Unable to add trigger to epoch\n");
        }
      }
    }
    return prior_epoch + 1;
  }

  /// Compute latest epoch that is safe to reclaim, by scanning the epoch table
  uint64_t ComputeNewSafeToReclaimEpoch(uint64_t current_epoch_) {
    uint64_t oldest_ongoing_call = current_epoch_;
    for(uint32_t index = 0; index < kTableSize; ++index) {
      uint64_t entry_epoch = table_[index].local_current_epoch;
      if(entry_epoch != kUnprotected && entry_epoch < oldest_ongoing_call) {
        oldest_ongoing_call = entry_epoch;
      }
    }
    safe_to_reclaim_epoch = oldest_ongoing_call - 1;
    return safe_to_reclaim_epoch;
  }

  void SpinWaitForSafeToReclaim(uint64_t current_epoch_, uint64_t safe_to_reclaim_epoch_) {
    do {
      ComputeNewSafeToReclaimEpoch(current_epoch_);
    } while(safe_to_reclaim_epoch_ > safe_to_reclaim_epoch);
  }

  bool IsSafeToReclaim(uint64_t epoch) const {
    return (epoch <= safe_to_reclaim_epoch);
  }

  /// CPR checkpoint functions.
  inline void ResetPhaseFinished() {
    for(uint32_t idx = 0; idx < kTableSize; ++idx) {
      assert(table_[idx].phase_finished.load() == Phase::REST ||
             table_[idx].phase_finished.load() == Phase::INDEX_CHKPT ||
             table_[idx].phase_finished.load() == Phase::PERSISTENCE_CALLBACK ||
             table_[idx].phase_finished.load() == Phase::GC_IN_PROGRESS ||
             table_[idx].phase_finished.load() == Phase::GROW_IN_PROGRESS);
      table_[idx].phase_finished.store(Phase::REST);
    }
  }
  /// This thread has completed the specified phase.
  inline bool FinishThreadPhase(Phase phase) {
    uint32_t entry = Thread::id();
    table_[entry].phase_finished = phase;
    // Check if other threads have reported complete.
    for(uint32_t idx = 0; idx < kTableSize; ++idx) {
      Phase entry_phase = table_[idx].phase_finished.load();
      uint64_t entry_epoch = table_[idx].local_current_epoch;
      if(entry_epoch != 0 && entry_phase != phase) {
        return false;
      }
    }
    return true;
  }
  /// Has this thread completed the specified phase (i.e., is it waiting for other threads to
  /// finish the specified phase, before it can advance the global phase)?
  inline bool HasThreadFinishedPhase(Phase phase) const {
    uint32_t entry = Thread::id();
    return table_[entry].phase_finished == phase;
  }
};

}
} // namespace FASTER::core
