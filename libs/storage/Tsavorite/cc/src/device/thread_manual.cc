// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "thread.h"

namespace FASTER {
namespace core {

/// The first thread will have index 0.
std::atomic<uint32_t> Thread::next_index_{ 0 };

/// No thread IDs have been used yet.
std::atomic<bool> Thread::id_used_[kMaxNumThreads] = {};

#ifdef COUNT_ACTIVE_THREADS
std::atomic<int32_t> Thread::current_num_threads_ { 0 };
#endif

/// Give the new thread an invalid ID.
thread_local Thread::ThreadId Thread::id_{ Thread::ThreadId::kInvalidId };

// Track global thread id - used only with native device (thread_manual) for now
thread_local uint32_t Thread::thread_index_{ Thread::next_index_++ };
}
} // namespace FASTER::core
