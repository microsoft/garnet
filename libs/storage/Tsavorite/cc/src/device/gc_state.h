// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>

namespace FASTER {
namespace core {

/// State of the active garbage-collection call.
class GcState {
 public:
  typedef void(*truncate_callback_t)(uint64_t offset);
  typedef void(*complete_callback_t)(void);

  GcState()
    : truncate_callback{ nullptr }
    , complete_callback{ nullptr }
    , num_chunks{ 0 }
    , next_chunk{ 0 } {
  }

  void Initialize(truncate_callback_t truncate_callback_, complete_callback_t complete_callback_,
                  uint64_t num_chunks_) {
    truncate_callback = truncate_callback_;
    complete_callback = complete_callback_;
    num_chunks = num_chunks_;
    next_chunk = 0;
  }

  truncate_callback_t truncate_callback;
  complete_callback_t complete_callback;
  uint64_t num_chunks;
  std::atomic<uint64_t> next_chunk;
};

}
} // namespace FASTER::core
