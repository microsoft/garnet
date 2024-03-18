// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace FASTER {
namespace core {

class Utility {
 public:
  static inline uint64_t Rotr64(uint64_t x, std::size_t n) {
    return (((x) >> n) | ((x) << (64 - n)));
  }

  static inline uint64_t GetHashCode(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return Rotr64(local_rand_hash, 43);
    //Func<long, long> hash =
    //    e => 40343 * (40343 * (40343 * (40343 * (40343 * 8 + (long)((e) & 0xFFFF)) + (long)((e >> 16) & 0xFFFF)) + (long)((e >> 32) & 0xFFFF)) + (long)(e >> 48));
  }

  static inline uint64_t HashBytes(const uint16_t* str, size_t len) {
    // 40343 is a "magic constant" that works well,
    // 38299 is another good value.
    // Both are primes and have a good distribution of bits.
    const uint64_t kMagicNum = 40343;
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kMagicNum * hashState + str[idx];
    }

    // The final scrambling helps with short keys that vary only on the high order bits.
    // Low order bits are not always well distributed so shift them to the high end, where they'll
    // form part of the 14-bit tag.
    return Rotr64(kMagicNum * hashState, 6);
  }

  static inline uint64_t HashBytesUint8(const uint8_t* str, size_t len) {
    // 40343 is a "magic constant" that works well,
    // 38299 is another good value.
    // Both are primes and have a good distribution of bits.
    const uint64_t kMagicNum = 40343;
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kMagicNum * hashState + str[idx];
    }

    // The final scrambling helps with short keys that vary only on the high order bits.
    // Low order bits are not always well distributed so shift them to the high end, where they'll
    // form part of the 14-bit tag.
    return Rotr64(kMagicNum * hashState, 6);
  }

  static constexpr inline bool IsPowerOfTwo(uint64_t x) {
    return (x > 0) && ((x & (x - 1)) == 0);
  }
};

}
} // namespace FASTER::core
