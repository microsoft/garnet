// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal static class Constants
    {
        /// Size of cache line in bytes
        public const int kCacheLineBytes = 64;

        public const bool kFineGrainedHandoverRecord = false;
        public const bool kFineGrainedHandoverBucket = true;

        /// Number of entries per bucket (assuming 8-byte entries to fill a cacheline)
        /// Number of bits per bucket (assuming 8-byte entries to fill a cacheline)
        public const int kBitsPerBucket = 3;

        public const int kEntriesPerBucket = 1 << kBitsPerBucket;

        // Position of fields in hash-table entry
        public const int kTentativeBitShift = 63;

        public const long kTentativeBitMask = 1L << kTentativeBitShift;

        public const int kPendingBitShift = 62;

        public const long kPendingBitMask = 1L << kPendingBitShift;

        public const int kReadCacheBitShift = 47;
        public const long kReadCacheBitMask = 1L << kReadCacheBitShift;

        public const int kTagSize = 14;
        public const int kTagShift = 62 - kTagSize;
        public const long kTagMask = (1L << kTagSize) - 1;
        public const long kTagPositionMask = kTagMask << kTagShift;
        public const int kAddressBits = 48;
        public const long kAddressMask = (1L << kAddressBits) - 1;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;

        // Default number of entries in the lock table.
        public const int kDefaultLockTableSize = 16 * 1024;

        public const int kMaxLockSpins = 10;   // TODO verify these
        public const int kMaxReaderLockDrainSpins = kMaxLockSpins * 10;
        public const int kMaxWriterLockDrainSpins = kMaxLockSpins * 5;

        /// Invalid entry value
        public const int kInvalidEntrySlot = kEntriesPerBucket;

        /// Location of the special bucket entry
        public const long kOverflowBucketIndex = kEntriesPerBucket - 1;

        /// Invalid value in the hash table
        public const long kInvalidEntry = 0;

        /// Number of times to retry a compare-and-swap before failure
        public const long kRetryThreshold = 1000000;    // TODO unused

        /// Number of times to spin before awaiting or Waiting for a Flush Task.
        public const long kFlushSpinCount = 10;         // TODO verify this number

        /// Number of merge/split chunks.
        public const int kNumMergeChunkBits = 8;
        public const int kNumMergeChunks = 1 << kNumMergeChunkBits;

        // Size of chunks for garbage collection
        public const int kSizeofChunkBits = 14;
        public const int kSizeofChunk = 1 << 14;

        public const long kInvalidAddress = 0;
        public const long kTempInvalidAddress = 1;
        public const long kUnknownAddress = 2;
        public const int kFirstValidAddress = 64;
    }
}