// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

namespace Tsavorite.core
{
    internal static class Constants
    {
        /// Size of cache line in bytes
        public const int kCacheLineBytes = 64;

        // RecordInfo has a long field, so it should be aligned to 8-bytes
        public const int kRecordAlignment = 8;
        public const int kRecordAlignmentMask = kRecordAlignment - 1;

        /// Number of entries per bucket (assuming 8-byte entries to fill a cacheline)
        /// Number of bits per bucket (assuming 8-byte entries to fill a cacheline)
        public const int kBitsPerBucket = 3;
        public const int kEntriesPerBucket = 1 << kBitsPerBucket;

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

        /// Number of times to spin before awaiting or Waiting for a Flush Task.
        public const long kFlushSpinCount = 10;         // TODO verify this number

        /// Number of merge/split chunks.
        public const int kNumMergeChunkBits = 8;
        public const int kNumMergeChunks = 1 << kNumMergeChunkBits;

        // Size of chunks for garbage collection
        public const int kSizeofChunkBits = 14;
        public const int kSizeofChunk = 1 << 14;
    }
}