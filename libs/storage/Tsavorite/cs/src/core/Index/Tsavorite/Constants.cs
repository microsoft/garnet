// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal static class Constants
    {
        /// Size of cache line in bytes
        public const int kCacheLineBytes = 64;

        // RecordInfo has a long field, so it should be aligned to 8-bytes
        public const int kRecordAlignment = 8;

        /// Number of bits per bucket size (assuming 8-byte entries to fill a cacheline) and entries per bucket
        public const int kBitsPerBucket = 3;
        public const int kEntriesPerBucket = 1 << kBitsPerBucket;

        public const int kPendingBitShift = 62;
        public const long kPendingBitMask = 1L << kPendingBitShift;

        public const int kReadCacheBitShift = 47;
        public const long kReadCacheBitMask = 1L << kReadCacheBitShift;

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

        // Size of chunks for garbage collection
        public const int kSizeofChunkBits = 14;
        public const int kSizeofChunk = 1 << 14;

        public const long kInvalidAddress = 0;
        public const long kTempInvalidAddress = 1;
        public const int kFirstValidAddress = 64;
 
        public const int IntPtrSize = 8;
    }
}