// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

namespace Tsavorite.core
{
    internal static class Constants
    {
        /// Size of cache line in bytes
        public const int kCacheLineBytes = 64;

        /// <summary>
        /// When true (the default), the inline submitter-thread device drain reaps only the calling
        /// thread's affine completion context (<see cref="IDevice.TryCompleteMine"/>) instead of the
        /// legacy behavior that reaps a single fixed context 0 for every caller
        /// (<see cref="IDevice.TryComplete"/>).
        ///
        /// The legacy fixed-context drain serializes every inline-draining thread on context 0's
        /// kernel aio mutex: on a server whose disk-read completions are processed by a growing pool
        /// of threads (e.g. the .NET thread pool under RESP GET load), that mutex became ~46% of all
        /// CPU (osq_lock) and throughput spiralled downward as the pool grew (measured 6.75M -> 2.4M
        /// ops/s over successive runs). It also only covered 1/N of completions inline (context 0's
        /// share). Affine draining spreads the inline drain across every context — each thread reaps
        /// the context its own submits land on — which removes the storm (osq_lock 46% -> 7%) and
        /// holds throughput stable (~6.7M ops/s across many runs). It was measured NEUTRAL at the
        /// uncontended saturated peak, so defaulting it on is a strict improvement. Devices that do
        /// not shard completions fall back to <see cref="IDevice.TryComplete"/> automatically.
        /// Set GARNET_INLINE_DRAIN_AFFINE=0 to restore the legacy fixed-context-0 drain.
        /// </summary>
        public static readonly bool InlineDrainAffine =
            System.Environment.GetEnvironmentVariable("GARNET_INLINE_DRAIN_AFFINE") != "0";

        // RecordInfo has a long field, so it should be aligned to 8-bytes
        public const int kRecordAlignment = 8;
        public const int kRecordAlignmentMask = kRecordAlignment - 1;
        /// <summary>Bit-shift equivalent of <see cref="kRecordAlignment"/> (i.e., <c>1 &lt;&lt; kRecordAlignmentShift == kRecordAlignment</c>).
        /// Use this when converting between word counts and byte counts (e.g., FillerWords &lt;&lt; kRecordAlignmentShift = filler bytes).</summary>
        public const int kRecordAlignmentShift = 3;

        /// <summary>Combined fixed-size header of every log record: <see cref="RecordInfo.Size"/> (8) + <see cref="RecordDataHeader.Size"/> (8) = 16 bytes.
        /// This is the offset from the record base address to the namespace byte / extended-namespace bytes / key data,
        /// and the minimum number of bytes a scanner length-walks past when the RDH has not yet been Initialized (word == 0).</summary>
        public const int FixedHeaderSize = RecordInfo.Size + RecordDataHeader.Size;

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