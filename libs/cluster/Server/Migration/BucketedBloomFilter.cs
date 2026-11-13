// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// Single-writer Bloom filter with equally sized buckets in one contiguous bitmap.
    /// Callers coordinate construction and clearing with readers before publishing membership.
    /// Key overloads use MurmurHash2x64A; hash overloads allow one hash to be reused across filters.
    /// </summary>
    /// <remarks>
    /// <para>
    /// SLOTS migration sizes each worker's filter once from its configured maximum batch size, not the
    /// current batch's key count. The batch-size constructor targets 64 keys per bucket with
    /// 16384 bits per bucket and three probes. The key collection's reserved entry is excluded from the batch size.
    /// Buckets are reused and cleared between batches; scan-ahead does not allocate another bitmap.
    /// Empty filters skip clearing, and completed batches can retain their bitmap until the next rebuild.
    /// Explicitly supplied sketches and other users of this class can choose different dimensions.
    /// </para>
    /// <code>
    /// N = configured maximum keys per batch, per worker
    /// B = NextPowerOfTwo(Ceiling(N / 64)) buckets
    /// S = 16384 bits per bucket; k = 3 probes
    /// Bitmap per worker = B * S / 8 = B * 2048 bytes
    /// Bitmap for 16 workers = 16 * B * 2048 bytes
    /// Generation tags per worker = B * sizeof(uint) = B * 4 bytes
    ///
    /// At power-of-two batch sizes, the bitmap budgets 256 bits (32 bytes) per key.
    /// Rounding other batch sizes up keeps the average occupancy at most 64 keys per bucket;
    /// individual buckets can contain more keys. This is not a per-bucket capacity limit.
    /// </code>
    /// <para>
    /// The comparison global bitmap uses M = NextPowerOfTwo(N * 256) bits and two probes.
    /// Across the supported batch-size range [1024, 262143], it consumes the same bitmap memory.
    /// Its false-positive rate is approximately 0.006056%. The table assumes n = N inserted distinct
    /// keys and uniformly distributed, independent hash pieces; rates are theoretical per filter,
    /// not aggregate probabilities across workers. The table excludes generation tags, key collections,
    /// and other buffers. Tags add approximately 0.195% to the bitmap budget: at most 16 KiB per worker
    /// or 256 KiB for 16 workers over the supported migration batch-size range.
    /// </para>
    /// <code>
    /// Batch keys | Buckets | Bitmap/worker* | Bitmap/16 workers* | Bucketed FPR (%) |
    /// -----------+---------+----------------+--------------------+------------------+
    ///       1024 |      16 |         32 KiB |            512 KiB |       0.00016502 |
    ///       4096 |      64 |        128 KiB |              2 MiB |       0.00016536 |
    ///       8192 |     128 |        256 KiB |              4 MiB |       0.00016542 |
    ///      16384 |     256 |        512 KiB |              8 MiB |       0.00016545 |
    ///      32768 |     512 |          1 MiB |             16 MiB |       0.00016546 |
    ///      65536 |    1024 |          2 MiB |             32 MiB |       0.00016547 |
    ///     131072 |    2048 |          4 MiB |             64 MiB |       0.00016547 |
    ///     262143 |    4096 |          8 MiB |            128 MiB |       0.00016547 |
    ///
    /// * Both bucketed and global bitmaps have these memory sizes: bucketed/global memory = 1.
    /// FPR improvement = global FPR / bucketed FPR; larger values mean fewer false positives.
    /// </code>
    /// <para>
    /// The following calculation includes bucket-load variance and coincident probe positions.
    /// A_j is the probability that none of j specified positions in the query's bucket are set.
    /// q_j is the probability that the query's three probes select exactly j distinct positions.
    /// P and P_global are probabilities; multiply by 100 for the percentages in the table.
    /// Evaluate with high precision to avoid cancellation in the inclusion-exclusion terms.
    /// </para>
    /// <code>
    /// A_j = (1 - (1 - (1 - j/S)^3) / B)^n, for j = 1, 2, 3
    /// q_1 = 1/S^2
    /// q_2 = 3*(S-1)/S^2
    /// q_3 = (S-1)*(S-2)/S^2
    /// P = q_1*(1-A_1) + q_2*(1-2*A_1+A_2) + q_3*(1-3*A_1+3*A_2-A_3)
    ///
    /// U_j = (1 - j/M)^(2*n), for j = 1, 2
    /// P_global = (1-U_1)/M + (1-1/M)*(1-2*U_1+U_2)
    /// </code>
    /// <para>
    /// Clear advances a generation instead of zeroing the bitmap. Queries treat a mismatched bucket tag
    /// as empty; the first insertion into that bucket clears its bytes and publishes its current tag
    /// after setting the new key's bits. Generation zero uses the runtime-zeroed bitmap directly.
    /// Counter wraparound invalidates all bucket tags before restarting at generation one, without
    /// clearing the bitmap. False-positive rates are unchanged for the same current-generation keys.
    /// </para>
    /// <para>
    /// Clearing is normally O(1), with an O(bucket count) tag reset on wraparound. Full batches still
    /// zero essentially every bucket during rebuilding. Tag publication does not replace the migration
    /// epoch barriers: the single writer initializes membership before publishing batch protection,
    /// and must finish the protected work before advancing the generation. Arbitrary concurrent mutation
    /// and lookup are not supported.
    /// </para>
    /// <para>
    /// The third probe improves precision at the same memory budget, but adds an insertion bit update.
    /// Bucket selection is address calculation, not an additional membership probe. A bucket occupies
    /// 2 KiB, not one cache line; sizing alone does not establish a CPU or throughput improvement.
    /// </para>
    /// </remarks>
    internal sealed class BucketedBloomFilter
    {
        public const int MaxBucketCount = 1 << 16;
        public const int MaxBfSize = 1 << 16;
        public const int DefaultBucketCount = 1024;
        public const int DefaultBfSize = 16384;
        public const int DefaultMigrationHashCount = 3;
        const int TargetKeysPerBucket = 64;
        const int BucketIndexBits = 16;
        const int HashBits = 64 - BucketIndexBits;

        readonly byte[] bitmap;
        readonly uint[] bucketGenerations;
        readonly int bucketMask;
        readonly int bytesPerBucket;
        readonly int bitMask;
        readonly int hashShift;
        readonly int hashCount;
        uint currentGeneration;
        bool hasEntries;

        /// <summary>
        /// Create a migration filter targeting 64 keys per bucket, 16384 bits per bucket, and three probes.
        /// </summary>
        /// <param name="batchSize">Maximum keys per batch, excluding the key collection's reserved entry.</param>
        public BucketedBloomFilter(int batchSize)
            : this(GetBucketCountForBatch(batchSize), DefaultBfSize, DefaultMigrationHashCount)
        {
        }

        static int GetBucketCountForBatch(int batchSize)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(batchSize, MaxBucketCount * TargetKeysPerBucket);
            return (int)BitOperations.RoundUpToPowerOf2((uint)(((batchSize - 1) / TargetKeysPerBucket) + 1));
        }

        /// <summary>
        /// Use the low 16 hash bits to select a bucket and split the remaining 48 bits into equal-width indices.
        /// Unused high bits are ignored.
        /// </summary>
        /// <param name="bucketCount">Number of buckets, a positive power of two no greater than 65536.</param>
        /// <param name="bfSize">Bits per bucket, a positive power of two no greater than 65536.</param>
        /// <param name="hashCount">Number of indices, each wide enough to address every bit in its bucket.</param>
        /// <remarks>Each bucket is rounded up to a whole byte.</remarks>
        public BucketedBloomFilter(int bucketCount, int bfSize, int hashCount = 2)
        {
            if (!BitOperations.IsPow2(bucketCount) || bucketCount > MaxBucketCount)
                throw new ArgumentOutOfRangeException(nameof(bucketCount), "Bucket count must be a positive power of two no greater than 65536.");
            if (!BitOperations.IsPow2(bfSize) || bfSize > MaxBfSize)
                throw new ArgumentOutOfRangeException(nameof(bfSize), "Bits per bucket must be a positive power of two no greater than 65536.");

            ArgumentOutOfRangeException.ThrowIfLessThan(hashCount, 1);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(hashCount, HashBits);

            hashShift = HashBits / hashCount;
            if (hashShift < BitOperations.Log2((uint)bfSize))
                throw new ArgumentException("Hash indices must be wide enough to address the entire bucket.", nameof(hashCount));

            bucketMask = bucketCount - 1;
            bytesPerBucket = (bfSize + 7) >> 3;
            bitMask = bfSize - 1;
            this.hashCount = hashCount;
            bitmap = GC.AllocateArray<byte>(bucketCount * bytesPerBucket, pinned: true);
            bucketGenerations = new uint[bucketCount];
        }

        /// <summary>
        /// Hash a key and add its membership.
        /// </summary>
        /// <param name="key">Key bytes.</param>
        /// <param name="seed">Hash seed, including any namespace context.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ReadOnlySpan<byte> key, uint seed = 0)
            => Add(HashUtils.MurmurHash2x64A(key, seed));

        /// <summary>
        /// Add membership using a precomputed, uniformly distributed hash.
        /// </summary>
        /// <param name="hash">The full key hash.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ulong hash)
        {
            var bucket = (int)hash & bucketMask;
            var bucketOffset = bucket * bytesPerBucket;
            var generation = currentGeneration;
            var initializeBucket = bucketGenerations[bucket] != generation;
            if (initializeBucket)
                Array.Clear(bitmap, bucketOffset, bytesPerBucket);
            hash >>= BucketIndexBits;
            for (var i = 0; i < hashCount; i++)
            {
                var bit = (int)hash & bitMask;
                bitmap[bucketOffset + (bit >> 3)] |= (byte)(1 << (bit & 7));
                hash >>= hashShift;
            }
            if (initializeBucket)
                Volatile.Write(ref bucketGenerations[bucket], generation);
            hasEntries = true;
        }

        /// <summary>
        /// Hash a key and check its membership; positive results may be false positives.
        /// </summary>
        /// <param name="key">Key bytes.</param>
        /// <param name="seed">The same hash seed used when adding the key.</param>
        /// <returns>Whether every selected bit is set.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(ReadOnlySpan<byte> key, uint seed = 0)
            => Contains(HashUtils.MurmurHash2x64A(key, seed));

        /// <summary>
        /// Check membership using a precomputed hash; positive results may be false positives.
        /// </summary>
        /// <param name="hash">The full key hash.</param>
        /// <returns>Whether every selected bit is set.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(ulong hash)
        {
            var bucket = (int)hash & bucketMask;
            var generation = Volatile.Read(ref currentGeneration);
            if (Volatile.Read(ref bucketGenerations[bucket]) != generation)
                return false;
            var bucketOffset = bucket * bytesPerBucket;
            hash >>= BucketIndexBits;
            for (var i = 0; i < hashCount; i++)
            {
                var bit = (int)hash & bitMask;
                if ((bitmap[bucketOffset + (bit >> 3)] & (1 << (bit & 7))) == 0)
                    return false;
                hash >>= hashShift;
            }
            return true;
        }

        /// <summary>
        /// Logically clear membership; bucket bytes are zeroed lazily on their next insertion.
        /// Empty filters are unchanged. Generation wraparound resets tags, not the bitmap.
        /// </summary>
        public void Clear()
        {
            if (!hasEntries)
                return;
            var generation = unchecked(currentGeneration + 1);
            if (generation == 0)
            {
                Array.Clear(bucketGenerations);
                generation = 1;
            }
            Volatile.Write(ref currentGeneration, generation);
            hasEntries = false;
        }
    }
}