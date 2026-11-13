// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class Sketch
    {
        readonly BucketedBloomFilter bloomFilter;
        public ArgSliceVector argSliceVector { get; private set; }

        public List<(PinnedSpanByte, bool)> Keys { private set; get; }
        public SketchStatus Status { private set; get; }

        /// <summary>
        /// Create a migration sketch with a batch-sized filter and one reserved key collection entry.
        /// </summary>
        /// <param name="keySendBatchSize">Maximum keys per migration batch.</param>
        public Sketch(int keySendBatchSize)
            : this(new BucketedBloomFilter(keySendBatchSize), maxBatchSize: keySendBatchSize + 1)
        {
        }

        /// <summary>
        /// Create a migration sketch with independently sized Bloom buckets and a bounded key collection.
        /// </summary>
        /// <param name="bucketCount">Number of Bloom buckets.</param>
        /// <param name="bfSize">Bloom-filter bits per bucket.</param>
        /// <param name="maxBatchSize">Key collection capacity, including its reserved entry.</param>
        /// <param name="hashCount">Number of Bloom probes derived from the upper 48 hash bits.</param>
        public Sketch(int bucketCount = BucketedBloomFilter.DefaultBucketCount, int bfSize = BucketedBloomFilter.DefaultBfSize, int maxBatchSize = 1 << 18, int hashCount = BucketedBloomFilter.DefaultMigrationHashCount)
            : this(new BucketedBloomFilter(bucketCount, bfSize, hashCount), maxBatchSize)
        {
        }

        Sketch(BucketedBloomFilter bloomFilter, int maxBatchSize)
        {
            this.bloomFilter = bloomFilter;
            Status = SketchStatus.INITIALIZING;
            Keys = [];
            argSliceVector = new(maxBatchSize);
        }

        #region sketchMethods

        public bool TryHashAndStore(ReadOnlySpan<byte> key)
        {
            if (!argSliceVector.TryAddItem(key))
                return false;

            bloomFilter.Add(key);

            return true;
        }

        public bool TryHashAndStore(ReadOnlySpan<byte> ns, ReadOnlySpan<byte> key)
        {
            Debug.Assert(ns.Length is (sizeof(byte) or sizeof(uint)), "Longer namespaces not yet supported");

            if (!argSliceVector.TryAddItem(ns, key))
                return false;

            var nsRaw = VectorManager.ExtractContextFromNamespaces(ns);

            bloomFilter.Add(key, seed: (uint)nsRaw);

            return true;
        }

        /// <summary>
        /// Hash key to bloomfilter and store it for future use (NOTE: Use only with KEYS option)
        /// </summary>
        /// <param name="key"></param>
        public void HashAndStore(PinnedSpanByte key)
        {
            bloomFilter.Add(key.ReadOnlySpan);
            Keys.Add((key, false));
        }

        /// <summary>
        /// Rebuild protection for a collected batch and return the reusable previous collection.
        /// </summary>
        /// <param name="batch">The next batch, activated only after the previous batch finishes.</param>
        /// <returns>The previous collection, cleared when it differs from the next batch.</returns>
        public ArgSliceVector ReplaceBatch(ArgSliceVector batch)
        {
            Debug.Assert(Status is SketchStatus.INITIALIZING or SketchStatus.MIGRATED);
            var previous = argSliceVector;
            if (previous != batch)
                previous.Clear();
            argSliceVector = batch;
            ClearMembership();
            foreach (var (ns, key, hasNamespace) in batch)
            {
                var seed = hasNamespace ? (uint)VectorManager.ExtractContextFromNamespaces(ns.ReadOnlySpan) : 0;
                bloomFilter.Add(key.ReadOnlySpan, seed);
            }
            return previous;
        }

        /// <summary>
        /// Probe sketch using a precomputed key hash.
        /// </summary>
        /// <param name="keyHash">Unmasked MurmurHash2x64A key hash.</param>
        /// <param name="status"></param>
        /// <returns></returns>
        public bool Probe(ulong keyHash, out SketchStatus status)
        {
            var exists = bloomFilter.Contains(keyHash);
            status = exists ? Status : SketchStatus.INITIALIZING;
            return exists;
        }

        /// <summary>
        /// Clear keys from working set
        /// </summary>
        public void Clear()
        {
            argSliceVector.Clear();
            ClearMembership();
        }

        /// <summary>
        /// Release batch protection and collected keys without zeroing the membership bitmap.
        /// Clear or replace the batch before building protection for another batch.
        /// </summary>
        public void RetireBatch()
        {
            argSliceVector.Clear();
            Status = SketchStatus.INITIALIZING;
        }

        void ClearMembership()
        {
            bloomFilter.Clear();
            Status = SketchStatus.INITIALIZING;
        }

        /// <summary>
        /// Set KeyMigrationStatus
        /// </summary>
        /// <param name="status"></param>
        public void SetStatus(SketchStatus status) => Status = status;
        #endregion
    }
}