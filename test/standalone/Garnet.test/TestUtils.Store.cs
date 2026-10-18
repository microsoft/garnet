// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Test helpers that read the store's internal state. Kept out of TestUtils.cs, which the cluster test projects compile
    /// from source without access to Tsavorite's internals.
    /// </summary>
    internal static partial class TestUtils
    {
        /// <summary>
        /// Finds a key other than <paramref name="key"/> that falls in the same hash index bucket and has the same tag,
        /// so the records of the two keys share a chain in the hash index. A record that is not alone in its chain is not
        /// elided when an update or delete supersedes it; it stays in the log, sealed, until its page is evicted.
        /// </summary>
        /// <param name="key">The key to share a chain with.</param>
        /// <param name="indexBuckets">The number of hash index buckets, a power of two (the index size divided by 64).</param>
        public static string FindKeyInSameHashChain(string key, long indexBuckets)
            => FindKeyInSameHashChain(SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(key)), indexBuckets, prefix: key);

        /// <summary>
        /// Finds a key, starting with <paramref name="prefix"/>, that falls in the same hash index bucket and has the same
        /// tag as keys whose hash is <paramref name="keyHash"/>, such as a namespaced key, so the records of the two share a
        /// chain in the hash index.
        /// </summary>
        /// <param name="keyHash">The hash of the key to share a chain with.</param>
        /// <param name="indexBuckets">The number of hash index buckets, a power of two (the index size divided by 64).</param>
        /// <param name="prefix">The start of the key to return.</param>
        public static string FindKeyInSameHashChain(long keyHash, long indexBuckets, string prefix)
        {
            Debug.Assert(indexBuckets > 0 && (indexBuckets & (indexBuckets - 1)) == 0, "The hash index has a power-of-two number of buckets");

            var bucket = keyHash & (indexBuckets - 1);
            var tag = HashBucketEntry.GetTag(keyHash);

            // Random suffixes from a fixed seed: the key hash is a multiply-accumulate, so sequential suffixes barely
            // change the high bits that form the tag.
            var random = new Random(0);
            var candidate = new byte[prefix.Length + 9];
            _ = Encoding.ASCII.GetBytes(prefix.AsSpan(), candidate);
            candidate[prefix.Length] = (byte)'-';

            while (true)
            {
                for (var i = prefix.Length + 1; i < candidate.Length; i++)
                    candidate[i] = (byte)('a' + random.Next(26));

                var candidateHash = SpanByteComparer.StaticGetHashCode64(candidate);
                if ((candidateHash & (indexBuckets - 1)) == bucket && HashBucketEntry.GetTag(candidateHash) == tag)
                    return Encoding.ASCII.GetString(candidate);
            }
        }

        /// <summary>
        /// Number of active transactions, which a checkpoint or an index grow waits for. Only the current version and the
        /// next one can have active transactions.
        /// </summary>
        public static long GetActiveTransactionCount(StateMachineDriver driver)
        {
            var version = driver.SystemState.Version;
            return driver.GetNumActiveTransactions(version) + driver.GetNumActiveTransactions(version + 1);
        }

        /// <summary>
        /// True if the hash index bucket of <paramref name="key"/> is locked, as a transaction locks the buckets of its keys.
        /// Reads the lock state without taking the lock, so it never waits.
        /// </summary>
        public static unsafe bool IsKeyBucketLocked<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, string key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => HashBucket.IsLatched(store.LockTable.GetBucket(SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(key))));
    }
}