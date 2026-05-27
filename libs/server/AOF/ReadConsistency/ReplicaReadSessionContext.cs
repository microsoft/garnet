// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 26)]
    public struct ReplicaReadSessionContext
    {
        /// <summary>
        /// Session version
        /// </summary>
        [FieldOffset(0)]
        public long sessionVersion;

        /// <summary>
        /// Maximum session sequence number established from all keys read so far
        /// </summary>
        [FieldOffset(8)]
        public long maximumSessionSequenceNumber;

        /// <summary>
        /// Last read hash
        /// </summary>
        [FieldOffset(16)]
        public long lastHash;

        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(24)]
        public short lastVirtualSublogIdx;
    }

    public class ReadSessionState : IDisposable
    {
        /// <summary>
        /// GarnetAppendOnlyFile instance
        /// </summary>
        readonly GarnetAppendOnlyFile appendOnlyFile;

        /// <summary>
        /// Gets the configuration options for the Garnet server.
        /// </summary>
        readonly GarnetServerOptions serverOptions;

        /// <summary>
        /// Replica read context used with sharded log
        /// </summary>
        ReplicaReadSessionContext replicaReadContext;

        /// <summary>
        /// Read context for batch reads. Used to track max sequence number of all keys involved in the read.
        /// </summary>
        ReplicaReadSessionContext batchReadContext;

        /// <summary>
        /// A cancellation token source used to signal cancellation for consistent read operations (e.g., on dispose).
        /// </summary>
        readonly CancellationTokenSource consistentReadCts;

        /// <summary>
        /// Timeout duration for consistent read wait operations.
        /// </summary>
        readonly TimeSpan readTimeout;

        /// <summary>
        /// Consistent read in progress lock
        /// </summary>
        SingleWriterMultiReaderLock inProgress;

        /// <summary>
        /// Array of key hashes used for consistent read key batch.
        /// </summary>
        long[] keyHashCache = null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int GetPowerOfTwoSize(int value)
            => value <= 1 ? 1 : (int)BitOperations.RoundUpToPowerOf2((uint)value);

        void ExpandKeyHashCache(int keyCount)
        {
            var newSize = GetPowerOfTwoSize(keyCount);
            keyHashCache = GC.AllocateArray<long>(newSize, pinned: true);
        }

        void ShrinkKeyHashCache(int keyCount)
        {
            var newSize = GetPowerOfTwoSize(keyCount);
            keyHashCache = GC.AllocateArray<long>(newSize, pinned: true);
        }

        /// <summary>
        /// Read session state constructor
        /// </summary>
        /// <param name="appendOnlyFile"></param>
        /// <param name="serverOptions"></param>
        public ReadSessionState(GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.serverOptions = serverOptions;
            replicaReadContext = new() { sessionVersion = -1, maximumSessionSequenceNumber = 0, lastVirtualSublogIdx = -1 };
            consistentReadCts = new();
            readTimeout = serverOptions.ReplicaSyncTimeout;
        }

        /// <summary>
        /// Releases all resources used by the current instance of the class.
        /// </summary>
        public void Dispose()
        {
            consistentReadCts.Cancel();
            inProgress.WriteLock();
            consistentReadCts.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeforeConsistentReadKeyCallback(long hash)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(BeforeConsistentReadKeyCallback)}");
            try
            {
                appendOnlyFile.readConsistencyManager.BeforeConsistentReadKey(hash & long.MaxValue, ref replicaReadContext, readTimeout, consistentReadCts.Token);
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AfterConsistentReadKeyCallback()
            => appendOnlyFile.readConsistencyManager.AfterConsistentReadKey(ref replicaReadContext);

        /// <summary>
        /// Initialize context for read key batch.
        /// </summary>
        /// <param name="parameters"></param>
        public void BeforeConsistentReadKeyBatch(ReadOnlySpan<PinnedSpanByte> parameters)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(BeforeConsistentReadKeyCallback)}");
            try
            {
                var keyCount = parameters.Length;
                var consistencyManager = appendOnlyFile.readConsistencyManager;
                // First check if version of consistency manager has changed
                appendOnlyFile.readConsistencyManager.CheckConsistencyManagerVersion(ref replicaReadContext);

                // Allocate array to cache key hashes for batch read
                if (keyHashCache == null || keyCount > keyHashCache.Length)
                    ExpandKeyHashCache(keyCount);
                else if ((keyCount << 2) < keyHashCache.Length)
                    ShrinkKeyHashCache(keyCount);

                // NOTE: this context is a copy used to emulate standalone reads.
                // The actual update of the session max will happen after the read succeeds.
                batchReadContext = replicaReadContext;
                for (var i = 0; i < parameters.Length; i++)
                {
                    var key = parameters[i];
                    consistencyManager.BeforeConsistentReadKeyBatch(key.ReadOnlySpan, ref batchReadContext, readTimeout, consistentReadCts.Token, out var hash);
                    keyHashCache[i] = hash;
                }
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// Validate keys have not changed after reading a key batch.
        /// </summary>
        /// <param name="keyCount"></param>
        /// <returns></returns>
        public bool AfterConsistentReadKeyBatch(int keyCount)
        {
            var consistencyManager = appendOnlyFile.readConsistencyManager;
            for (var i = 0; i < keyCount; i++)
            {
                var hash = keyHashCache[i];
                if (!consistencyManager.AfterConsistentReadKeyBatch(hash, ref batchReadContext))
                    return false;
            }

            // Propagate batch context back to session context to maintain prefix consistency
            // for subsequent single-key reads across different sublogs.
            replicaReadContext.maximumSessionSequenceNumber = batchReadContext.maximumSessionSequenceNumber;
            replicaReadContext.lastVirtualSublogIdx = batchReadContext.lastVirtualSublogIdx;
            replicaReadContext.lastHash = batchReadContext.lastHash;

            return true;
        }
    }
}