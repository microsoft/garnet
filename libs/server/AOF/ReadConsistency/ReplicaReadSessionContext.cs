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
    [StructLayout(LayoutKind.Explicit)]
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

        /// <summary>
        /// Per-sublog cached maximum sequence numbers. Avoids repeated volatile reads of the
        /// shared sketchMax value when the cached value already satisfies the read requirement.
        /// Shared across batch/session context copies (same backing array).
        /// </summary>
        [FieldOffset(32)]
        public long[] cachedSublogMax;

        /// <summary>
        /// Reusable waiter owned by this session, used when a consistent read must block for replay
        /// to catch up. Allocated once per session and reused across every wait, avoiding a per-wait
        /// node + <see cref="ManualResetEventSlim"/> allocation. Shared by reference across
        /// batch/session context copies; a session waits sequentially so the single node is never
        /// contended.
        /// </summary>
        [FieldOffset(40)]
        internal ReadSessionWaiter waiter;

        /// <summary>
        /// Resets the cached per-sublog max values (e.g., on version change).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void ResetCachedSublogMax()
        {
            if (cachedSublogMax != null)
                Array.Clear(cachedSublogMax);
        }
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
            var sublogMax = new long[serverOptions.AofVirtualSublogCount];
            replicaReadContext = new() { sessionVersion = -1, maximumSessionSequenceNumber = 0, lastVirtualSublogIdx = -1, cachedSublogMax = sublogMax, waiter = new ReadSessionWaiter() };
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
            // batchReadContext shares the same waiter reference, so dispose it once here.
            replicaReadContext.waiter?.Dispose();
        }

        /// <summary>
        /// Probes, without waiting, whether reading the keys of a command in order would make the session wait for
        /// replay to catch up.
        /// </summary>
        /// <remarks>
        /// Leaves the session's read context as it was: the probe advances a copy. The keys are hashed the way
        /// <see cref="PreSingleKeyConsistentRead"/> hashes them.
        /// </remarks>
        /// <param name="parseState">The parsed arguments of the command.</param>
        /// <param name="keySpecs">Where the command's keys are among its arguments.</param>
        /// <param name="isSubCommand">Whether key positions count from a command whose name is two tokens.</param>
        /// <param name="manager">The manager the probe ran against, which a wait must be listed with.</param>
        /// <param name="virtualSublogIdx">When the command would wait, the virtual sublog it waits on.</param>
        /// <param name="sequenceNumber">When the command would wait, the sequence number replay of that sublog has to pass.</param>
        /// <returns>True if the command would wait.</returns>
        internal bool MustWaitForReplay(ref SessionParseState parseState, SimpleRespKeySpec[] keySpecs, bool isSubCommand,
            out ReadConsistencyManager manager, out int virtualSublogIdx, out long sequenceNumber)
        {
            virtualSublogIdx = -1;
            sequenceNumber = 0;
            manager = appendOnlyFile.readConsistencyManager;
            if (manager is null || !inProgress.TryReadLock())
                return false;

            try
            {
                var probe = replicaReadContext;
                manager.CheckConsistencyManagerVersion(ref probe);

                foreach (var keySpec in keySpecs)
                {
                    if (!parseState.TryGetKeySearchArgsFromSimpleKeySpec(keySpec, isSubCommand, out var searchArgs))
                        continue;

                    for (var i = searchArgs.firstIdx; i <= searchArgs.lastIdx; i += searchArgs.step)
                    {
                        var hash = GarnetLog.HASH(parseState.GetArgSliceByRef(i).ReadOnlySpan) & long.MaxValue;
                        if (manager.ProbeKeyFreshness(hash, ref probe, out virtualSublogIdx, out sequenceNumber))
                            return true;
                    }
                }

                return false;
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// The most times in a row a scan stops to wait for replay and is run again, before it is given the blocking path.
        /// </summary>
        internal const int MaxScanParks = 8;

        // The state of a scan that does not wait inside the scan. It checks each record against a copy of the read
        // context, and stops at the first record that would wait. All of it is touched only by the scan, on the
        // session's own thread.
        ReplicaReadSessionContext scanContext;
        ReadConsistencyManager scanManager;
        int scanVirtualSublogIdx;
        long scanSequenceNumber;
        int scanParks;
        bool scanProbing;
        bool scanMustWait;

        /// <summary>
        /// Whether a scan started by <see cref="BeginScanProbe"/> is running.
        /// </summary>
        internal bool ScanProbing => scanProbing;

        /// <summary>
        /// Whether the scan running has reached a record that would make it wait for replay.
        /// </summary>
        internal bool ScanMustWait => scanMustWait;

        /// <summary>
        /// Starts a scan that does not wait for replay inside the scan, so it does not hold a thread while it waits.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The scan stops at the first record that would wait. It has then produced nothing and changed nothing: the
        /// session parks, and runs the scan again from the start once replay has caught up (see <see cref="EndScanProbe"/>).
        /// Only a scan that gets to the end without waiting advances the session's read context.
        /// </para>
        /// <para>
        /// A scan that keeps meeting a record to wait for, because the keys it has read keep being written, is run again
        /// at most <see cref="MaxScanParks"/> times in a row. The next one is given the blocking path, which waits where
        /// it stands and so always gets to the end.
        /// </para>
        /// </remarks>
        /// <returns>False if the scan has to wait inside the scan.</returns>
        internal bool BeginScanProbe()
        {
            var manager = appendOnlyFile.readConsistencyManager;
            if (manager is null)
                return false;

            if (scanParks >= MaxScanParks)
            {
                scanParks = 0;
                return false;
            }

            if (!inProgress.TryReadLock())
                return false;

            try
            {
                scanContext = replicaReadContext;
                manager.CheckConsistencyManagerVersion(ref scanContext);
            }
            finally
            {
                inProgress.ReadUnlock();
            }

            scanManager = manager;
            scanMustWait = false;
            scanProbing = true;
            return true;
        }

        /// <summary>
        /// Checks, without waiting, whether reading the next key of the scan would make it wait for replay.
        /// </summary>
        /// <param name="hash">Hash of the key.</param>
        /// <returns>True if the scan has to stop here.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ProbeScanKey(long hash)
        {
            if (!scanManager.ProbeKeyFreshness(hash & long.MaxValue, ref scanContext, out scanVirtualSublogIdx, out scanSequenceNumber))
                return false;

            scanMustWait = true;
            return true;
        }

        /// <summary>
        /// Updates the maximum sequence number of the scan after it has read the key last probed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PostScanKey()
            => scanManager.PostSingleKeyConsistentRead(ref scanContext);

        /// <summary>
        /// Ends a scan started by <see cref="BeginScanProbe"/>.
        /// </summary>
        /// <param name="completed">Whether the scan ran to the end, as opposed to throwing.</param>
        /// <param name="manager">When the scan has to wait, the manager the wait must be listed with.</param>
        /// <param name="virtualSublogIdx">When the scan has to wait, the virtual sublog it waits on.</param>
        /// <param name="sequenceNumber">When the scan has to wait, the sequence number replay of that sublog has to pass.</param>
        /// <returns>True if the scan stopped to wait for replay, in which case its result is to be dropped.</returns>
        internal bool EndScanProbe(bool completed, out ReadConsistencyManager manager, out int virtualSublogIdx, out long sequenceNumber)
        {
            manager = scanManager;
            virtualSublogIdx = scanVirtualSublogIdx;
            sequenceNumber = scanSequenceNumber;
            var mustWait = scanMustWait;

            scanProbing = false;
            scanMustWait = false;
            scanManager = null;

            if (mustWait)
            {
                scanParks++;
                return true;
            }

            scanParks = 0;
            if (completed && inProgress.TryReadLock())
            {
                replicaReadContext = scanContext;
                inProgress.ReadUnlock();
            }

            return false;
        }

        /// <summary>
        /// Freshness check before actual read for a single key
        /// </summary>
        /// <param name="hash"></param>
        /// <exception cref="GarnetException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PreSingleKeyConsistentRead(long hash)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(PreSingleKeyConsistentRead)}");
            try
            {
                appendOnlyFile.readConsistencyManager.PreSingleKeyConsistentRead(hash & long.MaxValue, ref replicaReadContext, readTimeout, consistentReadCts.Token);
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// Post read update maximum sequence number for a single key.
        /// </summary>
        /// <exception cref="GarnetException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleKeyConsistentReadCallback()
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(PostSingleKeyConsistentReadCallback)}");
            try
            {
                appendOnlyFile.readConsistencyManager.PostSingleKeyConsistentRead(ref replicaReadContext);
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// Initialize context for read key batch.
        /// </summary>
        /// <param name="parameters"></param>
        public void PreBatchKeyConsistentReadCallback(ReadOnlySpan<PinnedSpanByte> parameters)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(PreBatchKeyConsistentReadCallback)}");
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
                    consistencyManager.PreBatchKeyConsistentRead(key.ReadOnlySpan, ref batchReadContext, readTimeout, consistentReadCts.Token, out var hash);
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
        public bool PostBatchKeyConsistentReadCallback(int keyCount)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(PostBatchKeyConsistentReadCallback)}");
            try
            {
                var consistencyManager = appendOnlyFile.readConsistencyManager;
                for (var i = 0; i < keyCount; i++)
                {
                    var hash = keyHashCache[i];
                    if (!consistencyManager.PostBatchKeyConsistentReadValidate(hash, ref batchReadContext))
                        return false;
                }

                // Propagate batch context back to session context to maintain prefix consistency
                // for subsequent single-key reads across different sublogs.
                replicaReadContext.maximumSessionSequenceNumber = batchReadContext.maximumSessionSequenceNumber;
                replicaReadContext.lastVirtualSublogIdx = batchReadContext.lastVirtualSublogIdx;
                replicaReadContext.lastHash = batchReadContext.lastHash;

                return true;
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }
    }
}