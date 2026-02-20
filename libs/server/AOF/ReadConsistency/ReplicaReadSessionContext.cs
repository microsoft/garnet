// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// A cancellation token source used to signal cancellation for consistent read operations.
        /// </summary>
        readonly CancellationTokenSource consistentReadCts;

        /// <summary>
        /// Timeout cancellation token source.
        /// </summary>
        CancellationTokenSource timeoutCts;

        /// <summary>
        /// Consistent read in progress lock
        /// </summary>
        SingleWriterMultiReaderLock inProgress;

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
            timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(consistentReadCts.Token);
            timeoutCts.CancelAfter(serverOptions.ReplicaSyncTimeout);
        }

        /// <summary>
        /// Releases all resources used by the current instance of the class.
        /// </summary>
        public void Dispose()
        {
            consistentReadCts.Cancel();
            timeoutCts.Cancel();
            inProgress.WriteLock();
            consistentReadCts.Dispose();
            timeoutCts.Dispose();
        }

        void ResetTimeoutCts()
        {
            if (timeoutCts.TryReset())
            {
                timeoutCts.CancelAfter(serverOptions.ReplicaSyncTimeout);
            }
            else
            {
                // TryReset failed (too many resets), recreate the CTS
                timeoutCts?.Dispose();
                timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(consistentReadCts.Token);
                timeoutCts.CancelAfter(serverOptions.ReplicaSyncTimeout);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeforeConsistentReadKeyCallback(PinnedSpanByte key)
        {
            if (!inProgress.TryReadLock())
                throw new GarnetException($"Failed to acquire inProgress lock at {nameof(BeforeConsistentReadKeyCallback)}");
            try
            {
                ResetTimeoutCts();
                appendOnlyFile.readConsistencyManager.BeforeConsistentReadKey(key, ref replicaReadContext, timeoutCts.Token);
            }
            finally
            {
                inProgress.ReadUnlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AfterConsistentReadKeyCallback()
            => appendOnlyFile.readConsistencyManager.AfterConsistentReadKey(ref replicaReadContext);

        public void InitializeForBatch(int keyCount)
        {

        }

        public void BeforeConsistentReadKeyBatchCallback()
        {

        }
    }
}