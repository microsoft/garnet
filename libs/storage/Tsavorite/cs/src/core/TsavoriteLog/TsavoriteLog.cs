﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite log
    /// </summary>
    public sealed class TsavoriteLog : IDisposable
    {
        private Exception cannedException = null;

        readonly BlittableAllocator<Empty, byte> allocator;
        readonly LightEpoch epoch;
        readonly ILogCommitManager logCommitManager;
        readonly bool disposeLogCommitManager;
        readonly GetMemory getMemory;
        readonly int headerSize;
        readonly LogChecksumType logChecksum;
        readonly WorkQueueLIFO<CommitInfo> commitQueue;

        internal readonly bool readOnlyMode;
        internal readonly bool fastCommitMode;
        internal readonly bool tolerateDeviceFailure;

        TaskCompletionSource<LinkedCommitInfo> commitTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource<Empty> refreshUncommittedTcs;

        // Offsets for all currently unprocessed commit records
        readonly Queue<(long, TsavoriteLogRecoveryInfo)> ongoingCommitRequests;
        readonly List<TsavoriteLogRecoveryInfo> coveredCommits = new();
        long commitNum, commitCoveredAddress;

        readonly LogCommitPolicy commitPolicy;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => beginAddress;

        /// <summary>
        /// BeginAddress as per allocator, used in tests
        /// </summary>
        internal long AllocatorBeginAddress => allocator.BeginAddress;

        // Here's a soft begin address that is observed by all access at the TsavoriteLog level but not actually on the
        // allocator. This is to make sure that any potential physical deletes only happen after commit.
        long beginAddress;

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        /// <summary>
        /// Log flushed until address
        /// </summary>
        public long FlushedUntilAddress => allocator.FlushedUntilAddress;

        /// <summary>
        /// Log safe read-only address
        /// </summary>
        public long SafeTailAddress;

        /// <summary>
        /// Dictionary of recovered iterators and their committed until addresses
        /// </summary>
        public Dictionary<string, long> RecoveredIterators { get; private set; }

        /// <summary>
        /// Log committed until address
        /// </summary>
        public long CommittedUntilAddress;

        /// <summary>
        /// Log committed begin address
        /// </summary>
        public long CommittedBeginAddress;

        /// <summary>
        /// Recovered Commit Cookie
        /// </summary>
        public byte[] RecoveredCookie;

        /// <summary>
        /// Header size used by TsavoriteLog
        /// </summary>
        public int HeaderSize => headerSize;

        /// <summary>
        /// Task notifying commit completions
        /// </summary>
        internal Task<LinkedCommitInfo> CommitTask => commitTcs.Task;

        /// <summary>
        /// Task notifying log flush completions
        /// </summary>
        internal CompletionEvent FlushEvent => allocator.FlushEvent;

        /// <summary>
        /// Table of persisted iterators
        /// </summary>
        internal readonly ConcurrentDictionary<string, TsavoriteLogScanIterator> PersistedIterators = new();

        /// <summary>
        /// Committed view of commitMetadataVersion
        /// </summary>
        private long persistedCommitNum;

        internal Dictionary<string, long> LastPersistedIterators;

        /// <summary>
        /// Numer of references to log, including itself
        /// Used to determine disposability of log
        /// </summary>
        internal int logRefCount = 1;

        readonly ILogger logger;

        /// <summary>
        /// Whether we refresh safe tail as records are inserted
        /// </summary>
        readonly bool AutoRefreshSafeTailAddress;

        /// <summary>
        /// Callback when safe tail shifts
        /// </summary>
        public Action<long, long> SafeTailShiftCallback;

        /// <summary>
        /// Whether we automatically commit as records are inserted
        /// </summary>
        readonly bool AutoCommit;

        /// <summary>
        /// Whether there is an ongoing auto refresh safe tail
        /// </summary>
        int _ongoingAutoRefreshSafeTailAddress = 0;

        /// <summary>
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings">Log settings</param>
        /// <param name="logger">Log settings</param>
        public TsavoriteLog(TsavoriteLogSettings logSettings, ILogger logger = null)
            : this(logSettings, logSettings.TryRecoverLatest, logger)
        { }

        /// <summary>
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings">Log settings</param>
        /// <param name="syncRecover">Recover synchronously</param>
        /// <param name="logger">Log settings</param>
        private TsavoriteLog(TsavoriteLogSettings logSettings, bool syncRecover, ILogger logger = null)
        {
            this.logger = logger;
            AutoRefreshSafeTailAddress = logSettings.AutoRefreshSafeTailAddress;
            AutoCommit = logSettings.AutoCommit;
            logCommitManager = logSettings.LogCommitManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        logSettings.LogCommitDir ??
                        new FileInfo(logSettings.LogDevice.FileName).Directory.FullName),
                    !logSettings.ReadOnlyMode && logSettings.RemoveOutdatedCommits);

            if (logSettings.LogCommitManager == null)
                disposeLogCommitManager = true;

            // Reserve 8 byte checksum in header if requested
            logChecksum = logSettings.LogChecksum;
            headerSize = logChecksum == LogChecksumType.PerEntry ? 12 : 4;
            getMemory = logSettings.GetMemory;
            epoch = new LightEpoch();
            CommittedUntilAddress = Constants.kFirstValidAddress;
            CommittedBeginAddress = Constants.kFirstValidAddress;
            SafeTailAddress = Constants.kFirstValidAddress;
            commitQueue = new WorkQueueLIFO<CommitInfo>(SerialCommitCallbackWorker);
            allocator = new BlittableAllocator<Empty, byte>(
                logSettings.GetLogSettings(), null,
                null, epoch, CommitCallback, logger);
            allocator.Initialize();
            beginAddress = allocator.BeginAddress;

            // TsavoriteLog is used as a read-only iterator
            if (logSettings.ReadOnlyMode)
            {
                readOnlyMode = true;
                allocator.HeadAddress = long.MaxValue;
            }

            fastCommitMode = logSettings.FastCommitMode;

            ongoingCommitRequests = new Queue<(long, TsavoriteLogRecoveryInfo)>();
            commitPolicy = logSettings.LogCommitPolicy ?? LogCommitPolicy.Default();
            commitPolicy.OnAttached(this);

            tolerateDeviceFailure = logSettings.TolerateDeviceFailure;

            if (syncRecover)
            {
                try
                {
                    Recover(-1);
                }
                catch { }
            }
        }

        /// <summary>
        /// Reset TsavoriteLog to empty state
        /// WARNING: Run after database is quiesced
        /// </summary>
        public void Reset()
        {
            var beginAddress = allocator.GetFirstValidLogicalAddress(0);
            allocator.Reset();
            CommittedUntilAddress = beginAddress;
            CommittedBeginAddress = beginAddress;
            SafeTailAddress = beginAddress;

            commitNum = 0;
            this.beginAddress = beginAddress;
        }

        /// <summary>
        /// Initialize new log instance with specific begin address and (optional) last commit number
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="committedUntilAddress"></param>
        /// <param name="lastCommitNum"></param>
        public void Initialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
        {
            Debug.Assert(!readOnlyMode);

            if (beginAddress == 0)
                beginAddress = allocator.GetFirstValidLogicalAddress(0);

            if (committedUntilAddress == 0)
                committedUntilAddress = beginAddress;

            try
            {
                allocator.Reset();
                allocator.RestoreHybridLog(beginAddress, committedUntilAddress, committedUntilAddress, committedUntilAddress);
            }
            catch
            {
                if (!tolerateDeviceFailure) throw;
            }

            CommittedUntilAddress = committedUntilAddress;
            CommittedBeginAddress = beginAddress;
            SafeTailAddress = committedUntilAddress;

            commitNum = lastCommitNum;
            this.beginAddress = beginAddress;

            if (lastCommitNum > 0) logCommitManager.OnRecovery(lastCommitNum);
        }

        /// <summary>
        /// Recover TsavoriteLog to the specific commit number, or latest if -1
        /// </summary>
        /// <param name="requestedCommitNum">Requested commit number</param>
        public void Recover(long requestedCommitNum = -1)
        {
            if (CommittedUntilAddress > BeginAddress)
                throw new TsavoriteException($"Already recovered until address {CommittedUntilAddress}");

            Dictionary<string, long> it;
            if (requestedCommitNum == -1)
                RestoreLatest(out it, out RecoveredCookie);
            else
                RestoreSpecificCommit(requestedCommitNum, out it, out RecoveredCookie);
            RecoveredIterators = it;
        }

        /// <summary>
        /// Create new log instance asynchronously
        /// </summary>
        /// <param name="logSettings"></param>
        /// <param name="cancellationToken"></param>
        public static async ValueTask<TsavoriteLog> CreateAsync(TsavoriteLogSettings logSettings, CancellationToken cancellationToken = default)
        {
            var log = new TsavoriteLog(logSettings, false);
            if (logSettings.TryRecoverLatest)
            {
                var (it, cookie) = await log.RestoreLatestAsync(cancellationToken).ConfigureAwait(false);
                log.RecoveredIterators = it;
                log.RecoveredCookie = cookie;
            }
            return log;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Decrement(ref logRefCount) == 0)
                TrueDispose();
        }

        /// <summary>
        /// Mark the log as complete. A completed log will no longer allow enqueues, and all currently enqueued items will
        /// be immediately committed.
        /// </summary>
        /// <param name="spinWait"> whether to spin until log completion becomes committed </param>
        public void CompleteLog(bool spinWait = false)
        {
            // Ensure progress even if there is no thread in epoch table. Also, BumpCurrentEpoch must be done on a protected thread.
            bool isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                // Ensure all currently started entries will enqueue before we declare log closed
                epoch.BumpCurrentEpoch(() =>
                {
                    CommitInternal(out _, out _, false, Array.Empty<byte>(), long.MaxValue, null);
                });
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend();
            }

            if (spinWait)
                WaitForCommit(TailAddress, long.MaxValue);
        }

        /// <summary>
        /// Check if the log is complete. A completed log will no longer allow enqueues, and all currently enqueued items will
        /// be immediately committed.
        /// </summary>
        public bool LogCompleted => commitNum == long.MaxValue;

        internal void TrueDispose()
        {
            commitQueue.Dispose();
            commitTcs.TrySetException(new ObjectDisposedException("Log has been disposed"));
            allocator.Dispose();
            epoch.Dispose();
            if (disposeLogCommitManager)
                logCommitManager.Dispose();
        }

        #region Enqueue
        /// <summary>
        /// Enqueue entry to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(byte[] entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue entry to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(ReadOnlySpan<byte> entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue raw pre-formatted bytes with headers to the log (in memory).
        /// </summary>
        /// <param name="entryBytes">Raw bytes to be enqueued to log</param>
        /// <param name="noCommit">Do not auto-commit</param>
        /// <returns>First logical address of added entries</returns>
        public long UnsafeEnqueueRaw(ReadOnlySpan<byte> entryBytes, bool noCommit = false)
        {
            long logicalAddress;
            while (!UnsafeTryEnqueueRaw(entryBytes, noCommit, out logicalAddress))
                Thread.Yield();
            return logicalAddress;

        }

        /// <summary>
        /// Commit metadata only (no records added to main log)
        /// </summary>
        /// <param name="info"></param>
        public void UnsafeCommitMetadataOnly(TsavoriteLogRecoveryInfo info)
        {
            lock (ongoingCommitRequests)
            {
                ongoingCommitRequests.Enqueue((info.UntilAddress, info));
            }
            try
            {
                epoch.Resume();
                if (!allocator.ShiftReadOnlyToTail(out _, out _))
                    CommitMetadataOnly(ref info);
            }
            finally
            {
                epoch.Suspend();
            }
        }

        /// <summary>
        /// Get page size in bits
        /// </summary>
        /// <returns></returns>
        public int UnsafeGetLogPageSizeBits()
            => allocator.LogPageSizeBits;

        /// <summary>
        /// Get read only lag address
        /// </summary>
        public long UnsafeGetReadOnlyLagAddress()
            => allocator.GetReadOnlyLagAddress();

        /// <summary>
        /// Enqueue batch of entries to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch of entries to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress))
                Thread.Yield();
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue<T>(T entry) where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entries">Batch of entries to be enqueued to log</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue<T>(IEnumerable<T> entries) where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (!TryEnqueue(entries, out logicalAddress))
                Thread.Yield();
            return logicalAddress;
        }
        #endregion

        #region TryEnqueue
        /// <summary>
        /// Try to enqueue entry to log (in memory). If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue<T>(T entry, out long logicalAddress) where T : ILogEnqueueEntry
        {
            logicalAddress = 0;
            var length = entry.SerializedLength;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
                if (logicalAddress == 0)
                {
                    epoch.Suspend();
                    if (cannedException != null) throw cannedException;
                    return false;
                }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            entry.SerializeTo(new Span<byte>((void*)(headerSize + physicalAddress), length));
            SetHeader(length, (byte*)physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to enqueue batch of entries as a single atomic unit (to memory). Entire 
        /// batch needs to fit on one log page.
        /// </summary>
        /// <param name="entries">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue<T>(IEnumerable<T> entries, out long logicalAddress) where T : ILogEnqueueEntry
        {
            logicalAddress = 0;

            var allocatedLength = 0;
            foreach (var entry in entries)
            {
                allocatedLength += Align(entry.SerializedLength) + headerSize;
            }

            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();
            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);

            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            foreach (var entry in entries)
            {
                var length = entry.SerializedLength;
                entry.SerializeTo(new Span<byte>((void*)(headerSize + physicalAddress), length));
                SetHeader(length, (byte*)physicalAddress);
                physicalAddress += Align(length) + headerSize;
            }
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to enqueue entry to log (in memory). If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue(byte[] entry, out long logicalAddress)
        {
            logicalAddress = 0;
            var length = entry.Length;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
                if (logicalAddress == 0)
                {
                    epoch.Suspend();
                    if (cannedException != null) throw cannedException;
                    return false;
                }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), length, length);
            SetHeader(length, (byte*)physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to enqueue raw pre-formatted bytes with headers to the log (in memory). If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entryBytes">Entry bytes to be enqueued to log</param>
        /// <param name="noCommit">Do not auto-commit</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool UnsafeTryEnqueueRaw(ReadOnlySpan<byte> entryBytes, bool noCommit, out long logicalAddress)
        {
            int length = entryBytes.Length;

            // Length should be pre-aligned
            Debug.Assert(length == Align(length));
            logicalAddress = 0;
            int allocatedLength = length;
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
                if (logicalAddress == 0)
                {
                    epoch.Suspend();
                    if (cannedException != null) throw cannedException;
                    return false;
                }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            entryBytes.CopyTo(new Span<byte>((byte*)physicalAddress, length));
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit && !noCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to append entry to log. If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be appended to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue(ReadOnlySpan<byte> entry, out long logicalAddress)
        {
            logicalAddress = 0;
            var length = entry.Length;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            fixed (byte* bp = &entry.GetPinnableReference())
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), length, length);
            SetHeader(length, (byte*)physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Append a user-defined blittable struct header atomically to the log.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        public unsafe void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader);
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = AllocateBlock(allocatedLength);

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
        }

        /// <summary>
        /// Append a user-defined blittable struct header and one <see cref="SpanByte"/> entry atomically to the log.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        public unsafe void Enqueue<THeader>(THeader userHeader, ref SpanByte item, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader) + item.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = AllocateBlock(allocatedLength);

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            item.CopyTo(physicalAddress + headerSize + sizeof(THeader));
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
        }

        /// <summary>
        /// Append a user-defined blittable struct header and two <see cref="SpanByte"/> entries entries atomically to the log.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        public unsafe void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader) + item1.TotalSize + item2.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = AllocateBlock(allocatedLength);

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            item1.CopyTo(physicalAddress + headerSize + sizeof(THeader));
            item2.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize);
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
        }

        /// <summary>
        /// Append a user-defined blittable struct header and three <see cref="SpanByte"/> entries entries atomically to the log.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <param name="item3"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        public unsafe void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref SpanByte item3, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader) + item1.TotalSize + item2.TotalSize + item3.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = AllocateBlock(allocatedLength);

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            item1.CopyTo(physicalAddress + headerSize + sizeof(THeader));
            item2.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize);
            item3.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize + item2.TotalSize);
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
        }

        /// <summary>
        /// Append a user-defined header byte and a <see cref="SpanByte"/> entry atomically to the log.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        public unsafe void Enqueue(byte userHeader, ref SpanByte item, out long logicalAddress)
        {
            logicalAddress = 0;
            var length = sizeof(byte) + item.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = AllocateBlock(allocatedLength);

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *physicalAddress = userHeader;
            item.CopyTo(physicalAddress + sizeof(byte));
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long AllocateBlock(int recordSize)
        {
            while (true)
            {
                var flushEvent = allocator.FlushEvent;
                var logicalAddress = allocator.TryAllocate(recordSize);
                if (logicalAddress > 0)
                    return logicalAddress;

                if (logicalAddress == 0)
                {
                    epoch.Suspend();
                    if (cannedException != null) throw cannedException;
                    try
                    {
                        flushEvent.Wait();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                }

                // logicalAddress is < 0 so we do not expect flushEvent to be signaled; refresh the epoch and retry now
                allocator.TryComplete();
                epoch.ProtectAndDrain();
                Thread.Yield();
            }
        }

        /// <summary>
        /// Try to append a user-defined blittable struct header and two <see cref="SpanByte"/> entries entries atomically to the log.
        /// If it returns true, we are done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader) + item1.TotalSize + item2.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            item1.CopyTo(physicalAddress + headerSize + sizeof(THeader));
            item2.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize);
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to append a user-defined blittable struct header and three <see cref="SpanByte"/> entries entries atomically to the log.
        /// If it returns true, we are done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <param name="item3"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref SpanByte item3, out long logicalAddress)
            where THeader : unmanaged
        {
            logicalAddress = 0;
            var length = sizeof(THeader) + item1.TotalSize + item2.TotalSize + item3.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *(THeader*)(physicalAddress + headerSize) = userHeader;
            item1.CopyTo(physicalAddress + headerSize + sizeof(THeader));
            item2.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize);
            item3.CopyTo(physicalAddress + headerSize + sizeof(THeader) + item1.TotalSize + item2.TotalSize);
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to append a user-defined header byte and a <see cref="SpanByte"/> entry atomically to the log. If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="userHeader"></param>
        /// <param name="item"></param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue(byte userHeader, ref SpanByte item, out long logicalAddress)
        {
            logicalAddress = 0;
            var length = sizeof(byte) + item.TotalSize;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);
            *physicalAddress = userHeader;
            item.CopyTo(physicalAddress + sizeof(byte));
            SetHeader(length, physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        /// <summary>
        /// Try to enqueue batch of entries as a single atomic unit (to memory). Entire 
        /// batch needs to fit on one log page.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public bool TryEnqueue(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress)
        {
            return TryAppend(readOnlySpanBatch, out logicalAddress, out _);
        }
        #endregion

        #region EnqueueAsync
        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(byte[] entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entry, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entry, token);
        }

        private static async ValueTask<long> SlowEnqueueAsync(TsavoriteLog @this, byte[] entry, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(ReadOnlyMemory<byte> entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entry.Span, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entry, token);
        }

        private static async ValueTask<long> SlowEnqueueAsync(TsavoriteLog @this, ReadOnlyMemory<byte> entry, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry.Span, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(readOnlySpanBatch, out long address))
                return new ValueTask<long>(address);

            return SlowEnqueueAsync(this, readOnlySpanBatch, token);
        }

        private static async ValueTask<long> SlowEnqueueAsync(TsavoriteLog @this, IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public ValueTask<long> EnqueueAsync<T>(T entry, CancellationToken token = default) where T : ILogEnqueueEntry
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entry, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entry, token);
        }

        private static async ValueTask<long> SlowEnqueueAsync<T>(TsavoriteLog @this, T entry, CancellationToken token)
            where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entries">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of first added entry</returns>
        public ValueTask<long> EnqueueAsync<T>(IEnumerable<T> entries, CancellationToken token = default) where T : ILogEnqueueEntry
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entries, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entries, token);
        }

        private static async ValueTask<long> SlowEnqueueAsync<T>(TsavoriteLog @this, IEnumerable<T> entry, CancellationToken token)
            where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
        }
        #endregion

        #region WaitForCommit and WaitForCommitAsync

        /// <summary>
        /// Spin-wait until specified address (or tail) and commit num (or latest), to commit to 
        /// storage. Does NOT itself issue a commit, just waits for commit. So you should 
        /// ensure that someone else causes the commit to happen.
        /// </summary>
        /// <param name="untilAddress">Address until which we should wait for commit, default 0 for tail of log</param>
        /// <param name ="commitNum">CommitNum until which we should wait for commit, default -1 for latest as of now</param>
        /// <returns></returns>
        public void WaitForCommit(long untilAddress = 0, long commitNum = -1)
        {
            if (untilAddress == 0) untilAddress = TailAddress;
            if (commitNum == -1) commitNum = this.commitNum;

            while (commitNum > persistedCommitNum || untilAddress > CommittedUntilAddress)
            {
                if (cannedException != null) throw cannedException;
                Thread.Yield();
            }
        }

        /// <summary>
        /// Wait until specified address (or tail) and commit num (or latest), to commit to 
        /// storage. Does NOT itself issue a commit, just waits for commit. So you should 
        /// ensure that someone else causes the commit to happen.
        /// </summary>
        /// <param name="untilAddress">Address until which we should wait for commit, default 0 for tail of log</param>
        /// <param name ="commitNum">CommitNum until which we should wait for commit, default -1 for latest as of now</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            var tailAddress = untilAddress;
            if (tailAddress == 0) tailAddress = allocator.GetTailAddress();

            if (commitNum == -1) commitNum = this.commitNum;
            while (CommittedUntilAddress < tailAddress || persistedCommitNum < commitNum)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = linkedCommitInfo.NextTask;
            }
        }

        /// <summary>
        /// Wait for more data to get added to the uncommitted tail of the log
        /// </summary>
        /// <returns>true if there's more data available to be read; false if there will never be more data (log has been shutdown)</returns>
        public async ValueTask<bool> WaitUncommittedAsync(long nextAddress, CancellationToken token = default)
        {
            Debug.Assert(AutoRefreshSafeTailAddress);
            if (nextAddress < SafeTailAddress)
                return true;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                if (LogCompleted && nextAddress == TailAddress) return false;

                var tcs = refreshUncommittedTcs;
                if (tcs == null)
                {
                    var newTcs = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);
                    tcs = Interlocked.CompareExchange(ref refreshUncommittedTcs, newTcs, null);
                    tcs ??= newTcs; // successful CAS so update the local var
                }

                if (nextAddress < SafeTailAddress)
                    return true;

                // Ignore refresh-uncommitted exceptions, except when the token is signaled
                try
                {
                    await tcs.Task.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return false; }
                catch when (!token.IsCancellationRequested) { }
            }
        }
        #endregion

        #region Commit and CommitAsync

        /// <summary>
        /// Issue commit request for log (until tail)
        /// </summary>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately.</param>
        /// <returns> whether there is anything to commit. </returns>

        public void Commit(bool spinWait = false)
        {
            // Take a lower-bound of the content of this commit in case our request is filtered but we need to spin
            var tail = TailAddress;
            var lastCommit = commitNum;

            var success = CommitInternal(out var actualTail, out var actualCommitNum, true, null, -1, null);
            if (!spinWait) return;
            if (success)
                WaitForCommit(actualTail, actualCommitNum);
            else
                // Still need to imitate semantics to spin until all previous enqueues are committed when commit has been filtered  
                WaitForCommit(tail, lastCommit);
        }

        /// <summary>
        /// Issue a strong commit request for log (until tail) with the given commitNum. Strong commits bypass commit policies
        /// and will never be compressed with other concurrent commit requests.
        /// </summary>
        /// <param name="commitTail">The tail committed by this call</param>
        /// <param name="actualCommitNum">
        /// A unique, monotonically increasing identifier for the commit that can be used to recover to exactly this commit
        /// </param>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately</param>
        /// <param name="cookie">
        /// A custom piece of metadata to be associated with this commit. If commit is successful, any recovery from
        /// this commit will recover the cookie in RecoveredCookie field. Note that cookies are not stored by TsavoriteLog
        /// itself, so the user is responsible for tracking cookie content and supplying it to every commit call if needed
        /// </param>
        /// <param name="proposedCommitNum">
        /// Proposal for the identifier to use for this commit, or -1 if the system should pick one. If supplied with
        /// a non -1 value, commit is guaranteed to have the supplied identifier if commit call is successful
        /// </param>
        /// <param name="callback"> callback function that will be invoked when strong commit is persistent </param>
        /// <returns>Whether commit is successful </returns>
        public bool CommitStrongly(out long commitTail, out long actualCommitNum, bool spinWait = false, byte[] cookie = null, long proposedCommitNum = -1, Action callback = null)
        {
            if (!CommitInternal(out commitTail, out actualCommitNum, false, cookie, proposedCommitNum, callback))
                return false;
            if (spinWait)
                WaitForCommit(commitTail, actualCommitNum);
            return true;
        }

        /// <summary>
        /// Async commit log (until tail), completes only when we 
        /// complete the commit. Throws exception if this or any 
        /// ongoing commit fails.
        /// </summary>
        /// <returns></returns>
        public async ValueTask CommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Take a lower-bound of the content of this commit in case our request is filtered but we need to wait
            var tail = TailAddress;
            var lastCommit = commitNum;

            var task = CommitTask;
            var success = CommitInternal(out var actualTail, out var actualCommitNum, true, null, -1, null);

            if (success)
            {
                while (CommittedUntilAddress < actualTail || persistedCommitNum < actualCommitNum)
                {
                    var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                    task = linkedCommitInfo.NextTask;
                }
            }
            else
            {
                while (CommittedUntilAddress < tail || persistedCommitNum < lastCommit)
                {
                    var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                    task = linkedCommitInfo.NextTask;
                }
            }
        }

        /// <summary>
        /// Async commit log (until tail), completes only when we 
        /// complete the commit. Throws exception if any commit
        /// from prevCommitTask to current fails.
        /// </summary>
        /// <returns></returns>
        public async ValueTask<Task<LinkedCommitInfo>> CommitAsync(Task<LinkedCommitInfo> prevCommitTask, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Take a lower-bound of the content of this commit in case our request is filtered but we need to spin
            var tail = TailAddress;
            var lastCommit = commitNum;

            if (prevCommitTask == null) prevCommitTask = CommitTask;

            var success = CommitInternal(out var actualTail, out var actualCommitNum, true, null, -1, null);


            if (success)
            {
                while (CommittedUntilAddress < actualTail || persistedCommitNum < actualCommitNum)
                {
                    var linkedCommitInfo = await prevCommitTask.WithCancellationAsync(token).ConfigureAwait(false);
                    if (linkedCommitInfo.CommitInfo.UntilAddress < actualTail || persistedCommitNum < actualCommitNum)
                        prevCommitTask = linkedCommitInfo.NextTask;
                    else
                        return linkedCommitInfo.NextTask;
                }
            }
            else
            {
                while (CommittedUntilAddress < tail || persistedCommitNum < lastCommit)
                {
                    var linkedCommitInfo = await prevCommitTask.WithCancellationAsync(token).ConfigureAwait(false);
                    if (linkedCommitInfo.CommitInfo.UntilAddress < actualTail || persistedCommitNum < actualCommitNum)
                        prevCommitTask = linkedCommitInfo.NextTask;
                    else
                        return linkedCommitInfo.NextTask;
                }
            }

            return prevCommitTask;
        }

        /// <summary>
        /// Issue commit request for log (until tail) with the given commitNum
        /// </summary>
        /// <param name="cookie">
        /// A custom piece of metadata to be associated with this commit. If commit is successful, any recovery from
        /// this commit will recover the cookie in RecoveredCookie field. Note that cookies are not stored by TsavoriteLog
        /// itself, so the user is responsible for tracking cookie content and supplying it to every commit call if needed
        /// </param>
        /// <param name="proposedCommitNum">
        /// Proposal for the identifier to use for this commit, or -1 if the system should pick one. If supplied with
        /// a non -1 value, commit is guaranteed to have the supplied identifier if commit call is successful
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Whether commit is successful, commit tail, and actual commit number</returns>
        public async ValueTask<(bool success, long commitTail, long actualCommitNum)> CommitStronglyAsync(byte[] cookie = null, long proposedCommitNum = -1, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            if (!CommitInternal(out var commitTail, out var actualCommitNum, false, cookie, proposedCommitNum, null))
                return (false, commitTail, actualCommitNum);

            while (CommittedUntilAddress < commitTail || persistedCommitNum < actualCommitNum)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = linkedCommitInfo.NextTask;
            }

            return (true, commitTail, actualCommitNum);
        }
        #endregion

        #region EnqueueAndWaitForCommit

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(byte[] entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            WaitForCommit(logicalAddress + 1);
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(ReadOnlySpan<byte> entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            WaitForCommit(logicalAddress + 1);
            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress))
                Thread.Yield();
            WaitForCommit(logicalAddress + 1);
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public long EnqueueAndWaitForCommit<T>(T entry) where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            WaitForCommit(logicalAddress + 1);
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entries">Entries to be enqueued to log</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of first added entry</returns>
        public long EnqueueAndWaitForCommit<T>(IEnumerable<T> entries) where T : ILogEnqueueEntry
        {
            long logicalAddress;
            while (!TryEnqueue(entries, out logicalAddress))
                Thread.Yield();
            WaitForCommit(logicalAddress + 1);
            return logicalAddress;
        }

        #endregion

        #region EnqueueAndWaitForCommitAsync

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(byte[] entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(ReadOnlyMemory<byte> entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entry.Span, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log (async) - completes after batch is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync<T>(T entry, CancellationToken token = default) where T : ILogEnqueueEntry
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log (async) - completes after batch is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entries"> entries to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">type of entry</typeparam>
        /// <returns>Logical address of added entry</returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync<T>(IEnumerable<T> entries,
            CancellationToken token = default) where T : ILogEnqueueEntry
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entries, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }
        #endregion

        /// <summary>
        /// Truncate the log until, but not including, untilAddress. **User should ensure
        /// that the provided address is a valid starting address for some record.** The
        /// truncation is not persisted until the next commit.
        /// </summary>
        /// <param name="untilAddress">Until address</param>
        public void TruncateUntil(long untilAddress)
        {
            Utility.MonotonicUpdate(ref beginAddress, untilAddress, out _);
        }

        /// <summary>
        /// Unsafely shift the begin address of the log and optionally truncate files on disk, without committing.
        /// Do not use unless you know what you are doing.
        /// </summary>
        /// <param name="untilAddress"></param>
        /// <param name="snapToPageStart"></param>
        /// <param name="truncateLog"></param>
        /// <param name="noFlush"></param>
        public void UnsafeShiftBeginAddress(long untilAddress, bool snapToPageStart = false, bool truncateLog = false, bool noFlush = false)
        {
            if (Utility.MonotonicUpdate(ref beginAddress, untilAddress, out _))
            {
                if (snapToPageStart)
                    untilAddress &= ~allocator.PageSizeMask;

                bool epochProtected = epoch.ThisInstanceProtected();
                try
                {
                    if (!epochProtected)
                        epoch.Resume();
                    allocator.ShiftBeginAddress(untilAddress, truncateLog, noFlush);
                }
                finally
                {
                    if (!epochProtected)
                        epoch.Suspend();
                }
            }
        }

        /// <summary>
        /// Truncate the log until the start of the page corresponding to untilAddress. This is 
        /// safer than TruncateUntil, as page starts are always a valid truncation point. The
        /// truncation is not persisted until the next commit.
        /// </summary>
        /// <param name="untilAddress">Until address</param>
        public void TruncateUntilPageStart(long untilAddress)
        {
            Utility.MonotonicUpdate(ref beginAddress, untilAddress & ~allocator.PageSizeMask, out _);
        }

        /// <summary>
        /// Pull-based iterator interface for scanning Tsavorite log
        /// </summary>
        /// <param name="beginAddress">Begin address for scan.</param>
        /// <param name="endAddress">End address for scan (or long.MaxValue for tailing).</param>
        /// <param name="name">Name of iterator, if we need to persist/recover it (default null - do not persist).</param>
        /// <param name="recover">Whether to recover named iterator from latest commit (if exists). If false, iterator starts from beginAddress.</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <param name="scanUncommitted">Whether we scan uncommitted data</param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public TsavoriteLogScanIterator Scan(long beginAddress, long endAddress, string name = null, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
        {
            if (readOnlyMode)
            {
                scanBufferingMode = ScanBufferingMode.SinglePageBuffering;

                if (name != null)
                    throw new TsavoriteException("Cannot use named iterators with read-only TsavoriteLog");
                if (scanUncommitted)
                    throw new TsavoriteException("Cannot use scanUncommitted with read-only TsavoriteLog");
            }

            if (scanUncommitted && !AutoRefreshSafeTailAddress)
                throw new TsavoriteException("Cannot use scanUncommitted without setting AutoRefreshSafeTailAddress to true in TsavoriteLog settings");

            TsavoriteLogScanIterator iter;
            if (recover && name != null && RecoveredIterators != null && RecoveredIterators.ContainsKey(name))
                iter = new TsavoriteLogScanIterator(this, allocator, RecoveredIterators[name], endAddress, getMemory, scanBufferingMode, epoch, headerSize, name, scanUncommitted, logger: logger);
            else
                iter = new TsavoriteLogScanIterator(this, allocator, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize, name, scanUncommitted, logger: logger);

            if (name != null)
            {
                if (name.Length > 20)
                    throw new TsavoriteException("Max length of iterator name is 20 characters");
                if (PersistedIterators.ContainsKey(name))
                    logger?.LogDebug("Iterator name exists, overwriting");
                PersistedIterators[name] = iter;
            }

            if (Interlocked.Increment(ref logRefCount) == 1)
                throw new TsavoriteException("Cannot scan disposed log instance");
            return iter;
        }

        /// <summary>
        /// Random read record from log, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="estimatedLength">Estimated length of entry, if known</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<(byte[], int)> ReadAsync(long address, int estimatedLength = 0, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            epoch.Resume();
            if (address >= CommittedUntilAddress || address < BeginAddress)
            {
                epoch.Suspend();
                return default;
            }
            var ctx = new SimpleReadContext
            {
                logicalAddress = address,
                completedRead = new SemaphoreSlim(0)
            };
            unsafe
            {
                allocator.AsyncReadRecordToMemory(address, headerSize + estimatedLength, AsyncGetFromDiskCallback, ref ctx);
            }
            epoch.Suspend();
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            return GetRecordAndFree(ctx.record);
        }

        /// <summary>
        /// Random read record from log as IMemoryOwner&lt;byte&gt;, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="memoryPool">MemoryPool to rent the destination buffer from</param>
        /// <param name="estimatedLength">Estimated length of entry, if known</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<(IMemoryOwner<byte>, int)> ReadAsync(long address, MemoryPool<byte> memoryPool, int estimatedLength = 0, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            epoch.Resume();
            if (address >= CommittedUntilAddress || address < BeginAddress)
            {
                epoch.Suspend();
                return default;
            }
            var ctx = new SimpleReadContext
            {
                logicalAddress = address,
                completedRead = new SemaphoreSlim(0)
            };
            unsafe
            {
                allocator.AsyncReadRecordToMemory(address, headerSize + estimatedLength, AsyncGetFromDiskCallback, ref ctx);
            }
            epoch.Suspend();
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            return GetRecordAsMemoryOwnerAndFree(ctx.record, memoryPool);
        }

        /// <summary>
        /// Random read record from log, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<int> ReadRecordLengthAsync(long address, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            epoch.Resume();
            if (address >= CommittedUntilAddress || address < BeginAddress)
            {
                epoch.Suspend();
                return default;
            }
            var ctx = new SimpleReadContext
            {
                logicalAddress = address,
                completedRead = new SemaphoreSlim(0)
            };
            unsafe
            {
                allocator.AsyncReadRecordToMemory(address, headerSize, AsyncGetHeaderOnlyFromDiskCallback, ref ctx);
            }
            epoch.Suspend();
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            return GetRecordLengthAndFree(ctx.record);
        }

        /// <summary>
        /// Initiate auto refresh safe tail address, called with epoch protection
        /// </summary>
        private void DoAutoRefreshSafeTailAddress()
        {
            if (_ongoingAutoRefreshSafeTailAddress == 0 && Interlocked.CompareExchange(ref _ongoingAutoRefreshSafeTailAddress, 1, 0) == 0)
                AutoRefreshSafeTailAddressRunner(false);
        }

        private void EpochProtectAutoRefreshSafeTailAddressRunner()
        {
            try
            {
                epoch.Resume();
                AutoRefreshSafeTailAddressRunner(false);
            }
            finally
            {
                epoch.Suspend();
            }
        }

        private void AutoRefreshSafeTailAddressRunner(bool recurse)
        {
            long tail = 0;
            do
            {
                tail = TailAddress;
                if (tail > SafeTailAddress)
                {
                    if (recurse)
                        Task.Run(EpochProtectAutoRefreshSafeTailAddressRunner);
                    else
                        epoch.BumpCurrentEpoch(() => AutoRefreshSafeTailAddressBumpCallback(tail));
                    return;
                }
                _ongoingAutoRefreshSafeTailAddress = 0;
            } while (tail > SafeTailAddress && _ongoingAutoRefreshSafeTailAddress == 0 && Interlocked.CompareExchange(ref _ongoingAutoRefreshSafeTailAddress, 1, 0) == 0);
        }

        private void AutoRefreshSafeTailAddressBumpCallback(long tailAddress)
        {
            if (Utility.MonotonicUpdate(ref SafeTailAddress, tailAddress, out long oldSafeTailAddress))
            {
                var tcs = refreshUncommittedTcs;
                if (tcs != null && Interlocked.CompareExchange(ref refreshUncommittedTcs, null, tcs) == tcs)
                    tcs.SetResult(Empty.Default);
                var _callback = SafeTailShiftCallback;
                if (_callback != null)
                {
                    // We invoke callback outside epoch protection
                    bool isProtected = epoch.ThisInstanceProtected();
                    if (isProtected) epoch.Suspend();
                    try
                    {
                        _callback.Invoke(oldSafeTailAddress, tailAddress);
                    }
                    finally
                    {
                        if (isProtected) epoch.Resume();
                    }
                }
            }
            AutoRefreshSafeTailAddressRunner(true);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Align(int length)
        {
            return (length + 3) & ~3;
        }

        /// <summary>
        /// Commit log
        /// </summary>
        private void CommitCallback(CommitInfo commitInfo)
        {
            // Using count is safe as a fast filtering mechanism to reduce number of invocations despite concurrency
            if (ongoingCommitRequests.Count == 0 && commitInfo.ErrorCode == 0) return;
            commitQueue.EnqueueAndTryWork(commitInfo, asTask: true);
        }

        private unsafe bool TryEnqueueCommitRecord(ref TsavoriteLogRecoveryInfo info)
        {
            var entryBodySize = info.SerializedSize();

            int allocatedLength = headerSize + Align(entryBodySize);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            var logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }
            // Finish filling in all fields
            info.BeginAddress = BeginAddress;
            info.UntilAddress = logicalAddress + allocatedLength;

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);

            var entryBody = info.ToByteArray();
            fixed (byte* bp = entryBody)
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), entryBody.Length, entryBody.Length);
            SetCommitRecordHeader(entryBody.Length, (byte*)physicalAddress);
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            // Return the commit tail
            return true;
        }

        private bool ShouldCommmitMetadata(ref TsavoriteLogRecoveryInfo info)
        {
            return beginAddress > CommittedBeginAddress || IteratorsChanged(ref info) || info.Cookie != null;
        }

        private void CommitMetadataOnly(ref TsavoriteLogRecoveryInfo info)
        {
            var fromAddress = CommittedUntilAddress > info.BeginAddress ? CommittedUntilAddress : info.BeginAddress;
            var untilAddress = FlushedUntilAddress > info.BeginAddress ? FlushedUntilAddress : info.BeginAddress;

            CommitCallback(new CommitInfo
            {
                FromAddress = fromAddress,
                UntilAddress = untilAddress,
                ErrorCode = 0,
            });
        }

        private void UpdateCommittedState(TsavoriteLogRecoveryInfo recoveryInfo)
        {
            LastPersistedIterators = recoveryInfo.Iterators;
            CommittedBeginAddress = recoveryInfo.BeginAddress;
            CommittedUntilAddress = recoveryInfo.UntilAddress;
            recoveryInfo.CommitIterators(PersistedIterators);
            Utility.MonotonicUpdate(ref persistedCommitNum, recoveryInfo.CommitNum, out _);
        }

        private void WriteCommitMetadata(TsavoriteLogRecoveryInfo recoveryInfo)
        {
            // TODO: can change to write this in separate thread for fast commit

            // If we are in fast-commit, we may not write every metadata to disk. However, when we are deleting files
            // on disk, we have to write metadata for the new start location on disk so we know where to scan forward from.
            bool forceWriteMetadata = fastCommitMode && (allocator.BeginAddress < recoveryInfo.BeginAddress);
            logCommitManager.Commit(recoveryInfo.BeginAddress, recoveryInfo.UntilAddress,
                recoveryInfo.ToByteArray(), recoveryInfo.CommitNum, forceWriteMetadata);

            // If not fast committing, set committed state as we commit metadata explicitly only after metadata commit
            if (!fastCommitMode)
                UpdateCommittedState(recoveryInfo);
            // Issue any potential physical deletes due to shifts in begin address
            if (allocator.BeginAddress < recoveryInfo.BeginAddress)
            {
                try
                {
                    epoch.Resume();
                    allocator.ShiftBeginAddress(recoveryInfo.BeginAddress, true);
                }
                finally
                {
                    epoch.Suspend();
                }
            }
        }

        private void SerialCommitCallbackWorker(CommitInfo commitInfo)
        {
            if (commitInfo.ErrorCode != 0)
            {
                var exception = new CommitFailureException(new LinkedCommitInfo { CommitInfo = commitInfo },
                    $"Commit of address range [{commitInfo.FromAddress}-{commitInfo.UntilAddress}] failed with error code {commitInfo.ErrorCode}");
                if (tolerateDeviceFailure)
                {
                    var oldCommitTcs = commitTcs;
                    commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    oldCommitTcs.TrySetException(exception);
                    // Silently set flushed until past this range
                    Utility.MonotonicUpdate(ref allocator.FlushedUntilAddress, commitInfo.UntilAddress, out _);
                    allocator.UnsafeSkipError(commitInfo);
                }
                else
                {
                    cannedException = exception;
                    // Make sure future waiters do not get a fresh tcs
                    commitTcs.TrySetException(cannedException);
                }
                return;
            }
            // Check for the commit records included in this flush
            coveredCommits.Clear();
            lock (ongoingCommitRequests)
            {
                while (ongoingCommitRequests.Count != 0)
                {
                    var (addr, recoveryInfo) = ongoingCommitRequests.Peek();
                    if (addr > commitInfo.UntilAddress) break;
                    coveredCommits.Add(recoveryInfo);
                    ongoingCommitRequests.Dequeue();
                }
            }

            // Nothing was committed --- this was probably an auto-flush. Return now without touching any
            // commit task tracking.
            if (coveredCommits.Count == 0) return;

            var latestCommit = coveredCommits[coveredCommits.Count - 1];
            if (fastCommitMode)
            {
                // In fast commit mode, can safely set committed state to the latest flushed and invoke callbacks early
                UpdateCommittedState(latestCommit);
                foreach (var recoveryInfo in coveredCommits)
                {
                    recoveryInfo.Callback?.Invoke();
                    commitPolicy.OnCommitFinished(recoveryInfo);
                }
            }

            foreach (var recoveryInfo in coveredCommits)
            {
                // Only write out commit metadata if user cares about this as a distinct recoverable point
                if (!recoveryInfo.FastForwardAllowed) WriteCommitMetadata(recoveryInfo);
                if (!fastCommitMode)
                {
                    recoveryInfo.Callback?.Invoke();
                    commitPolicy.OnCommitFinished(recoveryInfo);
                }
            }

            // We fast-forwarded commits earlier, so write it out if not covered by another commit
            if (latestCommit.FastForwardAllowed) WriteCommitMetadata(latestCommit);

            // TODO: Can invoke earlier in the case of fast commit
            var _commitTcs = commitTcs;
            commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            var lci = new LinkedCommitInfo
            {
                CommitInfo = commitInfo,
                NextTask = commitTcs.Task
            };
            _commitTcs?.TrySetResult(lci);
        }

        private bool IteratorsChanged(ref TsavoriteLogRecoveryInfo info)
        {
            var _lastPersistedIterators = LastPersistedIterators;
            if (_lastPersistedIterators == null)
            {
                return info.Iterators != null && info.Iterators.Count != 0;
            }
            if (info.Iterators == null || _lastPersistedIterators.Count != info.Iterators.Count)
                return true;
            foreach (var item in _lastPersistedIterators)
            {
                if (info.Iterators.TryGetValue(item.Key, out var other))
                {
                    if (item.Value != other) return true;
                }
                else
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Synchronously recover instance to TsavoriteLog's latest valid commit, when being used as a readonly log iterator
        /// </summary>
        public void RecoverReadOnly()
        {
            if (!readOnlyMode)
                throw new TsavoriteException("This method can only be used with a read-only TsavoriteLog instance used for iteration. Set TsavoriteLogSettings.ReadOnlyMode to true during creation to indicate this.");

            RestoreLatest(out _, out _);
            SignalWaitingROIterators();
        }

        /// <summary>
        /// Asynchronously recover instance to TsavoriteLog's latest commit, when being used as a readonly log iterator
        /// </summary>
        public async ValueTask RecoverReadOnlyAsync(CancellationToken cancellationToken = default)
        {
            if (!readOnlyMode)
                throw new TsavoriteException("This method can only be used with a read-only TsavoriteLog instance used for iteration. Set TsavoriteLogSettings.ReadOnlyMode to true during creation to indicate this.");

            await RestoreLatestAsync(cancellationToken).ConfigureAwait(false);
            SignalWaitingROIterators();
        }

        private void SignalWaitingROIterators()
        {
            // One RecoverReadOnly use case is to allow a TsavoriteLogIterator to continuously read a mirror TsavoriteLog (over the same log storage) of a primary TsavoriteLog.
            // In this scenario, when the iterator arrives at the tail after a previous call to RestoreReadOnly, it will wait asynchronously until more data
            // is committed and read by a subsequent call to RecoverReadOnly. Here, we signal iterators that we have completed recovery.
            var _commitTcs = commitTcs;
            if (commitTcs.Task.Status != TaskStatus.Faulted || commitTcs.Task.Exception.InnerException as CommitFailureException != null)
            {
                commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            // Update commit to release pending iterators.
            var lci = new LinkedCommitInfo
            {
                CommitInfo = new CommitInfo { FromAddress = BeginAddress, UntilAddress = FlushedUntilAddress },
                NextTask = commitTcs.Task
            };
            _commitTcs?.TrySetResult(lci);
        }

        private bool LoadCommitMetadata(long commitNum, out TsavoriteLogRecoveryInfo info)
        {
            var commitInfo = logCommitManager.GetCommitMetadata(commitNum);
            if (commitInfo is null)
            {
                info = default;
                return false;
            }

            info = new TsavoriteLogRecoveryInfo();
            info.Initialize(commitInfo);

            if (info.CommitNum == -1)
                info.CommitNum = commitNum;

            return true;
        }

        private void RestoreLatest(out Dictionary<string, long> iterators, out byte[] cookie)
        {
            iterators = null;
            cookie = null;
            TsavoriteLogRecoveryInfo info = new();

            long scanStart = 0;
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                    {
                        scanStart = metadataCommit;
                        break;
                    }
                }
                catch { }
            }

            // Only in fast commit mode will we potentially need to recover from an entry in the log
            if (fastCommitMode)
            {
                // Disable safe guards temporarily
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                try
                {
                    using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                    scanIterator.ScanForwardForCommit(ref info);
                }
                catch { }
            }

            // If until address is 0, that means info is still its default value and we haven't been able to recover
            // from any any commit. Set the log to its start position and return
            if (info.UntilAddress == 0)
            {
                logger?.LogInformation("Unable to recover using any available commit");

                // Reset variables to normal
                allocator.Initialize();
                CommittedUntilAddress = Constants.kFirstValidAddress;
                beginAddress = allocator.BeginAddress;
                if (readOnlyMode)
                    allocator.HeadAddress = long.MaxValue;
                return;
            }

            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;

                try
                {
                    allocator.RestoreHybridLog(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress);
                }
                catch
                {
                    if (!tolerateDeviceFailure) throw;
                }
            }

            iterators = CompleteRestoreFromCommit(info);
            cookie = info.Cookie;
            commitNum = info.CommitNum;
            // After recovery  persisted commitnum remians 0 so we need to set it to latest commit number
            persistedCommitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;

            if (scanStart > 0) logCommitManager.OnRecovery(scanStart);
        }

        private void RestoreSpecificCommit(long requestedCommitNum, out Dictionary<string, long> iterators, out byte[] cookie)
        {
            iterators = null;
            cookie = null;
            TsavoriteLogRecoveryInfo info = new();

            // Find the closest commit metadata with commit num smaller than requested
            long scanStart = 0;
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                if (metadataCommit > requestedCommitNum) continue;
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                    {
                        scanStart = metadataCommit;
                        break;
                    }
                }
                catch { }
            }

            // Need to potentially scan log for the entry 
            if (scanStart < requestedCommitNum)
            {
                // If not in fast commit mode, do not scan log
                if (!fastCommitMode)
                    // In the case where precisely requested commit num is not available, can just throw exception
                    throw new TsavoriteException("requested commit num is not available");

                // If no exact metadata is found, scan forward to see if we able to find a commit entry
                // Shut up safe guards, I know what I am doing
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                try
                {
                    using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                    if (!scanIterator.ScanForwardForCommit(ref info, requestedCommitNum))
                        throw new TsavoriteException("requested commit num is not available");
                }
                catch { }
            }

            // At this point, we should have found the exact commit num requested
            Debug.Assert(info.CommitNum == requestedCommitNum);
            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
                try
                {
                    allocator.RestoreHybridLog(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress);
                }
                catch
                {
                    if (!tolerateDeviceFailure) throw;
                }
            }

            iterators = CompleteRestoreFromCommit(info);
            cookie = info.Cookie;
            commitNum = persistedCommitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;

            if (scanStart > 0) logCommitManager.OnRecovery(scanStart);
        }

        /// <summary>
        /// Restore log asynchronously
        /// </summary>
        private async ValueTask<(Dictionary<string, long>, byte[])> RestoreLatestAsync(CancellationToken cancellationToken)
        {
            TsavoriteLogRecoveryInfo info = new();

            long scanStart = 0;
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                    {
                        scanStart = metadataCommit;
                        break;
                    }
                }
                catch { }
            }

            // Only in fast commit mode will we potentially need to recover from an entry in the log
            if (fastCommitMode)
            {
                // Shut up safe guards, I know what I am doing
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                try
                {
                    using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                    scanIterator.ScanForwardForCommit(ref info);
                }
                catch { }
            }

            // if until address is 0, that means info is still its default value and we haven't been able to recover
            // from any any commit. Set the log to its start position and return
            if (info.UntilAddress == 0)
            {
                logger?.LogDebug("Unable to recover using any available commit");
                // Reset things to be something normal lol
                allocator.Initialize();
                CommittedUntilAddress = Constants.kFirstValidAddress;
                beginAddress = allocator.BeginAddress;
                if (readOnlyMode)
                    allocator.HeadAddress = long.MaxValue;
                return (new Dictionary<string, long>(), null);
            }

            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
                await allocator.RestoreHybridLogAsync(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            var iterators = CompleteRestoreFromCommit(info);
            var cookie = info.Cookie;
            commitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;

            if (scanStart > 0) logCommitManager.OnRecovery(scanStart);

            return (iterators, cookie);
        }

        private Dictionary<string, long> CompleteRestoreFromCommit(TsavoriteLogRecoveryInfo info)
        {
            CommittedUntilAddress = info.UntilAddress;
            CommittedBeginAddress = info.BeginAddress;
            SafeTailAddress = info.UntilAddress;

            // Fix uncommitted addresses in iterators
            var recoveredIterators = info.Iterators;
            if (recoveredIterators != null)
            {
                List<string> keys = recoveredIterators.Keys.ToList();
                foreach (var key in keys)
                    if (recoveredIterators[key] > SafeTailAddress)
                        recoveredIterators[key] = SafeTailAddress;
            }
            return recoveredIterators;
        }

        /// <summary>
        /// Try to append batch of entries as a single atomic unit. Entire batch
        /// needs to fit on one page.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <param name="allocatedLength">Actual allocated length</param>
        /// <returns>Whether the append succeeded</returns>
        private unsafe bool TryAppend(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress, out int allocatedLength)
        {
            logicalAddress = 0;

            int totalEntries = readOnlySpanBatch.TotalEntries();
            allocatedLength = 0;
            for (int i = 0; i < totalEntries; i++)
            {
                allocatedLength += Align(readOnlySpanBatch.Get(i).Length) + headerSize;
            }

            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();
            if (commitNum == long.MaxValue) throw new TsavoriteException("Attempting to enqueue into a completed log");

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);

            if (logicalAddress == 0)
            {
                epoch.Suspend();
                if (cannedException != null) throw cannedException;
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            for (int i = 0; i < totalEntries; i++)
            {
                var span = readOnlySpanBatch.Get(i);
                var entryLength = span.Length;
                fixed (byte* bp = &span.GetPinnableReference())
                    Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), entryLength, entryLength);
                SetHeader(entryLength, (byte*)physicalAddress);
                physicalAddress += Align(entryLength) + headerSize;
            }
            if (AutoRefreshSafeTailAddress) DoAutoRefreshSafeTailAddress();
            epoch.Suspend();
            if (AutoCommit) Commit();
            return true;
        }

        private unsafe void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            var ctx = (SimpleReadContext)context;

            if (errorCode != 0)
            {
                logger?.LogError("AsyncGetFromDiskCallback error: {0}", errorCode);
                ctx.record.Return();
                ctx.record = null;
                ctx.completedRead.Release();
            }
            else
            {
                var record = ctx.record.GetValidPointer();
                var length = GetLength(record);

                if (length < 0 || length > allocator.PageSize)
                {
                    logger?.LogDebug("Invalid record length found: " + length);
                    ctx.record.Return();
                    ctx.record = null;
                    ctx.completedRead.Release();
                }
                else
                {
                    int requiredBytes = headerSize + length;
                    if (ctx.record.available_bytes >= requiredBytes)
                    {
                        ctx.completedRead.Release();
                    }
                    else
                    {
                        ctx.record.Return();
                        allocator.AsyncReadRecordToMemory(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ref ctx);
                    }
                }
            }
        }

        private void AsyncGetHeaderOnlyFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            var ctx = (SimpleReadContext)context;

            if (errorCode != 0)
            {
                logger?.LogError("AsyncGetFromDiskCallback error: {0}", errorCode);
                ctx.record.Return();
                ctx.record = null;
                ctx.completedRead.Release();
            }
            else
            {
                if (ctx.record.available_bytes < headerSize)
                {
                    logger?.LogDebug("No record header present at address: " + ctx.logicalAddress);
                    ctx.record.Return();
                    ctx.record = null;
                }
                ctx.completedRead.Release();
            }
        }

        private (byte[], int) GetRecordAndFree(SectorAlignedMemory record)
        {
            if (record == null)
                return (null, 0);

            byte[] result;
            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);
                if (!VerifyChecksum(ptr, length))
                {
                    throw new TsavoriteException("Checksum failed for read");
                }
                result = getMemory != null ? getMemory(length) : new byte[length];
                fixed (byte* bp = result)
                {
                    Buffer.MemoryCopy(ptr + headerSize, bp, length, length);
                }
            }
            record.Return();
            return (result, length);
        }

        private (IMemoryOwner<byte>, int) GetRecordAsMemoryOwnerAndFree(SectorAlignedMemory record, MemoryPool<byte> memoryPool)
        {
            if (record == null)
                return (null, 0);

            IMemoryOwner<byte> result;
            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);
                if (!VerifyChecksum(ptr, length))
                {
                    throw new TsavoriteException("Checksum failed for read");
                }
                result = memoryPool.Rent(length);

                fixed (byte* bp = result.Memory.Span)
                {
                    Buffer.MemoryCopy(ptr + headerSize, bp, length, length);
                }
            }

            record.Return();
            return (result, length);
        }

        private int GetRecordLengthAndFree(SectorAlignedMemory record)
        {
            if (record == null)
                return 0;

            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);

                // forego checksum verification since record may not be read in full by AsyncGetHeaderOnlyFromDiskCallback()
            }

            record.Return();
            return length;
        }


        private bool CommitInternal(out long commitTail, out long actualCommitNum, bool fastForwardAllowed, byte[] cookie, long proposedCommitNum, Action callback)
        {
            if (cannedException != null)
                throw cannedException;

            commitTail = actualCommitNum = 0;

            if (readOnlyMode)
                throw new TsavoriteException("Cannot commit in read-only mode");

            if (fastForwardAllowed && (cookie != null || proposedCommitNum != -1 || callback != null))
                throw new TsavoriteException(
                    "Fast forwarding a commit is only allowed when no cookie, commit num, or callback is specified");

            var info = new TsavoriteLogRecoveryInfo
            {
                FastForwardAllowed = fastForwardAllowed,
                Cookie = cookie,
                Callback = callback,
            };
            info.SnapshotIterators(PersistedIterators);
            var commitRequired = ShouldCommmitMetadata(ref info) || (commitCoveredAddress < TailAddress);
            // Only apply commit policy if not a strong commit
            if (fastForwardAllowed && !commitPolicy.AdmitCommit(TailAddress, commitRequired))
                return false;

            // This critical section serializes commit record creation / commit content generation and ensures that the
            // long address are sorted in outstandingCommitRecords. Ok because we do not expect heavy contention on the
            // commit code path
            lock (ongoingCommitRequests)
            {
                if (commitCoveredAddress == TailAddress && !commitRequired)
                    // Nothing to commit if no metadata update and no new entries
                    return false;
                if (commitNum == long.MaxValue)
                {
                    // log has been closed, throw an exception
                    throw new TsavoriteException("log has already been closed");
                }

                // Make sure we will not be allowed to back out of a commit if AdmitCommit returns true, as the commit policy
                // may need to update internal logic for every true response. We might waste some commit nums if commit
                // policy filters out a lot of commits, but that's fine.
                if (proposedCommitNum == -1)
                    info.CommitNum = actualCommitNum = ++commitNum;
                else if (proposedCommitNum > commitNum)
                    info.CommitNum = actualCommitNum = commitNum = proposedCommitNum;
                else
                    // Invalid commit num
                    return false;

                // Normally --- only need commit records if fast committing.
                if (fastCommitMode)
                {
                    // Ok to retry in critical section, any concurrently invoked commit would block, but cannot progress
                    // anyways if no record can be enqueued
                    while (!TryEnqueueCommitRecord(ref info)) Thread.Yield();
                    commitTail = info.UntilAddress;
                }
                else
                {
                    // If not using fastCommitMode, do not need to allocate a commit record. Instead, set the content
                    // of this commit to the current tail and base all commit metadata on this address, even though
                    // perhaps more entries will be flushed as part of this commit
                    info.BeginAddress = BeginAddress;
                    info.UntilAddress = commitTail = TailAddress;
                }

                Utility.MonotonicUpdate(ref commitCoveredAddress, commitTail, out _);

                commitPolicy.OnCommitCreated(info);
                // Enqueue the commit record's content and offset into the queue so it can be picked up by the next flush
                // At this point, we expect the commit record to be flushed out as a distinct recovery point
                ongoingCommitRequests.Enqueue((commitTail, info));
            }


            // As an optimization, if a concurrent flush has already advanced FlushedUntilAddress
            // past this commit, we can manually trigger a commit callback for safety, and return.
            if (commitTail <= FlushedUntilAddress)
            {
                CommitMetadataOnly(ref info);
                return true;
            }

            // Otherwise, move to set read-only tail and flush 
            bool isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                if (!allocator.ShiftReadOnlyToTail(out _, out _))
                    CommitMetadataOnly(ref info);
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int GetLength(byte* ptr)
        {
            if (logChecksum == LogChecksumType.None)
                return *(int*)ptr;
            else if (logChecksum == LogChecksumType.PerEntry)
                return *(int*)(ptr + 8);
            return 0;
        }

        /// <summary>
        /// Get length of entry from pointer to header
        /// </summary>
        /// <param name="headerPtr"></param>
        /// <returns></returns>
        public unsafe int UnsafeGetLength(byte* headerPtr)
            => GetLength(headerPtr);

        /// <summary>
        /// Get aligned version of record length
        /// </summary>
        /// <param name="length"></param>
        /// <returns></returns>
        public static int UnsafeAlign(int length)
            => Align(length);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool VerifyChecksum(byte* ptr, int length)
        {
            if (logChecksum == LogChecksumType.PerEntry)
            {
                var cs = Utility.XorBytes(ptr + 8, length + 4);
                if (cs != *(ulong*)ptr)
                {
                    return false;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ulong GetChecksum(byte* ptr)
        {
            if (logChecksum == LogChecksumType.PerEntry)
            {
                return *(ulong*)ptr;
            }
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void SetHeader(int length, byte* dest)
        {
            if (logChecksum == LogChecksumType.None)
            {
                *(int*)dest = length;
                return;
            }
            else if (logChecksum == LogChecksumType.PerEntry)
            {
                *(int*)(dest + 8) = length;
                *(ulong*)dest = Utility.XorBytes(dest + 8, length + 4);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void SetCommitRecordHeader(int length, byte* dest)
        {
            // commit record has negative length field to differentiate from normal records
            if (logChecksum == LogChecksumType.None)
            {
                *(int*)dest = -length;
                return;
            }
            else if (logChecksum == LogChecksumType.PerEntry)
            {
                *(int*)(dest + 8) = -length;
                *(ulong*)dest = Utility.XorBytes(dest + 8, length + 4);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateAllocatedLength(int numSlots)
        {
            if (numSlots > allocator.PageSize)
                throw new TsavoriteException("Entry does not fit on page");
        }
    }
}