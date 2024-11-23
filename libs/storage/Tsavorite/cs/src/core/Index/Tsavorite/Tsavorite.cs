// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite Key/Value store class
    /// </summary>
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase, IDisposable
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal readonly TAllocator hlog;
        internal readonly AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlogBase;
        internal readonly TAllocator readcache;
        internal readonly AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> readCacheBase;

        internal readonly TStoreFunctions storeFunctions;

        internal readonly bool UseReadCache;
        private readonly ReadCopyOptions ReadCopyOptions;
        internal readonly int sectorSize;

        /// <summary>
        /// Number of active entries in hash index (does not correspond to total records, due to hash collisions)
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Maximum number of memory pages ever allocated
        /// </summary>
        public long MaxAllocatedPageCount => hlogBase.MaxAllocatedPageCount;

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => state[resizeInfo.version].size;

        /// <summary>
        /// Number of overflow buckets in use (64 bytes each)
        /// </summary>
        public long OverflowBucketCount => overflowBucketsAllocator.GetMaxValidAddress();

        /// <summary>Number of allocations performed</summary>
        public long OverflowBucketAllocations => overflowBucketsAllocator.NumAllocations;

        /// <summary>
        /// Hybrid log used by this Tsavorite instance
        /// </summary>
        public LogAccessor<TKey, TValue, TStoreFunctions, TAllocator> Log { get; }

        /// <summary>
        /// Read cache used by this Tsavorite instance
        /// </summary>
        public LogAccessor<TKey, TValue, TStoreFunctions, TAllocator> ReadCache { get; }

        int maxSessionID;

        internal readonly bool CheckpointVersionSwitchBarrier;  // version switch barrier
        internal readonly OverflowBucketLockTable<TKey, TValue, TStoreFunctions, TAllocator> LockTable;

        internal void IncrementNumLockingSessions()
        {
            _hybridLogCheckpoint.info.manualLockingActive = true;
            Interlocked.Increment(ref hlogBase.NumActiveLockingSessions);
        }
        internal void DecrementNumLockingSessions() => Interlocked.Decrement(ref hlogBase.NumActiveLockingSessions);

        internal readonly int ThrottleCheckpointFlushDelayMs = -1;

        internal RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator> RevivificationManager;

        internal Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory;

        /// <summary>
        /// Create TsavoriteKV instance
        /// </summary>
        /// <param name="kvSettings">Config settings</param>
        /// <param name="storeFunctions">Store-level user function implementations</param>
        /// <param name="allocatorFactory">Func to call to create the allocator(s, if doing readcache)</param>
        public TsavoriteKV(KVSettings<TKey, TValue> kvSettings, TStoreFunctions storeFunctions, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory)
            : base(kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger("TsavoriteKV Index Overflow buckets"))
        {
            this.allocatorFactory = allocatorFactory;
            loggerFactory = kvSettings.loggerFactory;
            logger = kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger("TsavoriteKV");

            this.storeFunctions = storeFunctions;

            var checkpointSettings = kvSettings.GetCheckpointSettings() ?? new CheckpointSettings();

            CheckpointVersionSwitchBarrier = checkpointSettings.CheckpointVersionSwitchBarrier;
            ThrottleCheckpointFlushDelayMs = checkpointSettings.ThrottleCheckpointFlushDelayMs;

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                logger?.LogInformation("CheckpointManager and CheckpointDir specified, ignoring CheckpointDir");

            checkpointManager = checkpointSettings.CheckpointManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        new DirectoryInfo(checkpointSettings.CheckpointDir ?? ".").FullName), removeOutdated: checkpointSettings.RemoveOutdated);

            if (checkpointSettings.CheckpointManager is null)
                disposeCheckpointManager = true;

            var logSettings = kvSettings.GetLogSettings();

            UseReadCache = kvSettings.ReadCacheEnabled;

            ReadCopyOptions = logSettings.ReadCopyOptions;
            if (ReadCopyOptions.CopyTo == ReadCopyTo.Inherit)
                ReadCopyOptions.CopyTo = UseReadCache ? ReadCopyTo.ReadCache : ReadCopyTo.None;
            else if (ReadCopyOptions.CopyTo == ReadCopyTo.ReadCache && !UseReadCache)
                ReadCopyOptions.CopyTo = ReadCopyTo.None;

            if (ReadCopyOptions.CopyFrom == ReadCopyFrom.Inherit)
                ReadCopyOptions.CopyFrom = ReadCopyFrom.Device;

            bool isFixedLenReviv = hlog.IsFixedLength;

            // Create the allocator
            var allocatorSettings = new AllocatorSettings(logSettings, epoch, kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger(typeof(TAllocator).Name));
            hlog = allocatorFactory(allocatorSettings, storeFunctions);
            hlogBase = hlog.GetBase<TAllocator>();
            hlogBase.Initialize();
            Log = new(this, hlog);

            if (UseReadCache)
            {
                allocatorSettings.LogSettings = new()
                {
                    LogDevice = new NullDevice(),
                    ObjectLogDevice = hlog.HasObjectLog ? new NullDevice() : null,
                    PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                    MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                    SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                    MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                };
                allocatorSettings.logger = kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger($"{typeof(TAllocator).Name} ReadCache");
                allocatorSettings.evictCallback = ReadCacheEvict;
                readcache = allocatorFactory(allocatorSettings, storeFunctions);
                readCacheBase = readcache.GetBase<TAllocator>();
                readCacheBase.Initialize();
                ReadCache = new(this, readcache);
            }

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(kvSettings.GetIndexSizeCacheLines(), sectorSize);

            LockTable = new OverflowBucketLockTable<TKey, TValue, TStoreFunctions, TAllocator>(this);
            RevivificationManager = new(this, isFixedLenReviv, kvSettings.RevivificationSettings, logSettings);

            systemState = SystemState.Make(Phase.REST, 1);

            if (kvSettings.TryRecoverLatest)
            {
                try
                {
                    Recover();
                }
                catch { }
            }
        }

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(TKey key) => storeFunctions.GetKeyHashCode64(ref key);

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(ref TKey key) => storeFunctions.GetKeyHashCode64(ref key);

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TryInitiateFullCheckpoint(out Guid token, CheckpointType checkpointType, long targetVersion = -1, IScanIteratorFunctions<TKey, TValue> streamingSnapshotScanIteratorFunctions = null)
        {
            token = default;
            bool result;
            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>();
                result = StartStateMachine(new FullCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(backend, targetVersion));
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>();
                result = StartStateMachine(new FullCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(backend, targetVersion));
            }
            else if (checkpointType == CheckpointType.StreamingSnapshot)
            {
                this.streamingSnapshotScanIteratorFunctions = streamingSnapshotScanIteratorFunctions;
                result = StartStateMachine(new StreamingSnapshotCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(targetVersion));
            }
            else
                throw new TsavoriteException("Unsupported full checkpoint type");

            if (result)
                token = _hybridLogCheckpointToken;
            return result;
        }

        /// <summary>
        /// Take full (index + log) checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType,
            CancellationToken cancellationToken = default, long targetVersion = -1)
        {
            var success = TryInitiateFullCheckpoint(out Guid token, checkpointType, targetVersion);

            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);

            return (success, token);
        }

        /// <summary>
        /// Initiate index-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TryInitiateIndexCheckpoint(out Guid token)
        {
            var result = StartStateMachine(new IndexSnapshotStateMachine<TKey, TValue, TStoreFunctions, TAllocator>());
            token = _indexCheckpointToken;
            return result;
        }

        /// <summary>
        /// Take index-only checkpoint
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var success = TryInitiateIndexCheckpoint(out Guid token);

            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);

            return (success, token);
        }

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TryInitiateHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false,
            long targetVersion = -1)
        {
            token = default;
            bool result;
            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>();
                result = StartStateMachine(new HybridLogCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(backend, targetVersion));
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                ISynchronizationTask<TKey, TValue, TStoreFunctions, TAllocator> backend;
                if (tryIncremental && _lastSnapshotCheckpoint.info.guid != default && _lastSnapshotCheckpoint.info.finalLogicalAddress > hlogBase.FlushedUntilAddress && !hlog.HasObjectLog)
                    backend = new IncrementalSnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>();
                else
                    backend = new SnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>();
                result = StartStateMachine(new HybridLogCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(backend, targetVersion));
            }
            else if (checkpointType == CheckpointType.StreamingSnapshot)
            {
                result = StartStateMachine(new StreamingSnapshotCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator>(targetVersion));
            }
            else
                throw new TsavoriteException("Unsupported hybrid log checkpoint type");

            if (result)
                token = _hybridLogCheckpointToken;
            return result;
        }

        /// <summary>
        /// Take log-only checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType,
            bool tryIncremental = false, CancellationToken cancellationToken = default, long targetVersion = -1)
        {
            var success = TryInitiateHybridLogCheckpoint(out Guid token, checkpointType, tryIncremental, targetVersion);

            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);

            return (success, token);
        }

        /// <summary>
        /// Recover from the latest valid checkpoint (blocking operation)
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory (beyond what needs to be read for recovery)</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="recoverTo"> specific version requested or -1 for latest version. Tsavorite will recover to the largest version number checkpointed that's smaller than the required version. </param>
        /// <returns>Version we actually recovered to</returns>
        public long Recover(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1)
        {
            FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
            return InternalRecover(recoveredIcInfo, recoveredHlcInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        /// <summary>
        /// Asynchronously recover from the latest valid checkpoint (blocking operation)
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory (beyond what needs to be read for recovery)</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="recoverTo"> specific version requested or -1 for latest version. Tsavorite will recover to the largest version number checkpointed that's smaller than the required version.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        public ValueTask<long> RecoverAsync(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1,
            CancellationToken cancellationToken = default)
        {
            FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
            return InternalRecoverAsync(recoveredIcInfo, recoveredHlcInfo, numPagesToPreload, undoNextVersion, recoverTo, cancellationToken);
        }

        /// <summary>
        /// Recover from specific token (blocking operation)
        /// </summary>
        /// <param name="fullCheckpointToken">Token</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <returns>Version we actually recovered to</returns>
        public long Recover(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true)
        {
            return InternalRecover(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoNextVersion, -1);
        }

        /// <summary>
        /// Asynchronously recover from specific token (blocking operation)
        /// </summary>
        /// <param name="fullCheckpointToken">Token</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        public ValueTask<long> RecoverAsync(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default)
            => InternalRecoverAsync(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoNextVersion, -1, cancellationToken);

        /// <summary>
        /// Recover from specific index and log token (blocking operation)
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <returns>Version we actually recovered to</returns>
        public long Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true)
            => InternalRecover(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoNextVersion, -1);

        /// <summary>
        /// Asynchronously recover from specific index and log token (blocking operation)
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        public ValueTask<long> RecoverAsync(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default)
            => InternalRecoverAsync(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoNextVersion, -1, cancellationToken);

        /// <summary>
        /// Wait for ongoing checkpoint to complete
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            if (epoch.ThisInstanceProtected())
                throw new TsavoriteException("Cannot use CompleteCheckpointAsync when using non-async sessions");

            token.ThrowIfCancellationRequested();

            while (true)
            {
                var systemState = this.systemState;
                if (systemState.Phase == Phase.REST || systemState.Phase == Phase.PREPARE_GROW ||
                    systemState.Phase == Phase.IN_PROGRESS_GROW)
                    return;

                List<ValueTask> valueTasks = new();

                try
                {
                    epoch.Resume();
                    ThreadStateMachineStep<Empty, Empty, Empty, NullSession>(null, NullSession.Instance, valueTasks, token);
                }
                catch (Exception)
                {
                    _indexCheckpoint.Reset();
                    _hybridLogCheckpoint.Dispose();
                    throw;
                }
                finally
                {
                    epoch.Suspend();
                }

                if (valueTasks.Count == 0)
                {
                    // Note: The state machine will not advance as long as there are active locking sessions.
                    continue; // we need to re-check loop, so we return only when we are at REST
                }

                foreach (var task in valueTasks)
                {
                    if (!task.IsCompleted)
                        await task.ConfigureAwait(false);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, ref TOutput output, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions);
            OperationStatus internalStatus;
            var keyHash = storeFunctions.GetKeyHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context,
                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions, noKey: true);
            TKey key = default;
            return ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions, noKey: false);
            return ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, ref PendingContext<TInput, TOutput, TContext> pcontext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStatus internalStatus;
            do
                internalStatus = InternalReadAtAddress(address, ref key, ref input, ref output, ref readOptions, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TValue value, ref TOutput output, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TValue value, ref TOutput output, out RecordMetadata recordMetadata,
                                                                            TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata,
                                                                          TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRMW(ref key, keyHash, ref input, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalDelete(ref key, keyHash, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
            return status;
        }

        /// <summary>
        /// Grow the hash index by a factor of two. Caller should take a full checkpoint after growth, for persistence.
        /// </summary>
        /// <returns>Whether the grow completed</returns>
        public bool GrowIndex()
        {
            if (epoch.ThisInstanceProtected())
                throw new TsavoriteException("Cannot use GrowIndex when using non-async sessions");

            if (!StartStateMachine(new IndexResizeStateMachine<TKey, TValue, TStoreFunctions, TAllocator>()))
                return false;

            epoch.Resume();

            try
            {
                while (true)
                {
                    var _systemState = SystemState.Copy(ref systemState);
                    if (_systemState.Phase == Phase.PREPARE_GROW)
                        ThreadStateMachineStep<Empty, Empty, Empty, NullSession>(null, NullSession.Instance, default);
                    else if (_systemState.Phase == Phase.IN_PROGRESS_GROW)
                        SplitBuckets(0);
                    else if (_systemState.Phase == Phase.REST)
                        break;
                    epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                }
            }
            finally
            {
                epoch.Suspend();
            }
            return true;
        }

        /// <summary>
        /// Dispose Tsavorite instance
        /// </summary>
        public void Dispose()
        {
            Free();
            hlogBase.Dispose();
            readCacheBase?.Dispose();
            LockTable.Dispose();
            _lastSnapshotCheckpoint.Dispose();
            if (disposeCheckpointManager)
                checkpointManager?.Dispose();
            RevivificationManager.Dispose();
        }

        /// <summary>
        /// Total number of valid entries in hash table
        /// </summary>
        /// <returns></returns>
        private unsafe long GetEntryCount()
        {
            var version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_entry_count = 0;
            long beginAddress = hlogBase.BeginAddress;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                        if (b.bucket_entries[bucket_entry] >= beginAddress)
                            ++total_entry_count;
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask) == 0) break;
                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask);
                }
            }
            return total_entry_count;
        }

        private unsafe string DumpDistributionInternal(int version)
        {
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_record_count = 0;
            long beginAddress = hlogBase.BeginAddress;
            Dictionary<int, long> histogram = new();

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                List<int> tags = new();
                int cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        var x = default(HashBucketEntry);
                        x.word = b.bucket_entries[bucket_entry];
                        if (((!x.ReadCache) && (x.Address >= beginAddress)) || (x.ReadCache && (x.AbsoluteAddress >= readCacheBase.HeadAddress)))
                        {
                            if (tags.Contains(x.Tag) && !x.Tentative)
                                throw new TsavoriteException("Duplicate tag found in index");
                            tags.Add(x.Tag);
                            ++cnt;
                            ++total_record_count;
                        }
                    }
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask) == 0) break;
                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask);
                }

                if (!histogram.ContainsKey(cnt)) histogram[cnt] = 0;
                histogram[cnt]++;
            }

            var distribution =
                $"Number of hash buckets: {table_size_}\n" +
                $"Number of overflow buckets: {OverflowBucketCount}\n" +
                $"Size of each bucket: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry)} bytes\n" +
                $"Total distinct hash-table entry count: {{{total_record_count}}}\n" +
                $"Average #entries per hash bucket: {{{total_record_count / (double)table_size_:0.00}}}\n" +
                $"Histogram of #entries per bucket:\n";

            foreach (var kvp in histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\n";
            }

            return distribution;
        }

        /// <summary>
        /// Dumps the distribution of each non-empty bucket in the hash table.
        /// </summary>
        public string DumpDistribution()
        {
            return DumpDistributionInternal(resizeInfo.version);
        }
    }
}