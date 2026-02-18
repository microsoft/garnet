// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.LogAddress;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite Key/Value store class
    /// </summary>
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase, IDisposable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal readonly TAllocator hlog;
        internal readonly AllocatorBase<TStoreFunctions, TAllocator> hlogBase;
        internal readonly TAllocator readcache;
        internal readonly AllocatorBase<TStoreFunctions, TAllocator> readcacheBase;

        internal readonly TStoreFunctions storeFunctions;

        internal readonly bool UseReadCache;
        private readonly ReadCopyOptions ReadCopyOptions;
        internal readonly int sectorSize;
        internal readonly StateMachineDriver stateMachineDriver;

        /// <summary>
        /// ObjectIdMap to be used by operations that map it transiently, such as RENAME
        /// </summary>
        public ObjectIdMap TransientObjectIdMap => hlogBase.transientObjectIdMap;

        /// <summary>
        /// Number of active entries in hash index (does not correspond to total records, due to hash collisions)
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// High-water mark of the number of memory pages that were allocated in the circular buffer
        /// </summary>
        public long HighWaterAllocatedPageCount => hlogBase.HighWaterAllocatedPageCount;

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
        public LogAccessor<TStoreFunctions, TAllocator> Log { get; }

        /// <summary>
        /// Readonly accessor for StoreFunctions
        /// </summary>
        public TStoreFunctions StoreFunctions => storeFunctions;

        /// <summary>
        /// Read cache used by this Tsavorite instance
        /// </summary>
        public LogAccessor<TStoreFunctions, TAllocator> ReadCache { get; }

        int maxSessionID;

        internal readonly OverflowBucketLockTable<TStoreFunctions, TAllocator> LockTable;

        internal readonly int ThrottleCheckpointFlushDelayMs = -1;

        internal RevivificationManager<TStoreFunctions, TAllocator> RevivificationManager;

        internal Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory;

        /// <summary>
        /// Pause Revivification
        /// </summary>
        public void PauseRevivification()
            => RevivificationManager.PauseRevivification();

        /// <summary>
        /// Resume Revivification
        /// </summary>
        public void ResumeRevivification()
            => RevivificationManager.ResumeRevivification();

        /// <summary>
        /// Create TsavoriteKV instance
        /// </summary>
        /// <param name="kvSettings">Config settings</param>
        /// <param name="storeFunctions">Store-level user function implementations</param>
        /// <param name="allocatorFactory">Func to call to create the allocator(s, if doing readcache)</param>
        public TsavoriteKV(KVSettings kvSettings, TStoreFunctions storeFunctions, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory)
            : base(kvSettings.Epoch, kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger("TsavoriteKV Index Overflow buckets"))
        {
            this.allocatorFactory = allocatorFactory;
            loggerFactory = kvSettings.loggerFactory;
            logger = kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger("TsavoriteKV");

            this.storeFunctions = storeFunctions;

            var checkpointSettings = kvSettings.GetCheckpointSettings() ?? new CheckpointSettings();

            ThrottleCheckpointFlushDelayMs = checkpointSettings.ThrottleCheckpointFlushDelayMs;

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                logger?.LogInformation("CheckpointManager and CheckpointDir specified, ignoring CheckpointDir");

            checkpointManager = checkpointSettings.CheckpointManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactoryCreator(),
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
                    MemorySize = logSettings.ReadCacheSettings.MemorySize,
                    PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                    SegmentSizeBits = logSettings.ReadCacheSettings.PageSizeBits + 1,   // Not used by readcache but make sure it passes validation
                    MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                };
                allocatorSettings.logger = kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger($"{typeof(TAllocator).Name} ReadCache");
                allocatorSettings.evictCallback = ReadCacheEvict;
                readcache = allocatorFactory(allocatorSettings, storeFunctions);
                readcacheBase = readcache.GetBase<TAllocator>();
                readcacheBase.Initialize();
                ReadCache = new(this, readcache);
            }

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(kvSettings.GetIndexSizeCacheLines(), sectorSize);

            LockTable = new OverflowBucketLockTable<TStoreFunctions, TAllocator>(this);
            RevivificationManager = new(this, kvSettings.RevivificationSettings, logSettings);

            stateMachineDriver = kvSettings.StateMachineDriver ?? new(epoch, kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger($"StateMachineDriver"));

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
        public long GetKeyHash(ReadOnlySpan<byte> key) => storeFunctions.GetKeyHashCode64(key);

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="streamingSnapshotIteratorFunctions">Iterator for streaming snapshot records</param>
        /// <param name="cancellationToken">Caller's cancellation token</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TryInitiateFullCheckpoint(out Guid token, CheckpointType checkpointType, IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions = null, CancellationToken cancellationToken = default)
        {
            IStateMachine stateMachine;

            if (checkpointType == CheckpointType.StreamingSnapshot)
            {
                if (streamingSnapshotIteratorFunctions is null)
                    throw new TsavoriteException("StreamingSnapshot checkpoint requires a streaming snapshot iterator");
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                stateMachine = Checkpoint.Streaming(this, out token);
            }
            else
            {
                stateMachine = Checkpoint.Full(this, checkpointType, out token);
            }
            return stateMachineDriver.Register(stateMachine, cancellationToken);
        }

        /// <summary>
        /// Take full (index + log) checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="streamingSnapshotIteratorFunctions">Iterator for streaming snapshot records</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType,
            CancellationToken cancellationToken = default, IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions = null)
        {
            var success = TryInitiateFullCheckpoint(out Guid token, checkpointType, streamingSnapshotIteratorFunctions, cancellationToken);

            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);

            return (success, token);
        }

        /// <summary>
        /// Initiate index-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="cancellationToken">Caller's cancellation token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TryInitiateIndexCheckpoint(out Guid token, CancellationToken cancellationToken = default)
        {
            var stateMachine = Checkpoint.IndexOnly(this, out token);
            return stateMachineDriver.Register(stateMachine, cancellationToken);
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
            var success = TryInitiateIndexCheckpoint(out Guid token, cancellationToken);

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
        /// <param name="streamingSnapshotIteratorFunctions">Iterator functions</param>
        /// <param name="cancellationToken">Caller's cancellation token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TryInitiateHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false,
            IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions = null, CancellationToken cancellationToken = default)
        {
            IStateMachine stateMachine;

            if (checkpointType == CheckpointType.StreamingSnapshot)
            {
                if (streamingSnapshotIteratorFunctions is null)
                    throw new TsavoriteException("StreamingSnapshot checkpoint requires a streaming snapshot iterator");
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                stateMachine = Checkpoint.Streaming(this, out token);
            }
            else
            {
                token = _lastSnapshotCheckpoint.info.guid;
                stateMachine = tryIncremental && InternalCanTakeIncrementalCheckpoint(checkpointType, ref token)
                    ? Checkpoint.IncrementalHybridLogOnly(this, token)
                    : stateMachine = Checkpoint.HybridLogOnly(this, checkpointType, out token);
            }
            return stateMachineDriver.Register(stateMachine, cancellationToken);
        }

        /// <summary>
        /// Whether we can take an incremental snapshot checkpoint given current state of the store
        /// </summary>
        public bool CanTakeIncrementalCheckpoint(CheckpointType checkpointType, out Guid token)
        {
            token = _lastSnapshotCheckpoint.info.guid;
            return InternalCanTakeIncrementalCheckpoint(checkpointType, ref token);
        }

        /// <summary>
        /// Whether we can take an incremental snapshot checkpoint given current state of the store
        /// </summary>
        private bool InternalCanTakeIncrementalCheckpoint(CheckpointType checkpointType, ref Guid token)
        {
            return checkpointType == CheckpointType.Snapshot
                    && token != default
                    && _lastSnapshotCheckpoint.info.finalLogicalAddress > hlogBase.FlushedUntilAddress
                    && !hlog.HasObjectLog;
        }

        /// <summary>
        /// Take log-only checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType,
            bool tryIncremental = false, CancellationToken cancellationToken = default)
        {
            var success = TryInitiateHybridLogCheckpoint(out Guid token, checkpointType, tryIncremental, cancellationToken: cancellationToken);

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
            // Do not recover
            if (recoverTo == 0)
                return 0;
            FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
            return InternalRecover(recoveredIcInfo, recoveredHlcInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        /// <summary>
        /// Get the version we would recover to if we were to request recovery the specified version
        /// </summary>
        /// <param name="recoverTo">Specified version</param>
        /// <returns></returns>
        public long GetRecoverVersion(long recoverTo = -1)
        {
            try
            {
                FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
                return recoveredHlcInfo.info.version;
            }
            catch
            {
                // Do not recover
                return 0;
            }
        }

        /// <summary>
        /// Asynchronously recover from the latest valid checkpoint (blocking operation)
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory (beyond what needs to be read for recovery)</param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="recoverTo">Specific version requested to recover to, or -1 for latest version. Tsavorite will recover to the largest version number checkpointed that's smaller than the required version.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        public ValueTask<long> RecoverAsync(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1,
            CancellationToken cancellationToken = default)
        {
            // Do not recover
            if (recoverTo == 0)
                return ValueTask.FromResult(0L);
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
            => InternalRecover(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoNextVersion, -1);

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
            try
            {
                await stateMachineDriver.CompleteAsync(token);
            }
            catch
            {
                _indexCheckpoint.Reset();
                _hybridLogCheckpoint.Dispose();
                throw;
            }
            return;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions);
            OperationStatus internalStatus;
            var keyHash = storeFunctions.GetKeyHashCode64(key);

            do
                internalStatus = InternalRead(key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit] // Span<long> in here can be sizeable, so 0-init'ing isn't free
        internal unsafe void ContextReadWithPrefetch<TBatch, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TBatch batch, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TBatch : IReadArgBatch<TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            if (batch.Count == 1)
            {
                // Not actually a batch, no point prefetching

                batch.GetKey(0, out var key);
                batch.GetInput(0, out var input);
                batch.GetOutput(0, out var output);

                var hash = storeFunctions.GetKeyHashCode64(key);

                var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions);
                OperationStatus internalStatus;

                do
                    internalStatus = InternalRead(key, hash, ref input, ref output, context, ref pcontext, sessionFunctions);
                while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

                batch.SetStatus(0, HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus));
                batch.SetOutput(0, output);
            }
            else
            {
                // Prefetch if we can

                if (Sse.IsSupported)
                {
                    const int PrefetchSize = 12;

                    var hashes = stackalloc long[PrefetchSize];

                    // Prefetch the hash table entries for all keys
                    var tableAligned = state[resizeInfo.version].tableAligned;
                    var sizeMask = state[resizeInfo.version].size_mask;

                    var batchCount = batch.Count;

                    var nextBatchIx = 0;
                    while (nextBatchIx < batchCount)
                    {
                        // First level prefetch
                        var hashIx = 0;
                        for (; hashIx < PrefetchSize && nextBatchIx < batchCount; hashIx++)
                        {
                            batch.GetKey(nextBatchIx, out var key);
                            var hash = hashes[hashIx] = storeFunctions.GetKeyHashCode64(key);

                            Sse.Prefetch0(tableAligned + (hash & sizeMask));

                            nextBatchIx++;
                        }

                        // Second level prefetch
                        for (var i = 0; i < hashIx; i++)
                        {
                            var keyHash = hashes[i];
                            var hei = new HashEntryInfo(keyHash);

                            // If the hash entry exists in the table, points to main memory in the main log (not read cache), also prefetch the record header address
                            if (FindTag(ref hei) && !hei.IsReadCache && hei.Address >= hlogBase.HeadAddress)
                            {
                                Sse.Prefetch0((void*)hlogBase.GetPhysicalAddress(hei.Address));
                            }
                        }

                        nextBatchIx -= hashIx;

                        // Perform the reads
                        for (var i = 0; i < hashIx; i++)
                        {
                            batch.GetKey(nextBatchIx, out var key);
                            batch.GetInput(nextBatchIx, out var input);
                            batch.GetOutput(nextBatchIx, out var output);

                            var hash = hashes[i];

                            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions);
                            OperationStatus internalStatus;

                            do
                                internalStatus = InternalRead(key, hash, ref input, ref output, context, ref pcontext, sessionFunctions);
                            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

                            batch.SetStatus(nextBatchIx, HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus));
                            batch.SetOutput(nextBatchIx, output);

                            nextBatchIx++;
                        }
                    }
                }
                else
                {
                    // Perform the reads
                    for (var i = 0; i < batch.Count; i++)
                    {
                        batch.GetKey(i, out var key);
                        batch.GetInput(i, out var input);
                        batch.GetOutput(i, out var output);

                        var hash = storeFunctions.GetKeyHashCode64(key);

                        var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions);
                        OperationStatus internalStatus;

                        do
                            internalStatus = InternalRead(key, hash, ref input, ref output, context, ref pcontext, sessionFunctions);
                        while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

                        batch.SetStatus(i, HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus));
                        batch.SetOutput(i, output);
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context,
                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(key);

            do
                internalStatus = InternalRead(key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions);
            pcontext.SetIsNoKey();
            return ContextReadAtAddress(address, key: default, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.Ctx.ReadCopyOptions, ref readOptions);
            return ContextReadAtAddress(address, key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, ref PendingContext<TInput, TOutput, TContext> pcontext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStatus internalStatus;
            do
                internalStatus = InternalReadAtAddress(address, key, ref input, ref output, ref readOptions, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TInput input,
                ReadOnlySpan<byte> srcStringValue, ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;
            DiskLogRecord emptyLogRecord = default;

            do
                internalStatus = InternalUpsert<SpanUpsertValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, DiskLogRecord>(
                        key, keyHash, ref input, srcStringValue, srcObjectValue: null, in emptyLogRecord, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TInput input,
                IHeapObject srcObjectValue, ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;
            DiskLogRecord emptyLogRecord = default;

            do
                internalStatus = InternalUpsert<ObjectUpsertValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, DiskLogRecord>(
                        key, keyHash, ref input, srcStringValue: default, srcObjectValue, in emptyLogRecord, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ReadOnlySpan<byte> key, long keyHash, ref TInput input,
                in TSourceLogRecord inputLogRecord, ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert<LogRecordUpsertValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                        key, keyHash, ref input, srcStringValue: default, srcObjectValue: default, in inputLogRecord, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata,
                                                                          TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRMW(key, keyHash, ref input, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            recordMetadata = new(pcontext.logicalAddress, pcontext.eTag);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalDelete(key, keyHash, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus);
        }

        /// <summary>
        /// Grow the hash index by a factor of two. Caller should take a full checkpoint after growth, for persistence.
        /// </summary>
        /// <returns>Whether the grow completed successfully</returns>
        public async Task<bool> GrowIndexAsync()
        {
            if (epoch.ThisInstanceProtected())
                throw new TsavoriteException("Cannot use GrowIndex when using non-async sessions");

            var indexResizeTask = new IndexResizeSMTask<TStoreFunctions, TAllocator>(this);
            var indexResizeSM = new IndexResizeSM(indexResizeTask);
            return await stateMachineDriver.RunAsync(indexResizeSM);
        }

        /// <summary>
        /// Dispose Tsavorite instance
        /// </summary>
        public void Dispose()
        {
            Free();
            hlogBase.Dispose();
            readcacheBase?.Dispose();
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

            for (long bucket = 0; bucket < table_size_; bucket++)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; bucket_entry++)
                        if (b.bucket_entries[bucket_entry] >= beginAddress)
                            ++total_entry_count;
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & kAddressBitMask) == 0) break;
                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & kAddressBitMask);
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

            Dictionary<int, long> ofb_chaining_histogram = new();
            long total_entries_in_ofb = 0;
            long total_zeroed_out_slots = 0;
            long total_entries_below_begin_address = 0;
            long total_entries_in_ofb_below_begin_address = 0;
            long total_entries_with_tentative_bit_set = 0;
            Dictionary<int, long> slots_unused_by_nonofb_buckets_histogram = new();
            Dictionary<int, long> slots_unused_by_ofb_buckets_histogram = new();

            for (long bucket = 0; bucket < table_size_; bucket++)
            {
                bool is_bucket_in_ofb_table = false;
                List<int> tags = new();
                int total_valid_records_in_this_bucket_cnt = 0;
                int total_valid_entries_in_ofb_in_this_bucket_cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    // per bucket calculate the number of zero'd out slots
                    int zeroed_out_slots = 0;
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; bucket_entry++)
                    {
                        var x = default(HashBucketEntry);
                        x.word = b.bucket_entries[bucket_entry];

                        if (x.Tentative)
                            ++total_entries_with_tentative_bit_set;

                        if (((!x.IsReadCache) && (x.Address >= beginAddress)) || (x.IsReadCache && (x.Address >= readcacheBase.HeadAddress)))
                        {
                            if (tags.Contains(x.Tag) && !x.Tentative)
                                throw new TsavoriteException("Duplicate tag found in index");
                            tags.Add(x.Tag);
                            ++total_valid_records_in_this_bucket_cnt;
                            ++total_record_count;
                        }
                        else if (x.word != default)
                        {
                            if (is_bucket_in_ofb_table)
                                ++total_entries_in_ofb_below_begin_address;

                            ++total_entries_below_begin_address;
                        }
                        else
                        {
                            ++zeroed_out_slots;
                        }

                        if (is_bucket_in_ofb_table)
                            total_valid_entries_in_ofb_in_this_bucket_cnt++;
                    }

                    total_zeroed_out_slots += zeroed_out_slots;

                    if (is_bucket_in_ofb_table)
                    {
                        if (!slots_unused_by_ofb_buckets_histogram.ContainsKey(zeroed_out_slots))
                            slots_unused_by_ofb_buckets_histogram[zeroed_out_slots] = 0;
                        slots_unused_by_ofb_buckets_histogram[zeroed_out_slots]++;
                    }
                    else
                    {
                        if (!slots_unused_by_nonofb_buckets_histogram.ContainsKey(zeroed_out_slots))
                            slots_unused_by_nonofb_buckets_histogram[zeroed_out_slots] = 0;
                        slots_unused_by_nonofb_buckets_histogram[zeroed_out_slots]++;
                    }

                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & kAddressBitMask) == 0)
                        break;

                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & kAddressBitMask);
                    is_bucket_in_ofb_table = true;
                }

                if (!histogram.ContainsKey(total_valid_records_in_this_bucket_cnt))
                    histogram[total_valid_records_in_this_bucket_cnt] = 1;
                else
                    histogram[total_valid_records_in_this_bucket_cnt]++;

                total_valid_entries_in_ofb_in_this_bucket_cnt /= (int)Constants.kOverflowBucketIndex;
                if (!ofb_chaining_histogram.ContainsKey(total_valid_entries_in_ofb_in_this_bucket_cnt)) ofb_chaining_histogram[total_valid_entries_in_ofb_in_this_bucket_cnt] = 0;
                ofb_chaining_histogram[total_valid_entries_in_ofb_in_this_bucket_cnt]++;

                total_entries_in_ofb += total_valid_entries_in_ofb_in_this_bucket_cnt;
            }

            var distribution =
                $"Number of hash buckets: {table_size_}\r\n" +
                $"Number of overflow buckets: {OverflowBucketCount}\r\n" +
                $"Size of each bucket: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry)} bytes\r\n" +
                $"Hash-table size: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry) * table_size_} bytes\r\n" +
                $"Total distinct hash-table entry count: {{{total_record_count}}}\r\n" +
                $"Average #entries per hash bucket: {{{total_record_count / (double)table_size_:0.00}}}\r\n" +
                $"Total zeroed out slots: {total_zeroed_out_slots} \r\n" +
                $"Total entries below begin addr: {total_entries_below_begin_address} \r\n" +
                $"Total entries in overflow buckets: {total_entries_in_ofb} \r\n" +
                $"Total entries in overflow buckets below begin addr: {total_entries_in_ofb_below_begin_address} \r\n" +
                $"Total entries with tentative bit set: {total_entries_with_tentative_bit_set} \r\n" +
                $"Histogram of #entries per bucket:\r\n";

            foreach (var kvp in histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\r\n";
            }

            distribution += $"Histogram of #buckets per OFB chain and their frequencies: \r\n";
            foreach (var kvp in ofb_chaining_histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\r\n";
            }

            // Histogram of slots unused per bucket in OFB and non-OFB
            distribution += $"Histogram of #unused slots per bucket in main hash index:\r\n";
            foreach (var kvp in slots_unused_by_nonofb_buckets_histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\r\n";
            }

            distribution += $"Histogram of #unused slots per bucket in overflow buckets:\r\n";
            foreach (var kvp in slots_unused_by_ofb_buckets_histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\r\n";
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