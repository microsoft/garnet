// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    public enum VectorManagerResult
    {
        Invalid = 0,

        OK,
        BadParams,
        Duplicate,
        MissingElement,
    }

    /// <summary>
    /// Methods for managing an implementation of various vector operations.
    /// </summary>
    public sealed partial class VectorManager : IDisposable
    {
        // MUST BE A POWER OF 2
        public const ulong ContextStep = 8;

        internal const int IndexSizeBytes = Index.Size;
        internal const long VADDAppendLogArg = long.MinValue;
        internal const long DeleteAfterDropArg = VADDAppendLogArg + 1;
        internal const long RecreateIndexArg = DeleteAfterDropArg + 1;
        internal const long VREMAppendLogArg = RecreateIndexArg + 1;
        internal const long MigrateElementKeyLogArg = VREMAppendLogArg + 1;
        internal const long MigrateIndexKeyLogArg = MigrateElementKeyLogArg + 1;

        /// <summary>
        /// Minimum size of an id is assumed to be at least 8 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 8;

        /// <summary>
        /// Maximum number of dimensions a vector can have.
        /// Matches Redis's VSET_MAX_VECTOR_DIM (65,536).
        /// </summary>
        internal const int MaxVectorDimensions = 1 << 16;

        /// <summary>
        /// Maximum number of results that can be requested in a single VSIM query.
        /// Practical limit to prevent integer overflow when computing buffer sizes
        /// (e.g. retrieveCount * MinimumSpacePerId) and to avoid excessive allocations
        /// from a single command (at 100M: ~400 MB for distances + ~1.2 GB for ids).
        /// </summary>
        internal const int MaxRetrieveCount = 100_000_000;

        /// <summary>
        /// Maximum exploration factor (EF) for build and search operations.
        /// Matches Redis's hardcoded limit of 1,000,000.
        /// </summary>
        internal const int MaxExplorationFactor = 1_000_000;

        /// <summary>
        /// Ensures the VSIM distance output buffer has at least <paramref name="retrieveCount"/> * sizeof(float) bytes.
        /// Rents from <see cref="MemoryPool{T}"/> if the current buffer is too small.
        /// </summary>
        private static void EnsureDistanceBufferSize(ref SpanByteAndMemory buffer, int retrieveCount)
        {
            // Verify no overflow: checked() ensures MaxRetrieveCount * sizeof(float) fits in int32
            Debug.Assert(retrieveCount <= MaxRetrieveCount && checked(MaxRetrieveCount * sizeof(float)) > 0);
            var sizeBytes = retrieveCount * sizeof(float);
            if (sizeBytes > buffer.Length)
            {
                if (!buffer.IsSpanByte)
                {
                    buffer.Memory.Dispose();
                }

                buffer = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(sizeBytes), sizeBytes);
            }

            buffer.Length = sizeBytes;
        }

        /// <summary>
        /// Ensures the VSIM id output buffer has at least <paramref name="retrieveCount"/> * <see cref="MinimumSpacePerId"/> bytes.
        /// Rents from <see cref="MemoryPool{T}"/> if the current buffer is too small.
        /// If we're still wrong, we'll end up using continuation callbacks which have more overhead.
        /// </summary>
        private static void EnsureIdBufferSize(ref SpanByteAndMemory buffer, int retrieveCount)
        {
            // Verify no overflow: checked() ensures MaxRetrieveCount * MinimumSpacePerId fits in int32
            Debug.Assert(retrieveCount <= MaxRetrieveCount && checked(MaxRetrieveCount * MinimumSpacePerId) > 0);
            var sizeBytes = retrieveCount * MinimumSpacePerId;
            if (sizeBytes > buffer.Length)
            {
                if (!buffer.IsSpanByte)
                {
                    buffer.Memory.Dispose();
                }

                buffer = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(sizeBytes), sizeBytes);
            }
        }

        /// <summary>
        /// Ensures the VSIM filter bitmap buffer has at least one bit per result
        /// (<paramref name="resultCount"/> bits, rounded up to whole bytes).
        /// Rents from <see cref="MemoryPool{T}"/> if the current buffer is too small.
        /// </summary>
        private static void EnsureFilterBitmapSize(ref SpanByteAndMemory buffer, int resultCount)
        {
            var sizeBytes = (resultCount + 7) >> 3;
            if (sizeBytes > buffer.Length)
            {
                if (!buffer.IsSpanByte)
                {
                    buffer.Memory.Dispose();
                }

                buffer = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(sizeBytes), sizeBytes);
            }

            buffer.Length = sizeBytes;
        }

        /// <summary>
        /// The process wide instances of DiskANN.
        /// 
        /// We only need the one, even if we have multiple DBs, because all context is provided by DiskANN instances and Garnet storage.
        /// </summary>
        private DiskANNService Service { get; } = new DiskANNService();

        /// <summary>
        /// Whether or not Vector Set preview is enabled.
        /// 
        /// TODO: This goes away once we're stable.
        /// </summary>
        public bool IsEnabled { get; }

        private bool initialized;

        /// <summary>
        /// Unique id for this <see cref="VectorManager"/>.
        /// 
        /// Is used to determine if an <see cref="Index"/> is backed by a DiskANN index that was created in this process.
        /// </summary>
        private readonly Guid processInstanceId = Guid.NewGuid();

        private readonly ILogger logger;

        private readonly int dbId;

        public VectorManager(int dbId, GarnetServerOptions serverOptions, Func<IMessageConsumer> getTempSession, ILoggerFactory loggerFactory)
        {
            this.dbId = dbId;

            IsEnabled = serverOptions.EnableVectorSetPreview;

            // Include DB and id so we correlate to what's actually stored in the log
            logger = loggerFactory?.CreateLogger($"{nameof(VectorManager)}:{dbId}:{processInstanceId}");

            replicationBlockEvent = CountingEventSlim.Create();
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });

            if (serverOptions.VectorSetReplayTaskCount < 0 || serverOptions.VectorSetReplayTaskCount > Environment.ProcessorCount)
                throw new GarnetException($"VectorSetReplayTaskCount should be in range [0,{Environment.ProcessorCount}]!");
            var vectorSetReplayCount = serverOptions.VectorSetReplayTaskCount == 0 ? Environment.ProcessorCount : serverOptions.VectorSetReplayTaskCount;
            replicationReplayTasks = new Task[vectorSetReplayCount];
            for (var i = 0; i < replicationReplayTasks.Length; i++)
            {
                replicationReplayTasks[i] = Task.CompletedTask;
            }

            vectorSetLocks = new(vectorSetReplayCount);

            this.getTempSession = getTempSession;
            cleanupTaskChannel = Channel.CreateUnbounded<object>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            cleanupTask = RunCleanupTaskAsync();

            quantizationChannel = Channel.CreateUnbounded<QuantizationState>(new() { SingleWriter = false, SingleReader = false, AllowSynchronousContinuations = false });

            if (serverOptions.VectorSetQuantizationTaskCount < 0 || serverOptions.VectorSetQuantizationTaskCount > Environment.ProcessorCount)
                throw new GarnetException($"VectorSetQuantizationTaskCount should be in range [0,{Environment.ProcessorCount}]!");
            var vectorSetQuantizationTaskCount = serverOptions.VectorSetQuantizationTaskCount == 0 ? Environment.ProcessorCount : serverOptions.VectorSetQuantizationTaskCount;
            quantizationTasks = new Task[vectorSetQuantizationTaskCount];
            Array.Fill(quantizationTasks, Task.CompletedTask);

            logger?.LogInformation("Created VectorManager");
        }

        /// <summary>
        /// Load state necessary for VectorManager from main store.
        /// </summary>
        public void Initialize()
        {
            if (!IsEnabled || initialized) return;

            initialized = true;

            using var session = (RespServerSession)getTempSession();
            if (session.activeDbId != dbId && !session.TrySwitchActiveDatabaseSession(dbId))
            {
                throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
            }

            Span<byte> keySpan = stackalloc byte[1];
            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

            var key = SpanByte.FromPinnedSpan(keySpan);

            key.MarkNamespace();
            key.SetNamespaceInPayload(0);

            var data = SpanByte.FromPinnedSpan(dataSpan);

            ref var ctx = ref session.storageSession.vectorContext;

            var status = ctx.Read(ref key, ref data);

            if (status.IsPending)
            {
                SpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }

            // Can be not found if we've never spun up a Vector Set
            if (status.Found)
            {
                lock (this)
                {
                    contextMetadata = MemoryMarshal.Cast<byte, ContextMetadata>(dataSpan)[0];
                }
            }

            StartQuantizationTasks();
        }

        /// <summary>
        /// Restart or update any pending work that was discovered as part of recovery.
        /// </summary>
        public void ResumePostRecovery()
        {
            if (!IsEnabled) return;

            using var session = (RespServerSession)getTempSession();

            ref var ctx = ref session.storageSession.vectorContext;

            // If we come up and contexts are marked for migration, that means the migration FAILED
            // and we'd like those contexts back ASAP
            lock (this)
            {
                var abandonedMigrations = contextMetadata.GetMigrating();

                if (abandonedMigrations != null)
                {
                    foreach (var abandoned in abandonedMigrations)
                    {
                        contextMetadata.MarkMigrationComplete(abandoned, ushort.MaxValue);
                        contextMetadata.MarkCleaningUp(abandoned);
                    }

                    UpdateContextMetadata(ref ctx);
                }
            }

            Span<byte> indexSpan = stackalloc byte[Index.Size];

            // Finish any deletes that were in progress before we restarted
            var failedDeletes = GetDeletesInProgress(session.storageSession);
            var clearInProgressDeletes = true;
            foreach (var (toDeleteKey, toDeleteCtx) in failedDeletes)
            {
                logger?.LogInformation("Cleaning up in progress Vector Set delete of {key} (context: {ctx})", Encoding.UTF8.GetString(toDeleteKey.Span), toDeleteCtx);

                unsafe
                {
                    fixed (byte* toDeleteKeyPtr = toDeleteKey.Span)
                    {
                        var toDeleteKeySpanByte = SpanByte.FromPinnedPointer(toDeleteKeyPtr, toDeleteKey.Span.Length);

                        RawStringInput input = new(RespCommand.VADD);

                        // Check if delete got far enough that we should re-apply it
                        using (ReadForDeleteVectorIndex(session.storageSession, ref toDeleteKeySpanByte, ref input, indexSpan, out var garnetStatus))
                        {
                            if (garnetStatus is not (GarnetStatus.BADSTATE or GarnetStatus.NOTFOUND))
                            {
                                // It didn't - so don't re-apply (But do remove the "we're deleting"-entry later)
                                continue;
                            }
                        }

                        try
                        {
                            if (TryDeleteVectorSet(session.storageSession, ref toDeleteKeySpanByte, out var garnetStatus).IsCompletedSuccessfully && garnetStatus != GarnetStatus.BADSTATE)
                            {
                                // Normal delete worked, easy enough
                                //
                                // This happens if we fail between the "remember we're deleting" and "zero everything out" steps
                                logger?.LogInformation("Vector Set under {key} (context: {ctx}) deleted normally", Encoding.UTF8.GetString(toDeleteKey.Span), toDeleteCtx);
                                continue;
                            }
                        }
                        catch (Exception ex)
                        {
                            logger?.LogError(ex, "Attempt at normal cleanup of {key} failed", Encoding.UTF8.GetString(toDeleteKey.Span));
                        }

                        // Partial delete, do these bits directly
                        //   1. Try to zero out the index key
                        //   2. Try to delete the index key
                        //   3. Try to drop the replication key
                        //   4. Mark the context as needing cleanup

                        // Zero out the index (which may already be zero'd, but that's fine to redo)
                        RawStringInput updateToDroppableVectorSet = new(RespCommand.VADD, arg1: DeleteAfterDropArg);
                        var update = session.storageSession.basicContext.RMW(ref toDeleteKeySpanByte, ref updateToDroppableVectorSet);
                        if (!update.IsCompletedSuccessfully)
                        {
                            throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                        }

                        // Note that we don't need to DROP the index because we know we haven't re-created it yet

                        // Actually delete the value
                        var del = session.storageSession.basicContext.Delete(ref toDeleteKeySpanByte);
                        if (!(del.Found || del.NotFound))
                        {
                            logger?.LogCritical("Failed to cleanup delete dropped Vector Set {key} (context: {ctx}), Vector Set will remain corrupted", Encoding.UTF8.GetString(toDeleteKey.Span), toDeleteCtx);
                            clearInProgressDeletes = false;
                            continue;
                        }

                        // Cleanup incidental additional state
                        if (!TryDropVectorSetReplicationKey(toDeleteKeySpanByte, ref session.storageSession.basicContext))
                        {
                            logger?.LogCritical("Failed to cleanup delete dropped Vector Set {key} (context: {ctx}), Vector Set will remain corrupted", Encoding.UTF8.GetString(toDeleteKey.Span), toDeleteCtx);
                            clearInProgressDeletes = false;
                            continue;
                        }

                        // Schedule cleanup of element data
                        CleanupDroppedIndex(ref session.storageSession.vectorContext, toDeleteCtx);

                        logger?.LogInformation("Vector Set under {key} (context: {ctx}) deleted normally", Encoding.UTF8.GetString(toDeleteKey.Span), toDeleteCtx);
                    }
                }
            }

            if (clearInProgressDeletes)
            {
                // We successfully dealt with all pending deletes, we can delete the metadata key
                Span<byte> toDeleteKeySpan = stackalloc byte[2];
                var toDeleteKey = SpanByte.FromPinnedSpan(toDeleteKeySpan);

                // 0:1 is InProgressDeletes
                toDeleteKey.MarkNamespace();
                toDeleteKey.SetNamespaceInPayload(0);
                toDeleteKey.AsSpan()[0] = 1;

                var deleteStatus = session.storageSession.vectorContext.Delete(ref toDeleteKey);
                Debug.Assert(!deleteStatus.IsPending, "Delete shouldn't go async");
            }

            // Resume any cleanups we didn't complete before recovery
            _ = cleanupTaskChannel.Writer.TryWrite(null);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // We must drain all these before disposing, otherwise we'll leave replicationBlockEvent unset
            _ = replicationReplayChannel.Writer.TryComplete();
            AsyncUtils.BlockingWait(replicationReplayChannel.Reader.Completion);
            AsyncUtils.BlockingWait(Task.WhenAll(replicationReplayTasks));

            replicationBlockEvent.Dispose();

            // Wait for any in progress cleanup to finish
            cleanupTaskChannel.Writer.Complete();
            AsyncUtils.BlockingWait(cleanupTaskChannel.Reader.Completion);
            AsyncUtils.BlockingWait(cleanupTask);

            // drain quantization task
            _ = quantizationChannel.Writer.TryComplete();
            while (quantizationChannel.Reader.TryRead(out _)) { }
            AsyncUtils.BlockingWait(Task.WhenAll(quantizationTasks));
        }

        private static void CompletePending<TContext>(ref Status status, ref SpanByte output, ref TContext ctx)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            _ = ctx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            Debug.Assert(!completedOutputs.Next());
            completedOutputs.Dispose();
        }

        /// <summary>
        /// Add a vector to a vector set encoded by <paramref name="indexValue"/>.
        /// 
        /// Assumes that the index is locked in the Tsavorite store.
        /// </summary>
        /// <returns>Result of the operation.</returns>
        internal VectorManagerResult TryAdd(
            scoped ref SpanByte key,
            scoped ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            ReadOnlySpan<byte> attributes,
            uint providedReduceDims,
            VectorQuantType providedQuantType,
            uint providedBuildExplorationFactor,
            uint providedNumLinks,
            VectorDistanceMetricType providedDistanceMetric,
            out ReadOnlySpan<byte> errorMsg
        )
        {
            AssertHaveStorageSession();

            errorMsg = default;

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out _, out var numLinks, out var distanceMetric, out var indexPtr, out _);

            if (providedReduceDims != 0 && providedReduceDims != reduceDims)
            {
                return VectorManagerResult.BadParams;
            }

            if (providedQuantType != VectorQuantType.Invalid && providedQuantType != quantType)
            {
                return VectorManagerResult.BadParams;
            }

            if (providedDistanceMetric != VectorDistanceMetricType.Invalid && providedDistanceMetric != distanceMetric)
            {
                errorMsg = Encoding.ASCII.GetBytes($"ERR Distance metric mismatch - got {providedDistanceMetric} but set has {distanceMetric}");
                return VectorManagerResult.BadParams;
            }

            if (providedNumLinks != numLinks)
            {
                // Matching Redis behavior
                errorMsg = "ERR asked M value mismatch with existing vector set"u8;
                return VectorManagerResult.BadParams;
            }

            bool insert;
            bool needsQuantization;
            using (var vectorData = PrepareVectorData(quantType, valueType, values, out errorMsg))
            {
                if (!errorMsg.IsEmpty)
                {
                    return VectorManagerResult.BadParams;
                }

                if (vectorData.ElementCount != dimensions)
                {
                    errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {vectorData.ElementCount} but set has {dimensions}");
                    return VectorManagerResult.BadParams;
                }

                if (providedReduceDims == 0 && reduceDims != 0)
                {
                    // Matching Redis behavior, which is definitely a bit weird here
                    errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {vectorData.ElementCount} but set has {reduceDims}");
                    return VectorManagerResult.BadParams;
                }

                insert =
                    Service.Insert(
                        context,
                        indexPtr,
                        element,
                        vectorData.ReadOnlySpan,
                        vectorData.ElementCount,
                        attributes,
                        out needsQuantization
                    );
            }

            if (insert)
            {
                if (needsQuantization)
                {
                    _ = quantizationChannel.Writer.TryWrite(new(key.ToByteArray(), QuantizationStep.BuildQuantizationTable, 0));
                }

                return VectorManagerResult.OK;
            }

            return VectorManagerResult.Duplicate;
        }

        /// <summary>
        /// Try to remove a vector (and associated attributes) from a Vector Set, as identified by element key.
        /// </summary>
        internal VectorManagerResult TryRemove(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            var del = Service.Remove(context, indexPtr, element);

            return del ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        internal Status TryDeleteVectorSet(StorageSession storageSession, ref SpanByte key, out GarnetStatus status)
        {
            storageSession.parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VADD, ref storageSession.parseState);

            Span<byte> indexSpan = stackalloc byte[Index.Size];

            using (ReadForDeleteVectorIndex(storageSession, ref key, ref input, indexSpan, out status))
            {
                if (status != GarnetStatus.OK)
                {
                    // This can happen is something else successfully deleted before we acquired the lock
                    return Status.CreateNotFound();
                }

                ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out _);

                if (!TryMarkDeleteInProgress(ref storageSession.vectorContext, ref key, context))
                {
                    // We can't recover from a crash or error, so fail the delete for safety
                    return Status.CreateError();
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

                // Update the index to be delete-able
                RawStringInput updateToDroppableVectorSet = new(RespCommand.VADD, arg1: DeleteAfterDropArg);

                var update = storageSession.basicContext.RMW(ref key, ref updateToDroppableVectorSet);
                if (!update.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                }

                // Drop the native side of the index now - we can't fault between the two unless the process is torn down
                DropIndex(indexSpan);

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

                // Actually delete the value
                var del = storageSession.basicContext.Delete(ref key);
                if (!del.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to delete dropped Vector Set, this should never happen but will leave vector sets corrupted");
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

                // Cleanup incidental additional state
                if (!TryDropVectorSetReplicationKey(key, ref storageSession.basicContext))
                {
                    logger?.LogCritical("Couldn't synthesize Vector Set delete operation for replication, data loss will occur");
                }

                // Schedule cleanup of element data
                CleanupDroppedIndex(ref storageSession.vectorContext, context);

                // Delete has finished, so remove the in progress metadata
                //
                // A crash or error before this will cause some work to be retried, but no correctness issues
                ClearDeleteInProgress(ref storageSession.vectorContext, ref key, context);

                return Status.CreateFound();
            }
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal unsafe VectorManagerResult ValueSimilarity(
            scoped ReadOnlySpan<byte> indexValue,
            VectorValueType valueType,
            scoped ReadOnlySpan<byte> values,
            int count,
            float delta,
            int searchExplorationFactor,
            scoped ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            scoped out ReadOnlySpan<byte> errorMsg,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            ref SpanByteAndMemory filterBitmap
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            var effectiveEF = Math.Max(searchExplorationFactor, count);

            EnsureDistanceBufferSize(ref outputDistances, count);
            EnsureIdBufferSize(ref outputIds, count);
            int found;
            nint continuation;
            using (var vectorData = PrepareVectorData(quantType, valueType, values, out var tempErrorMsg))
            {
                if (!tempErrorMsg.IsEmpty)
                {
                    // Have to copy for scoping reasons - it's an error path, so we'll just eat the perf hit for now
                    errorMsg = tempErrorMsg.ToArray();
                    outputIdFormat = VectorIdFormat.Invalid;
                    return VectorManagerResult.BadParams;
                }

                if (dimensions != vectorData.ElementCount)
                {
                    outputIdFormat = VectorIdFormat.Invalid;
                    errorMsg = default;
                    return VectorManagerResult.BadParams;
                }

                if (!filter.IsEmpty)
                {
                    // ── Inline filtered search path ─────────
                    // Compile the filter, set up callback state, and let Rust
                    // evaluate per-candidate via InlineFilterCandidateCallbackImpl.
                    // Only passing candidates are written to the output buffer,
                    // so we size it for the desired count, not the overfetch.

                    // Borrow scratch space for compiled filter program
                    var bufferSlice = ActiveThreadSession.scratchBufferBuilder.CreateArgSlice(
                        TotalPoolTokens * ExprToken.Size + MaxSelectors * 2 * sizeof(int));
                    var span = MemoryMarshal.Cast<byte, ExprToken>(bufferSlice.Span);
                    var selectorBuf = MemoryMarshal.Cast<byte, (int Start, int Length)>(
                        bufferSlice.Span.Slice(TotalPoolTokens * ExprToken.Size));

                    try
                    {
                        span.Clear();

                        var offset = 0;
                        var instrBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                        var tuplePoolBuf = span.Slice(offset, MaxTuplePool); offset += MaxTuplePool;
                        var tokensBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                        var opsStackBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                        var runtimePoolBuf = span.Slice(offset, MaxRuntimePool); offset += MaxRuntimePool;
                        var extractedFields = span.Slice(offset, MaxSelectors); offset += MaxSelectors;
                        var stackBuf = span.Slice(offset, StackCapacity);

                        var instrCount = ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
                        if (instrCount < 0)
                        {
                            // Compile failed — return zero results
                            outputDistances.Length = 0;
                            filterBitmap.Length = 0;
                            outputIdFormat = VectorIdFormat.I32LengthPrefixed;
                            errorMsg = Encoding.ASCII.GetBytes($"ERR compiling filter failed");
                            return VectorManagerResult.OK;
                        }

                        var selectorCount = GetSelectorRanges(instrBuf[..instrCount], instrCount, filter, selectorBuf);

                        // Pin filter bytes and scratch buffer pointers, then populate thread-static state
                        fixed (byte* filterPtr = filter)
                        fixed (ExprToken* instrPtr = instrBuf, tuplePtr = tuplePoolBuf, runtimePtr = runtimePoolBuf, fieldsPtr = extractedFields, stackPtr = stackBuf)
                        fixed ((int, int)* selPtr = selectorBuf)
                        {
                            t_inlineFilterState = new InlineFilterState
                            {
                                Context = context,
                                InstrCount = instrCount,
                                TupleCount = tupleCount,
                                SelectorCount = selectorCount,
                                InstrBufPtr = instrPtr,
                                TuplePoolBufPtr = tuplePtr,
                                RuntimePoolBufPtr = runtimePtr,
                                ExtractedFieldsPtr = fieldsPtr,
                                StackBufPtr = stackPtr,
                                SelectorRangesPtr = selPtr,
                                FilterBytesPtr = filterPtr,
                                FilterBytesLen = filter.Length,
                            };

                            found = Service.SearchVector(
                                context,
                                indexPtr,
                                vectorData.ReadOnlySpan,
                                vectorData.ElementCount,
                                delta,
                                effectiveEF,
                                filter,
                                maxFilteringEffort,
                                outputIds,
                                outputDistances,
                                out continuation
                            );
                        }
                    }
                    finally
                    {
                        ActiveThreadSession.scratchBufferBuilder.RewindScratchBuffer(ref bufferSlice);
                    }
                }
                else
                {
                    found =
                        Service.SearchVector(
                            context,
                            indexPtr,
                            vectorData.ReadOnlySpan,
                            vectorData.ElementCount,
                            delta,
                            effectiveEF,
                            filter,
                            0,
                            outputIds,
                            outputDistances,
                            out continuation
                        );
                }

                if (found < 0)
                {
                    logger?.LogWarning("Error indicating response from vector service {found}", found);
                    outputIdFormat = VectorIdFormat.Invalid;
                    errorMsg = Encoding.ASCII.GetBytes($"ERR Error indicating response from vector service {found}");
                    return VectorManagerResult.BadParams;
                }

                if (includeAttributes || !filter.IsEmpty)
                {
                    FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
                }

                // Apply post-filtering if filter is specified
                if (!filter.IsEmpty)
                {
                    EnsureFilterBitmapSize(ref filterBitmap, found);
                    _ = ApplyPostFilter(filter, found, outputAttributes.AsReadOnlySpan(), filterBitmap.AsSpan(), ActiveThreadSession.scratchBufferBuilder);
                }

                if (continuation != 0)
                {
                    // TODO: paged results!
                    throw new NotImplementedException();
                }

                outputDistances.Length = sizeof(float) * found;

                // Default assumption is length prefixed
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;

                errorMsg = default;

                return VectorManagerResult.OK;

            }
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal unsafe VectorManagerResult ElementSimilarity(
            ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            ref SpanByteAndMemory filterBitmap
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            var effectiveEF = Math.Max(searchExplorationFactor, count);

            EnsureDistanceBufferSize(ref outputDistances, count);
            EnsureIdBufferSize(ref outputIds, count);

            int found;
            nint continuation;

            if (!filter.IsEmpty)
            {
                // ── Inline-filtered search path ──────────────────────────
                // Size output buffers for desired result count
                EnsureDistanceBufferSize(ref outputDistances, count);
                EnsureIdBufferSize(ref outputIds, count);

                // Borrow scratch space for compiled filter program
                var bufferSlice = ActiveThreadSession.scratchBufferBuilder.CreateArgSlice(
                    TotalPoolTokens * ExprToken.Size + MaxSelectors * 2 * sizeof(int));
                var span = MemoryMarshal.Cast<byte, ExprToken>(bufferSlice.Span);
                var selectorBuf = MemoryMarshal.Cast<byte, (int Start, int Length)>(
                    bufferSlice.Span.Slice(TotalPoolTokens * ExprToken.Size));

                try
                {
                    span.Clear();

                    var offset = 0;
                    var instrBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                    var tuplePoolBuf = span.Slice(offset, MaxTuplePool); offset += MaxTuplePool;
                    var tokensBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                    var opsStackBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                    var runtimePoolBuf = span.Slice(offset, MaxRuntimePool); offset += MaxRuntimePool;
                    var extractedFields = span.Slice(offset, MaxSelectors); offset += MaxSelectors;
                    var stackBuf = span.Slice(offset, StackCapacity);

                    var instrCount = ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
                    if (instrCount < 0)
                    {
                        outputDistances.Length = 0;
                        filterBitmap.Length = 0;
                        outputIdFormat = VectorIdFormat.I32LengthPrefixed;
                        return VectorManagerResult.OK;
                    }

                    var selectorCount = GetSelectorRanges(instrBuf[..instrCount], instrCount, filter, selectorBuf);

                    fixed (byte* filterPtr = filter)
                    fixed (ExprToken* instrPtr = instrBuf, tuplePtr = tuplePoolBuf, runtimePtr = runtimePoolBuf, fieldsPtr = extractedFields, stackPtr = stackBuf)
                    fixed ((int, int)* selPtr = selectorBuf)
                    {
                        t_inlineFilterState = new InlineFilterState
                        {
                            Context = context,
                            InstrCount = instrCount,
                            TupleCount = tupleCount,
                            SelectorCount = selectorCount,
                            InstrBufPtr = instrPtr,
                            TuplePoolBufPtr = tuplePtr,
                            RuntimePoolBufPtr = runtimePtr,
                            ExtractedFieldsPtr = fieldsPtr,
                            StackBufPtr = stackPtr,
                            SelectorRangesPtr = selPtr,
                            FilterBytesPtr = filterPtr,
                            FilterBytesLen = filter.Length,
                        };

                        found = Service.SearchElement(
                            context,
                            indexPtr,
                            element,
                            delta,
                            effectiveEF,
                            filter,
                            maxFilteringEffort,
                            outputIds,
                            outputDistances,
                            out continuation
                        );

                    }
                }
                finally
                {
                    ActiveThreadSession.scratchBufferBuilder.RewindScratchBuffer(ref bufferSlice);
                }
            }
            else
            {
                found =
    Service.SearchElement(
        context,
        indexPtr,
        element,
        delta,
        effectiveEF,
        filter,
        0,
        outputIds,
        outputDistances,
        out continuation
    );
            }


            if (found < 0)
            {
                logger?.LogWarning("Error indicating response from vector service {found}", found);
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            if (includeAttributes || !filter.IsEmpty)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering if filter is specified
            if (!filter.IsEmpty)
            {
                EnsureFilterBitmapSize(ref filterBitmap, found);
                _ = ApplyPostFilter(filter, found, outputAttributes.AsReadOnlySpan(), filterBitmap.AsSpan(), ActiveThreadSession.scratchBufferBuilder);
            }

            if (continuation != 0)
            {
                // TODO: paged results!
                throw new NotImplementedException();
            }

            outputDistances.Length = sizeof(float) * found;

            // Default assumption is length prefixed
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            return VectorManagerResult.OK;
        }

        /// <summary>
        /// Fetch attributes for a single element id.
        /// 
        /// This must only be called while holding locks which prevent the Vector Set from being dropped.
        /// 
        /// IMPORTANT: outputAttributes may be replaced with an allocated memory, so the caller needs to check
        /// if the buffer is stack-based or heap-based, and dispose if it's the latter.
        /// </summary>
        internal VectorManagerResult FetchSingleVectorElementAttributes(ReadOnlySpan<byte> indexValue, SpanByte element, ref SpanByteAndMemory outputAttributes)
        {
            AssertHaveStorageSession();
            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out _, out _, out _);
            var found = ReadSizeUnknown(context | DiskANNService.Attributes, element.AsReadOnlySpan(), ref outputAttributes);
            return found ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Fetch attributes for a given set of element ids.
        /// 
        /// This must only be called while holding locks which prevent the Vector Set from being dropped.
        /// </summary>
        private void FetchVectorElementAttributes(ulong context, int numIds, SpanByteAndMemory ids, ref SpanByteAndMemory attributes)
        {
            var remainingIds = ids.AsReadOnlySpan();

            GCHandle idPin = default;
            byte[] idWithNamespaceArr = null;

            var attributesNextIx = 0;

            Span<byte> attributeFull = stackalloc byte[32];
            var attributeMem = SpanByteAndMemory.FromPinnedSpan(attributeFull);

            try
            {
                Span<byte> idWithNamespace = stackalloc byte[128];

                // TODO: we could scatter/gather this like MGET - doesn't matter when everything is in memory,
                //       but if anything is on disk it'd help perf
                for (var i = 0; i < numIds; i++)
                {
                    var idLen = BinaryPrimitives.ReadInt32LittleEndian(remainingIds);
                    if (idLen + sizeof(int) > remainingIds.Length)
                    {
                        throw new GarnetException($"Malformed ids, {idLen} + {sizeof(int)} > {remainingIds.Length}");
                    }

                    var id = remainingIds.Slice(sizeof(int), idLen);

                    // Make sure we've got enough space to query the element
                    if (id.Length + 1 > idWithNamespace.Length)
                    {
                        if (idWithNamespaceArr != null)
                        {
                            idPin.Free();
                            ArrayPool<byte>.Shared.Return(idWithNamespaceArr);
                        }

                        idWithNamespaceArr = ArrayPool<byte>.Shared.Rent(id.Length + 1);
                        idPin = GCHandle.Alloc(idWithNamespaceArr, GCHandleType.Pinned);
                        idWithNamespace = idWithNamespaceArr;
                    }

                    if (attributeMem.Memory != null)
                    {
                        attributeMem.Length = attributeMem.Memory.Memory.Length;
                    }
                    else
                    {
                        attributeMem.Length = attributeMem.SpanByte.Length;
                    }

                    var found = ReadSizeUnknown(context | DiskANNService.Attributes, id, ref attributeMem);

                    // Copy attribute into output buffer, length prefixed, resizing as necessary
                    var neededSpace = 4 + (found ? attributeMem.Length : 0);

                    var destSpan = attributes.AsSpan()[attributesNextIx..];
                    if (destSpan.Length < neededSpace)
                    {
                        var newAttrArr = MemoryPool<byte>.Shared.Rent(attributes.Length + neededSpace);
                        attributes.AsReadOnlySpan().CopyTo(newAttrArr.Memory.Span);

                        attributes.Memory?.Dispose();

                        attributes = new SpanByteAndMemory(newAttrArr, newAttrArr.Memory.Length);
                        destSpan = attributes.AsSpan()[attributesNextIx..];
                    }

                    BinaryPrimitives.WriteInt32LittleEndian(destSpan, attributeMem.Length);
                    attributeMem.AsReadOnlySpan().CopyTo(destSpan[sizeof(int)..]);

                    attributesNextIx += neededSpace;

                    remainingIds = remainingIds[(sizeof(int) + idLen)..];
                }

                attributes.Length = attributesNextIx;
            }
            finally
            {
                if (idWithNamespaceArr != null)
                {
                    idPin.Free();
                    ArrayPool<byte>.Shared.Return(idWithNamespaceArr);
                }

                attributeMem.Memory?.Dispose();
            }
        }

        /// <summary>
        /// Try to read the associated dimensions for an element out of a Vector Set.
        /// </summary>
        internal bool TryGetEmbedding(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            // Make sure enough space in distances for requested count
            if (dimensions * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent((int)dimensions * sizeof(float)), (int)dimensions * sizeof(float));
            }
            else
            {
                outputDistances.Length = (int)dimensions * sizeof(float);
            }

            Span<byte> internalId = stackalloc byte[sizeof(int)];
            var internalIdBytes = SpanByteAndMemory.FromPinnedSpan(internalId);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.InternalIdMap, element, ref internalIdBytes))
                {
                    return false;
                }

                Debug.Assert(internalIdBytes.IsSpanByte, "Internal Id should always be of known size");
            }
            finally
            {
                internalIdBytes.Memory?.Dispose();
            }

            Span<byte> asBytesSpan = stackalloc byte[1024];
            var asBytes = SpanByteAndMemory.FromPinnedSpan(asBytesSpan);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.FullVector, internalId, ref asBytes))
                {
                    return false;
                }

                var into = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());

                var from = asBytes.AsReadOnlySpan();

                // Internal vector format differs depend on the selected quantizer, so do that mapping as needed
                switch (quantType)
                {
                    // All Redis quantizers store F32s
                    case VectorQuantType.Bin:
                    case VectorQuantType.Q8:
                    case VectorQuantType.NoQuant:
                        MemoryMarshal.Cast<byte, float>(from).CopyTo(into);
                        break;

                    // XNoQuant_I8 & XBin_I8 stores _signed_ bytes
                    case VectorQuantType.XNoQuant_I8:
                    case VectorQuantType.XBin_I8:
                        for (var i = 0; i < from.Length; i++)
                        {
                            into[i] = (sbyte)from[i];
                        }
                        break;

                    // XNoQuant_I8 & NoQuant_U8 stores unsigned bytes
                    case VectorQuantType.XNoQuant_U8:
                    case VectorQuantType.XBin_U8:
                        for (var i = 0; i < from.Length; i++)
                        {
                            into[i] = from[i];
                        }
                        break;

                    case VectorQuantType.Invalid:
                    default:
                        throw new InvalidOperationException($"Unexpected VectorQuantType: {quantType}");
                }

                // Vector might have been deleted, so check that after getting data
                return Service.CheckInternalIdValid(context, indexPtr, internalId);
            }
            finally
            {
                asBytes.Memory?.Dispose();
            }
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }

        // DEBUG HACK HACK
        public byte[] GetInternalId(ulong context, byte[] externalId)
        {
            using var session = (RespServerSession)getTempSession();

            if (session.activeDbId != dbId && !session.TrySwitchActiveDatabaseSession(dbId))
            {
                throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
            }

            ActiveThreadSession = session.storageSession;
            var pin = GCHandle.Alloc(externalId, GCHandleType.Pinned);
            try
            {
                Span<byte> internalId = stackalloc byte[sizeof(int)];
                SpanByteAndMemory internalIdBytes = new(SpanByte.FromPinnedSpan(internalId));

                if (!ReadSizeUnknown(context | DiskANNService.InternalIdMap, externalId, ref internalIdBytes))
                {
                    throw new GarnetException("No internal id map");
                }

                var ret = internalIdBytes.AsReadOnlySpan().ToArray();
                internalIdBytes.Memory?.Dispose();

                return ret;
            }
            finally
            {
                pin.Free();
                ActiveThreadSession = null;
            }
        }

        public byte[] GetFullVector(ulong context, byte[] externalId)
        {
            using var session = (RespServerSession)getTempSession();

            if (session.activeDbId != dbId && !session.TrySwitchActiveDatabaseSession(dbId))
            {
                throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
            }

            ActiveThreadSession = session.storageSession;
            var pin = GCHandle.Alloc(externalId, GCHandleType.Pinned);
            try
            {
                Span<byte> fullVector = stackalloc byte[4 * 1024];
                SpanByteAndMemory fullVectorBytes = new(SpanByte.FromPinnedSpan(fullVector));

                if (!ReadSizeUnknown(context | DiskANNService.FullVector, externalId, ref fullVectorBytes))
                {
                    throw new GarnetException("No full vector stored");
                }

                var ret = fullVectorBytes.AsReadOnlySpan().ToArray();
                fullVectorBytes.Memory?.Dispose();

                return ret;
            }
            finally
            {
                pin.Free();
                ActiveThreadSession = null;
            }
        }
        // END DEBUG
    }
}