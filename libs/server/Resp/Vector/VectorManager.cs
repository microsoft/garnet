// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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

        // We reserve the first 7 namespaces (we can't use 0, so it's off limits) for store-wide metadata about Vector Sets
        internal const byte MetadataNamespace = 1;

        internal const int IndexSizeBytes = Index.Size;
        internal const long VADDAppendLogArg = long.MinValue;
        // DeleteAfterDropArg used to be here
        internal const long RecreateIndexArg = VADDAppendLogArg + 2;
        internal const long VREMAppendLogArg = RecreateIndexArg + 1;
        internal const long MigrateElementKeyLogArg = VREMAppendLogArg + 1;
        internal const long MigrateIndexKeyLogArg = MigrateElementKeyLogArg + 1;

        /// <summary>
        /// Byte stored on log records to distinguish the INDEX key as a Vector Set
        /// Element keys are tracked in separate namespaces and are not marked with a special RecordType
        /// </summary>
        public const byte RecordType = 1;

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
        /// This managers instance of <see cref="DiskANNService"/>.
        /// 
        /// We could probably share these, but its not a big loss to scope to the <see cref="VectorManager"/> instance.
        /// </summary>
        internal DiskANNService Service { get; } = new DiskANNService();

        /// <summary>
        /// Whether or not Vector Set preview is enabled.
        /// 
        /// TODO: This goes away once we're stable.
        /// </summary>
        public bool IsEnabled { get; }

        private bool initialized;

        private readonly ILogger logger;

        private readonly int dbId;

        private ConcurrentDictionary<ulong, byte> recoveredIndexes;

        public VectorManager(int dbId, GarnetServerOptions serverOptions, Func<IMessageConsumer> getTempSession, ILoggerFactory loggerFactory)
        {
            this.dbId = dbId;

            IsEnabled = serverOptions.EnableVectorSetPreview;

            // Include DB and id so we correlate to what's actually stored in the log
            logger = loggerFactory?.CreateLogger($"{nameof(VectorManager)}:{dbId}");

            replicationBlockEvent = CountingEventSlim.Create();
            // NOTE: for multi-log we need to disable single writer since multiple AOF replay tasks may append to this common channel.
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = !serverOptions.MultiLogEnabled, SingleReader = false, AllowSynchronousContinuations = false });

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
            requestCleanupTaskChannel = Channel.CreateUnbounded<(ulong Context, TaskCompletionSource Completion)>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            requestDropTaskChannel = Channel.CreateUnbounded<object>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });

            cleanupTask = RunCleanupTaskAsync();
            requestCleanupTask = RunRequestCleanupTaskAsync();
            requestDropTask = RunRequestDropTaskAsync();

            requestedDrops = new(ByteArrayComparer.Instance);
#if NET9_0_OR_GREATER
            requestedDropsLookup = requestedDrops.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif

            recoveredIndexes = new();

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

#pragma warning disable IDE0302 // [...]-style collection initialization doesn't actually _guarantee_ stackalloc (or inline arrays), which we need here
            ReadOnlySpan<byte> nsBytes = stackalloc byte[1] { MetadataNamespace };
#pragma warning restore IDE0302 
            VectorElementKey key = new(nsBytes, []);

            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

            VectorOutput data = new(dataSpan);

            ref var ctx = ref session.storageSession.vectorBasicContext;

            var status = ctx.Read(key, ref data);

            if (status.IsPending)
            {
                VectorOutput ignored = new();
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

            ref var ctx = ref session.storageSession.vectorBasicContext;

            lock (this)
            {
                // If we come up and contexts are marked for migration, that means the migration FAILED
                // and we'd like those contexts back ASAP
                var abandonedMigrations = contextMetadata.GetMigrating();
                var needsUpdated = false;

                if (abandonedMigrations != null)
                {
                    foreach (var abandoned in abandonedMigrations)
                    {
                        contextMetadata.MarkMigrationComplete(abandoned, ushort.MaxValue);
                        contextMetadata.MarkCleaningUp(abandoned);
                    }

                    needsUpdated = true;
                }

                // Any non-deleted records we recovered for contexts being deleted, we need to undo that
                foreach (var (context, _) in recoveredIndexes)
                {
                    if (contextMetadata.IsCleaningUp(context))
                    {
                        contextMetadata.ClearIsCleaningUp(context);
                        needsUpdated = true;
                    }

                    recoveredIndexes = null;
                }

                if (needsUpdated)
                {
                    UpdateContextMetadata(ref ctx);
                }
            }

            // Resume any cleanups we didn't complete before recovery
            _ = cleanupTaskChannel.Writer.TryWrite(null);
        }

        public void RecoveredVectorSetIndexKey(ref LogRecord record)
        {
            if (record.ValueSpan.Length != IndexSize)
            {
                return;
            }

            ReadIndex(record.ValueSpan, out var context, out _, out _, out _, out _, out _, out _, out _);
            recoveredIndexes[context] = 0;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // We must drain all these before disposing, otherwise we'll leave replicationBlockEvent unset
            _ = replicationReplayChannel.Writer.TryComplete();
            AsyncUtils.BlockingWait(replicationReplayChannel.Reader.Completion);
            AsyncUtils.BlockingWait(Task.WhenAll(replicationReplayTasks));

            replicationBlockEvent.Dispose();

            // Wait for any _drops_ in progress to finish
            requestDropTaskChannel.Writer.Complete();
            AsyncUtils.BlockingWait(requestDropTaskChannel.Reader.Completion);
            AsyncUtils.BlockingWait(requestDropTask);

            // Wait for any _marking_ of cleanup state to finish. PauseCleanupAsync callers MUST
            // have called ResumeCleanup before reaching here, otherwise the cleanup task
            // is permanently blocked on cleanupGate.WaitAsync() and Dispose will hang.
            requestCleanupTaskChannel.Writer.Complete();
            AsyncUtils.BlockingWait(requestCleanupTaskChannel.Reader.Completion);
            AsyncUtils.BlockingWait(requestCleanupTask);

            // Wait for any in progress cleanup to finish. PauseCleanupAsync callers MUST
            // have called ResumeCleanup before reaching here, otherwise the cleanup task
            // is permanently blocked on cleanupGate.WaitAsync() and Dispose will hang.
            cleanupTaskChannel.Writer.Complete();
            AsyncUtils.BlockingWait(cleanupTaskChannel.Reader.Completion);
            AsyncUtils.BlockingWait(cleanupTask);

            // Cleanup task has fully drained, so nothing else can take this gate.
            cleanupGate.Dispose();

            // drain quantization task
            _ = quantizationChannel.Writer.TryComplete();
            while (quantizationChannel.Reader.TryRead(out _)) { }
            AsyncUtils.BlockingWait(Task.WhenAll(quantizationTasks));
        }

        private static void CompletePending(ref Status status, ref VectorOutput output, ref VectorBasicContext ctx)
        {
            _ = ctx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            Debug.Assert(!completedOutputs.Next());
            completedOutputs.Dispose();
        }

        private static void CompletePending(ref Status status, ref StringBasicContext ctx)
        {
            _ = ctx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
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
            scoped ReadOnlySpan<byte> key,
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

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out _, out var numLinks, out var distanceMetric, out var indexPtr);

            if (providedReduceDims != 0 && providedReduceDims != reduceDims)
            {
                errorMsg = "ERR Provided REDUCE does not match Vector Set definition"u8;
                return VectorManagerResult.BadParams;
            }

            if (providedQuantType != VectorQuantType.Invalid && providedQuantType != quantType)
            {
                errorMsg = "ERR asked quantization mismatch with existing vector set"u8;
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
                    _ = quantizationChannel.Writer.TryWrite(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
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

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out _, out var indexPtr);

            var del = Service.Remove(context, indexPtr, element);

            return del ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Request deletion of a Vector Set given the VALUE of the index key.
        /// </summary>
        internal void RequestDeletion(Span<byte> value)
        {
            if (value.Length != IndexSize)
            {
                logger?.LogWarning($"Ignored Vector Set deletion due to size mismatch");
                return;
            }

            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

            ReadIndex(value, out var context, out _, out _, out _, out _, out _, out _, out _);

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!requestCleanupTaskChannel.Writer.TryWrite((context, tcs)))
            {
                throw new GarnetException("Could not submit request for Vector Set cleanup, aborting delete");
            }

            // Wait until the context is _marked_ for cleanup, but not the actual cleanup
            AsyncUtils.BlockingWait(tcs.Task);

            // Tell DiskANN to clean itself up
            DropIndex(value);
        }

        /// <summary>
        /// Request that the DiskANN side of an index be dropped.
        /// 
        /// This happens when a record is evicted to disk.
        /// 
        /// There's subtlety here because the DiskANN index might be in use (on the current or other threads)
        /// and we can't allow the index to be recreated until any requested drops are processed.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        internal void RequestDropInMemoryIndex(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (value.Length != IndexSize)
            {
                logger?.LogWarning($"Ignored Vector Set drop index due to size mismatch");
                return;
            }

            ReadIndex(value, out var context, out _, out _, out _, out _, out _, out _, out var indexPtr);

            // It's possible for an index to be recovered from disk but never initialized, which means we need no drop
            if (indexPtr != 0)
            {
                if (!requestedDrops.TryAdd(key.ToArray(), (context, indexPtr)))
                {
                    throw new GarnetException($"Drop triggered multiple times for same index: {SpanByte.ToShortString(key)}");
                }

                _ = requestDropTaskChannel.Writer.TryWrite(null);
            }
        }

        /// <summary>
        /// Ask DiskANN to drop its index.
        /// </summary>
        private void DropInMemoryIndex(ReadOnlySpan<byte> value)
        {
            if (value.Length != IndexSize)
            {
                logger?.LogWarning($"Ignored Vector Set drop index due to size mismatch");
                return;
            }

            ReadIndex(value, out var context, out _, out _, out _, out _, out _, out _, out var indexPtr);

            Service.DropIndex(context, indexPtr);
        }

        /// <summary>
        /// Clear out the index pointer stored in this record value.
        /// 
        /// Next time the record is touched, we'll recreate the index.
        /// </summary>
        internal static void ClearIndexPointer(Span<byte> value)
        {
            if (value.Length != IndexSize)
            {
                return;
            }

            ref var index = ref MemoryMarshal.Cast<byte, Index>(value)[0];
            index.IndexPtr = 0;
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ValueSimilarity(
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

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr);

            // When a filter is present, over-retrieve candidates from DiskANN so that
            // post-filtering has enough results to fill the requested count.
            //
            // FILTER-EF controls both the graph exploration breadth and the output
            // buffer size when a filter is active, allowing it to be tuned independently
            // from EF (which is used for unfiltered searches).
            var retrieveCount = !filter.IsEmpty ? maxFilteringEffort : count;
            var effectiveEF = !filter.IsEmpty
                ? Math.Max(searchExplorationFactor, maxFilteringEffort)
                : searchExplorationFactor;

            // No point in asking for more data than the effort we'll put in
            if (retrieveCount > effectiveEF)
            {
                retrieveCount = effectiveEF;
            }

            EnsureDistanceBufferSize(ref outputDistances, retrieveCount);
            EnsureIdBufferSize(ref outputIds, retrieveCount);

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
                    errorMsg = "ERR Dimensions provided do not match Vector Set dimensions"u8;
                    return VectorManagerResult.BadParams;
                }

                found =
                    Service.SearchVector(
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
                // Ensure bitmap is large enough for the over-retrieved result set
                var requiredBitmapBytes = (found + 7) >> 3;
                if (requiredBitmapBytes > filterBitmap.Length)
                {
                    if (!filterBitmap.IsSpanByte)
                    {
                        filterBitmap.Memory.Dispose();
                    }

                    filterBitmap = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(requiredBitmapBytes), requiredBitmapBytes);
                }

                _ = ApplyPostFilter(filter, found, outputAttributes.ReadOnlySpan, filterBitmap.Span, ActiveThreadSession.scratchBufferBuilder);
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

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ElementSimilarity(
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

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out _, out var indexPtr);

            // When a filter is present, over-retrieve candidates from DiskANN
            var retrieveCount = !filter.IsEmpty ? maxFilteringEffort : count;
            var effectiveEF = !filter.IsEmpty
                ? Math.Max(searchExplorationFactor, maxFilteringEffort)
                : searchExplorationFactor;

            // No point in asking for more data than the effort we'll put in
            if (retrieveCount > effectiveEF)
            {
                retrieveCount = effectiveEF;
            }

            EnsureDistanceBufferSize(ref outputDistances, retrieveCount);
            EnsureIdBufferSize(ref outputIds, retrieveCount);

            var found =
                Service.SearchElement(
                    context,
                    indexPtr,
                    element,
                    delta,
                    effectiveEF,
                    filter,
                    maxFilteringEffort,
                    outputIds,
                    outputDistances,
                    out var continuation
                );

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
                // Ensure bitmap is large enough for the over-retrieved result set
                var requiredBitmapBytes = (found + 7) >> 3;
                if (requiredBitmapBytes > filterBitmap.Length)
                {
                    if (!filterBitmap.IsSpanByte)
                    {
                        filterBitmap.Memory.Dispose();
                    }

                    filterBitmap = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(requiredBitmapBytes), requiredBitmapBytes);
                }

                _ = ApplyPostFilter(filter, found, outputAttributes.ReadOnlySpan, filterBitmap.Span, ActiveThreadSession.scratchBufferBuilder);
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
        internal VectorManagerResult FetchSingleVectorElementAttributes(ReadOnlySpan<byte> indexValue, PinnedSpanByte element, ref SpanByteAndMemory outputAttributes)
        {
            AssertHaveStorageSession();
            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out _, out _);
            var found = ReadSizeUnknown(context | DiskANNService.Attributes, element, ref outputAttributes);
            return found ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Fetch attributes for a given set of element ids.
        /// 
        /// This must only be called while holding locks which prevent the Vector Set from being dropped.
        /// </summary>
        private void FetchVectorElementAttributes(ulong context, int numIds, SpanByteAndMemory ids, ref SpanByteAndMemory attributes)
        {
            var remainingIds = ids.ReadOnlySpan;

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

                    var destSpan = attributes.Span[attributesNextIx..];
                    if (destSpan.Length < neededSpace)
                    {
                        var newAttrArr = MemoryPool<byte>.Shared.Rent(attributes.Length + neededSpace);
                        attributes.ReadOnlySpan.CopyTo(newAttrArr.Memory.Span);

                        attributes.Memory?.Dispose();

                        attributes = new SpanByteAndMemory(newAttrArr, newAttrArr.Memory.Length);
                        destSpan = attributes.Span[attributesNextIx..];
                    }

                    BinaryPrimitives.WriteInt32LittleEndian(destSpan, attributeMem.Length);
                    attributeMem.ReadOnlySpan.CopyTo(destSpan[sizeof(int)..]);

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

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr);

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

                var into = MemoryMarshal.Cast<byte, float>(outputDistances.Span);
                var from = asBytes.ReadOnlySpan;

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
    }
}