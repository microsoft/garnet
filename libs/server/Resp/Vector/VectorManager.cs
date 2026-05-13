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
        internal const long DeleteAfterDropArg = VADDAppendLogArg + 1;
        internal const long RecreateIndexArg = DeleteAfterDropArg + 1;
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
        /// Name for a context that will never be used by Vector Sets.
        /// </summary>
        private const ulong InvalidContext = 0;

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

        private readonly ILogger logger;

        private readonly int dbId;

        // context -> key for Vector Sets which are were discovered during recovery
        private ConcurrentDictionary<ulong, byte[]> vectorSetIndexKeyRecovery;

        public VectorManager(int dbId, GarnetServerOptions serverOptions, Func<IMessageConsumer> getCleanupSession, ILoggerFactory loggerFactory)
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

            this.getCleanupSession = getCleanupSession;
            cleanupTaskChannel = Channel.CreateUnbounded<ulong>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            cleanupTask = RunCleanupTaskAsync();
            vectorSetIndexKeyRecovery = new();

            logger?.LogInformation("Created VectorManager");
        }

        /// <summary>
        /// Load state necessary for VectorManager from main store.
        /// </summary>
        public void Initialize()
        {
            if (!IsEnabled) return;

            using var session = (RespServerSession)getCleanupSession();
            if (session.activeDbId != dbId && !session.TrySwitchActiveDatabaseSession(dbId))
            {
                throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
            }

            VectorElementKey key = new(MetadataNamespace, []);

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
        }

        /// <summary>
        /// Restart or update any pending work that was discovered as part of recovery.
        /// </summary>
        public void ResumePostRecovery()
        {
            if (!IsEnabled) return;

            using var session = (RespServerSession)getCleanupSession();

            ref var ctx = ref session.storageSession.vectorBasicContext;

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

            // Resume any cleanups we didn't complete before recovery
            _ = cleanupTaskChannel.Writer.TryWrite(InvalidContext);
        }

        /// <summary>
        /// Called during recovery to note any Vector Sets which might be in the process of being deleted.
        /// </summary>
        public void QueueForInProgressDeleteCheck(ref LogRecord logRecord)
        {
            if (logRecord.ValueSpan.Length != IndexSize)
            {
                logger?.LogWarning("Skipping Vector Set '{key}' deletion checking during recovery, index size {size} != {IndexSize}", SpanByte.ToShortString(logRecord.Key), logRecord.ValueSpan.Length, IndexSize);
                return;
            }

            ReadIndex(logRecord.ValueSpan, out var context, out _, out _, out _, out _, out _, out _, out _);

            ref var asIndex = ref MemoryMarshal.Cast<byte, Index>(logRecord.ValueSpan)[0];
            if (asIndex.NeedsCleanup)
            {
                var added = vectorSetIndexKeyRecovery.TryAdd(context, logRecord.Key.ToArray());
                Debug.Assert(added, "Recovery found multiple Vector Set indexes with same context");

                _ = cleanupTaskChannel.Writer.TryWrite(context);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // We must drain all these before disposing, otherwise we'll leave replicationBlockEvent unset
            _ = replicationReplayChannel.Writer.TryComplete();
            AsyncUtils.BlockingWait(replicationReplayChannel.Reader.Completion);
            AsyncUtils.BlockingWait(Task.WhenAll(replicationReplayTasks));

            replicationBlockEvent.Dispose();

            // Wait for any in progress cleanup to finish. PauseCleanupAsync callers MUST
            // have called ResumeCleanup before reaching here, otherwise the cleanup task
            // is permanently blocked on cleanupGate.WaitAsync() and Dispose will hang.
            cleanupTaskChannel.Writer.Complete();
            AsyncUtils.BlockingWait(cleanupTaskChannel.Reader.Completion);
            AsyncUtils.BlockingWait(cleanupTask);

            // Cleanup task has fully drained, so nothing else can take this gate.
            cleanupGate.Dispose();
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

            var valueDims = CalculateValueDimensions(valueType, values);

            if (dimensions != valueDims)
            {
                // Matching Redis behavior
                errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {valueDims} but set has {dimensions}");
                return VectorManagerResult.BadParams;
            }

            if (providedReduceDims == 0 && reduceDims != 0)
            {
                // Matching Redis behavior, which is definitely a bit weird here
                errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {valueDims} but set has {reduceDims}");
                return VectorManagerResult.BadParams;
            }
            else if (providedReduceDims != 0 && providedReduceDims != reduceDims)
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

            var insert =
                Service.Insert(
                    context,
                    indexPtr,
                    element,
                    valueType,
                    values,
                    attributes
                );

            if (insert)
            {
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

            // Tell DiskANN to clean itself up right now
            DropIndex(value);

            // Clear the DiskANN index
            ref var asIndex = ref MemoryMarshal.Cast<byte, Index>(value)[0];
            asIndex.IndexPtr = 0;
            asIndex.NeedsCleanup = true;

            _ = cleanupTaskChannel.Writer.TryWrite(context);
        }

        /// <summary>
        /// Ask DiskANN to drop its index.
        /// </summary>
        internal void DropInMemoryIndex(ReadOnlySpan<byte> value)
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
            ReadOnlySpan<byte> indexValue,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
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

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr);

            var valueDims = CalculateValueDimensions(valueType, values);
            if (dimensions != valueDims)
            {
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

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

            // Make sure enough space in distances for requested count
            if (retrieveCount > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(retrieveCount * sizeof(float)), retrieveCount * sizeof(float));
            }

            // Indicate requested # of matches
            outputDistances.Length = retrieveCount * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (retrieveCount * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(retrieveCount * MinimumSpacePerId), retrieveCount * MinimumSpacePerId);
            }

            var found =
                Service.SearchVector(
                    context,
                    indexPtr,
                    valueType,
                    values,
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

                ApplyPostFilter(filter, found, outputAttributes.ReadOnlySpan, filterBitmap.Span, ActiveThreadSession.scratchBufferBuilder);
            }

            if (continuation != 0)
            {
                // TODO: paged results!
                throw new NotImplementedException();
            }

            outputDistances.Length = sizeof(float) * found;

            // Default assumption is length prefixed
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            if (quantType == VectorQuantType.XPreQ8)
            {
                // But in this special case, we force them to be 4-byte ids
                //outputIdFormat = VectorIdFormat.FixedI32;
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;
            }

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

            // Make sure enough space in distances for requested count
            if (retrieveCount * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(retrieveCount * sizeof(float)), retrieveCount * sizeof(float));
            }

            // Indicate requested # of matches
            outputDistances.Length = retrieveCount * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (retrieveCount * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(retrieveCount * MinimumSpacePerId), retrieveCount * MinimumSpacePerId);
            }

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

                ApplyPostFilter(filter, found, outputAttributes.ReadOnlySpan, filterBitmap.Span, ActiveThreadSession.scratchBufferBuilder);
            }

            if (continuation != 0)
            {
                // TODO: paged results!
                throw new NotImplementedException();
            }

            outputDistances.Length = sizeof(float) * found;

            // Default assumption is length prefixed
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            if (quantType == VectorQuantType.XPreQ8)
            {
                // But in this special case, we force them to be 4-byte ids
                //outputIdFormat = VectorIdFormat.FixedI32;
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;
            }

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
            var found = ReadSizeUnknown(context | DiskANNService.Attributes, forceAlignment: true, element, ref outputAttributes);
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

                    var found = ReadSizeUnknown(context | DiskANNService.Attributes, forceAlignment: true, id, ref attributeMem);

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
                if (!ReadSizeUnknown(context | DiskANNService.InternalIdMap, forceAlignment: true, element, ref internalIdBytes))
                {
                    return false;
                }

                Debug.Assert(internalIdBytes.IsSpanByte, "Internal Id should always be of known size");
            }
            finally
            {
                internalIdBytes.Memory?.Dispose();
            }

            Span<byte> asBytesSpan = stackalloc byte[(int)dimensions];
            var asBytes = SpanByteAndMemory.FromPinnedSpan(asBytesSpan);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.FullVector, forceAlignment: true, internalId, ref asBytes))
                {
                    return false;
                }

                var into = MemoryMarshal.Cast<byte, float>(outputDistances.Span);

                var from = asBytes.ReadOnlySpan;
                if (quantType == VectorQuantType.NoQuant)
                {
                    var fromFloat = MemoryMarshal.Cast<byte, float>(from);
                    fromFloat.CopyTo(into);
                }
                else if (quantType == VectorQuantType.XPreQ8)
                {
                    for (var i = 0; i < asBytes.Length; i++)
                    {
                        into[i] = from[i];
                    }
                }
                else
                {
                    // TODO: Handle Q8 and BIN as they are implemented
                    throw new NotImplementedException($"Unexpected quantization: {quantType}");
                }

                // Vector might have been deleted, so check that after getting data
                return Service.CheckInternalIdValid(context, indexPtr, internalId);
            }
            finally
            {
                asBytes.Memory?.Dispose();
            }
        }

        /// <summary>
        /// Determine the dimensions of a vector given its <see cref="VectorValueType"/> and its raw data.
        /// </summary>
        internal static uint CalculateValueDimensions(VectorValueType valueType, ReadOnlySpan<byte> values)
        {
            if (valueType == VectorValueType.FP32)
            {
                return (uint)(values.Length / sizeof(float));
            }
            else if (valueType == VectorValueType.XB8)
            {
                return (uint)(values.Length);
            }
            else
            {
                throw new NotImplementedException($"{valueType}");
            }
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}