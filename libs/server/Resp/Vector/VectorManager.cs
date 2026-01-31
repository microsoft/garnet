// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
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
        /// Minimum size of an id is assumed to be at least 4 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 4;

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

        /// <summary>
        /// Unique id for this <see cref="VectorManager"/>.
        /// 
        /// Is used to determine if an <see cref="Index"/> is backed by a DiskANN index that was created in this process.
        /// </summary>
        private readonly Guid processInstanceId = Guid.NewGuid();

        private readonly ILogger logger;

        private readonly int dbId;

        public VectorManager(bool enabled, int dbId, Func<IMessageConsumer> getCleanupSession, ILoggerFactory loggerFactory)
        {
            this.dbId = dbId;

            IsEnabled = enabled;

            // Include DB and id so we correlate to what's actually stored in the log
            logger = loggerFactory?.CreateLogger($"{nameof(VectorManager)}:{dbId}:{processInstanceId}");

            replicationBlockEvent = CountingEventSlim.Create();
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });

            // TODO: Pull this off a config or something
            replicationReplayTasks = new Task[Environment.ProcessorCount];
            for (var i = 0; i < replicationReplayTasks.Length; i++)
            {
                replicationReplayTasks[i] = Task.CompletedTask;
            }

            // TODO: Probably configurable?
            // For now, just number of processors
            vectorSetLocks = new(Environment.ProcessorCount);

            this.getCleanupSession = getCleanupSession;
            cleanupTaskChannel = Channel.CreateUnbounded<object>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            cleanupTask = RunCleanupTaskAsync();

            logger?.LogInformation("Created VectorManager");
        }

        /// <summary>
        /// Load state necessary for VectorManager from main store.
        /// </summary>
        public void Initialize()
        {
            using var session = (RespServerSession)getCleanupSession();
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

        }

        /// <summary>
        /// Restart or update any pending work that was discovered as part of recovery.
        /// </summary>
        public void ResumePostRecovery()
        {
            using var session = (RespServerSession)getCleanupSession();

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
            replicationReplayChannel.Writer.Complete();
            replicationReplayChannel.Reader.Completion.Wait();

            Task.WhenAll(replicationReplayTasks).Wait();

            replicationBlockEvent.Dispose();

            // Wait for any in progress cleanup to finish
            cleanupTaskChannel.Writer.Complete();
            cleanupTaskChannel.Reader.Completion.Wait();
            cleanupTask.Wait();
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
            ref SpanByteAndMemory outputAttributes
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            var valueDims = CalculateValueDimensions(valueType, values);
            if (dimensions != valueDims)
            {
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            // No point in asking for more data than the effort we'll put in
            if (count > searchExplorationFactor)
            {
                count = searchExplorationFactor;
            }

            // Make sure enough space in distances for requested count
            if (count > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId));
            }

            var found =
                Service.SearchVector(
                    context,
                    indexPtr,
                    valueType,
                    values,
                    delta,
                    searchExplorationFactor,
                    filter,
                    maxFilteringEffort,
                    outputIds.AsSpan(),
                    MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan()),
                    out var continuation
                );

            if (found < 0)
            {
                logger?.LogWarning("Error indicating response from vector service {found}", found);
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering if filter is specified
            if (!filter.IsEmpty && includeAttributes)
            {
                found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes, includeAttributes);
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
            ref SpanByteAndMemory outputAttributes
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            // No point in asking for more data than the effort we'll put in
            if (count > searchExplorationFactor)
            {
                count = searchExplorationFactor;
            }

            // Make sure enough space in distances for requested count
            if (count * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId));
            }

            var found =
                Service.SearchElement(
                    context,
                    indexPtr,
                    element,
                    delta,
                    searchExplorationFactor,
                    filter,
                    maxFilteringEffort,
                    outputIds.AsSpan(),
                    MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan()),
                    out var continuation
                );

            if (found < 0)
            {
                logger?.LogWarning("Error indicating response from vector service {found}", found);
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering if filter is specified
            if (!filter.IsEmpty && includeAttributes)
            {
                found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes, includeAttributes);
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

            ReadIndex(indexValue, out var context, out var dimensions, out _, out _, out _, out _, out _, out var indexPtr, out _);

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

            Span<byte> asBytesSpan = stackalloc byte[(int)dimensions];
            var asBytes = SpanByteAndMemory.FromPinnedSpan(asBytesSpan);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.FullVector, internalId, ref asBytes))
                {
                    return false;
                }

                var from = asBytes.AsReadOnlySpan();
                var into = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());

                for (var i = 0; i < asBytes.Length; i++)
                {
                    into[i] = from[i];
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

        /// <summary>
        /// Apply post-filtering to vector search results based on JSON path filter expression.
        /// </summary>
        private int ApplyPostFilter(
            ReadOnlySpan<byte> filter,
            int numResults,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            bool hasAttributes)
        {
            if (numResults == 0 || !hasAttributes)
            {
                return numResults;
            }

            var filterStr = Encoding.UTF8.GetString(filter);
            var filteredCount = 0;

            var idsSpan = outputIds.AsSpan();
            var distancesSpan = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());
            var attributesSpan = outputAttributes.AsSpan();

            var idReadPos = 0;
            var attrReadPos = 0;
            var idWritePos = 0;
            var distWritePos = 0;
            var attrWritePos = 0;

            for (var i = 0; i < numResults; i++)
            {
                // Read ID
                var idLen = BinaryPrimitives.ReadInt32LittleEndian(idsSpan[idReadPos..]);
                var idTotalLen = sizeof(int) + idLen;

                // Read attribute
                var attrLen = BinaryPrimitives.ReadInt32LittleEndian(attributesSpan[attrReadPos..]);
                var attrData = attributesSpan.Slice(attrReadPos + sizeof(int), attrLen);

                // Evaluate filter
                if (EvaluateFilter(filterStr, attrData))
                {
                    // Copy ID if not already in place
                    if (idReadPos != idWritePos)
                    {
                        idsSpan.Slice(idReadPos, idTotalLen).CopyTo(idsSpan[idWritePos..]);
                    }

                    // Copy distance if not already in place
                    if (i != distWritePos)
                    {
                        distancesSpan[distWritePos] = distancesSpan[i];
                    }

                    // Copy attribute if not already in place
                    if (attrReadPos != attrWritePos)
                    {
                        attributesSpan.Slice(attrReadPos, sizeof(int) + attrLen).CopyTo(attributesSpan[attrWritePos..]);
                    }

                    idWritePos += idTotalLen;
                    distWritePos++;
                    attrWritePos += sizeof(int) + attrLen;
                    filteredCount++;
                }

                idReadPos += idTotalLen;
                attrReadPos += sizeof(int) + attrLen;
            }

            // Update lengths
            outputIds.Length = idWritePos;
            outputDistances.Length = distWritePos * sizeof(float);
            outputAttributes.Length = attrWritePos;

            return filteredCount;
        }

        /// <summary>
        /// Evaluate a JSON path filter expression against attribute data.
        /// Supports: arithmetic, comparison, logical operators, containment (in), and grouping with parentheses.
        /// </summary>
        private static bool EvaluateFilter(string filter, ReadOnlySpan<byte> attributeJson)
        {
            try
            {
                using var jsonDoc = JsonDocument.Parse(attributeJson.ToArray());
                var root = jsonDoc.RootElement;

                var tokens = VectorFilterTokenizer.Tokenize(filter);
                var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
                var result = VectorFilterEvaluator.EvaluateExpression(expr, root);

                return VectorFilterEvaluator.IsTruthy(result);
            }
            catch (Exception)
            {
                // If filter evaluation fails, exclude the result for safety
                return false;
            }
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}