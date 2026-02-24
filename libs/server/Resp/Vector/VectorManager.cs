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
using Garnet.server.Vector.Filter;
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
        /// When a post-filter is active, request this many times more candidates from DiskANN
        /// to compensate for candidates that will be discarded by the filter.
        /// A single search with this multiplier is tried first; if not enough results pass,
        /// a second search with the retry multiplier is attempted.
        /// </summary>
        private const int FilterOverFetchMultiplier = 5;

        /// <summary>
        /// When the initial over-fetch doesn't yield enough results, retry with this larger multiplier.
        /// </summary>
        private const int FilterOverFetchRetryMultiplier = 10;

        /// <summary>
        /// Maximum number of pages to fetch during paged search to prevent unbounded iteration.
        /// </summary>
        private const int MaxPagedSearchPages = 20;

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

            var requestedCount = count;
            var hasFilter = !filter.IsEmpty;

            // Static over-fetch path
            if (hasFilter)
            {
                count = Math.Min(count * FilterOverFetchMultiplier, searchExplorationFactor);
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

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId), count * MinimumSpacePerId);
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

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering if filter is specified
            if (hasFilter)
            {
                if (includeAttributes)
                {
                    found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes);
                }
                else
                {
                    var tempAttributes = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(found * 64), found * 64);
                    try
                    {
                        FetchVectorElementAttributes(context, found, outputIds, ref tempAttributes);
                        found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref tempAttributes);
                    }
                    finally
                    {
                        tempAttributes.Memory?.Dispose();
                    }
                }

                // If static over-fetch didn't yield enough results, retry with larger multiplier
                if (found < requestedCount)
                {
                    var retryCount = Math.Min(requestedCount * FilterOverFetchRetryMultiplier, searchExplorationFactor);
                    if (retryCount > count)
                    {
                        count = retryCount;

                        // Resize buffers for larger retry
                        if (count * sizeof(float) > outputDistances.Length)
                        {
                            if (!outputDistances.IsSpanByte) outputDistances.Memory.Dispose();
                            outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
                        }
                        outputDistances.Length = count * sizeof(float);

                        if (count * MinimumSpacePerId > outputIds.Length)
                        {
                            if (!outputIds.IsSpanByte) outputIds.Memory.Dispose();
                            outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId), count * MinimumSpacePerId);
                        }

                        found = Service.SearchVector(context, indexPtr, valueType, values,
                            delta, searchExplorationFactor, filter, maxFilteringEffort,
                            outputIds, outputDistances, out continuation);

                        if (found > 0)
                        {
                            if (includeAttributes)
                            {
                                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
                                found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes);
                            }
                            else
                            {
                                var tempAttrs = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(found * 64), found * 64);
                                try
                                {
                                    FetchVectorElementAttributes(context, found, outputIds, ref tempAttrs);
                                    found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref tempAttrs);
                                }
                                finally
                                {
                                    tempAttrs.Memory?.Dispose();
                                }
                            }
                        }
                    }
                }

                // Trim to originally requested count after post-filtering
                if (found > requestedCount)
                {
                    found = requestedCount;
                }
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

            var requestedCount = count;
            var hasFilter = !filter.IsEmpty;

            // Static over-fetch path
            if (hasFilter)
            {
                count = Math.Min(count * FilterOverFetchMultiplier, searchExplorationFactor);
            }

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

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId), count * MinimumSpacePerId);
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

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering if filter is specified
            if (hasFilter)
            {
                if (includeAttributes)
                {
                    found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes);
                }
                else
                {
                    var tempAttributes = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(found * 64), found * 64);
                    try
                    {
                        FetchVectorElementAttributes(context, found, outputIds, ref tempAttributes);
                        found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref tempAttributes);
                    }
                    finally
                    {
                        tempAttributes.Memory?.Dispose();
                    }
                }

                // If static over-fetch didn't yield enough results, retry with larger multiplier
                if (found < requestedCount)
                {
                    var retryCount = Math.Min(requestedCount * FilterOverFetchRetryMultiplier, searchExplorationFactor);
                    if (retryCount > count)
                    {
                        count = retryCount;

                        // Resize buffers for larger retry
                        if (count * sizeof(float) > outputDistances.Length)
                        {
                            if (!outputDistances.IsSpanByte) outputDistances.Memory.Dispose();
                            outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
                        }
                        outputDistances.Length = count * sizeof(float);

                        if (count * MinimumSpacePerId > outputIds.Length)
                        {
                            if (!outputIds.IsSpanByte) outputIds.Memory.Dispose();
                            outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId), count * MinimumSpacePerId);
                        }

                        found = Service.SearchElement(context, indexPtr, element,
                            delta, searchExplorationFactor, filter, maxFilteringEffort,
                            outputIds, outputDistances, out continuation);

                        if (found > 0)
                        {
                            if (includeAttributes)
                            {
                                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
                                found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes);
                            }
                            else
                            {
                                var tempAttrs = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(found * 64), found * 64);
                                try
                                {
                                    FetchVectorElementAttributes(context, found, outputIds, ref tempAttrs);
                                    found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref tempAttrs);
                                }
                                finally
                                {
                                    tempAttrs.Memory?.Dispose();
                                }
                            }
                        }
                    }
                }

                // Trim to originally requested count after post-filtering
                if (found > requestedCount)
                {
                    found = requestedCount;
                }
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
        /// Perform a filtered similarity search using DiskANN's paged search API.
        /// Fetches pages of results on-demand until enough pass the filter or max pages reached.
        /// </summary>
        private VectorManagerResult PagedFilterSearch(
            ulong context,
            nint indexPtr,
            VectorQuantType quantType,
            int requestedCount,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            bool isVector,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            ReadOnlySpan<byte> element,
            int initialPassingCount = 0)
        {
            // Parse the filter expression once outside the page loop
            Expr filterExpr;
            try
            {
                var filterStr = Encoding.UTF8.GetString(filter);
                var tokens = VectorFilterTokenizer.Tokenize(filterStr);
                filterExpr = VectorFilterParser.ParseExpression(tokens, 0, out var endIndex);

                if (endIndex != tokens.Count)
                {
                    throw new ArgumentException("Invalid filter expression: unexpected tokens after end of expression.", nameof(filter));
                }
            }
            catch (Exception ex) when (ex is ArgumentException || ex is FormatException || ex is InvalidOperationException)
            {
                throw new ArgumentException("Invalid filter expression.", nameof(filter), ex);
            }

            // Page size: use the full exploration factor for the fallback path.
            // Since this only runs when the static over-fetch didn't yield enough results,
            // we want maximum quality per page to minimize the number of pages needed.
            var pagedSearchL = searchExplorationFactor;
            var pageSize = pagedSearchL;

            // Allocate page buffers for each page of results from DiskANN
            var pageIdsSize = pageSize * MinimumSpacePerId;
            var pageDistsSize = pageSize * sizeof(float);
            var pageIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageIdsSize), pageIdsSize);
            var pageDists = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageDistsSize), pageDistsSize);

            // Reusable attribute buffer for pages (sized for pageSize, will grow if needed)
            var pageAttrsSize = pageSize * 64;
            var pageAttrs = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageAttrsSize), pageAttrsSize);

            // Accumulation buffers for passing results
            var accumIdsSize = requestedCount * MinimumSpacePerId;
            var accumDistsSize = requestedCount * sizeof(float);
            var accumAttrsSize = requestedCount * 64;

            // Ensure output buffers are large enough for the final results
            if (accumIdsSize > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumIdsSize), accumIdsSize);
            }

            if (accumDistsSize > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumDistsSize), accumDistsSize);
            }

            if (includeAttributes && accumAttrsSize > outputAttributes.Length)
            {
                if (!outputAttributes.IsSpanByte)
                {
                    outputAttributes.Memory.Dispose();
                }

                outputAttributes = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumAttrsSize), accumAttrsSize);
            }

            nint searchState = 0;
            try
            {
                // Start the paged search with the reduced L value
                searchState = isVector
                    ? Service.StartPagedSearchVector(context, indexPtr, valueType, values, pagedSearchL)
                    : Service.StartPagedSearchElement(context, indexPtr, element, pagedSearchL);

                if (searchState == 0)
                {
                    logger?.LogWarning("Failed to start paged search");
                    outputIdFormat = VectorIdFormat.Invalid;
                    return VectorManagerResult.BadParams;
                }

                var totalPassing = initialPassingCount;
                var accumIdPos = 0;
                var accumAttrPos = 0;

                // If we already have results from the static over-fetch, compute where we are in the output buffers
                if (initialPassingCount > 0)
                {
                    var idsSpan = outputIds.AsReadOnlySpan();
                    for (var i = 0; i < initialPassingCount; i++)
                    {
                        var idLen = BinaryPrimitives.ReadInt32LittleEndian(idsSpan[accumIdPos..]);
                        accumIdPos += sizeof(int) + idLen;
                    }

                    if (includeAttributes)
                    {
                        var attrsSpan = outputAttributes.AsReadOnlySpan();
                        for (var i = 0; i < initialPassingCount; i++)
                        {
                            var attrLen = BinaryPrimitives.ReadInt32LittleEndian(attrsSpan[accumAttrPos..]);
                            accumAttrPos += sizeof(int) + attrLen;
                        }
                    }
                }

                for (var page = 0; page < MaxPagedSearchPages; page++)
                {
                    // Reset page buffer lengths for this page
                    pageIds.Length = pageIdsSize;
                    pageDists.Length = pageDistsSize;

                    var pageFound = Service.NextPagedSearchResults(
                        context, indexPtr, searchState, pageSize, pageIds, pageDists);

                    if (pageFound <= 0)
                    {
                        break;
                    }

                    // Reset reusable attribute buffer for this page
                    if (pageAttrs.Memory != null)
                    {
                        pageAttrs.Length = pageAttrs.Memory.Memory.Length;
                    }
                    else
                    {
                        pageAttrs.Length = pageAttrsSize;
                    }

                    FetchVectorElementAttributes(context, pageFound, pageIds, ref pageAttrs);

                    // Apply pre-parsed filter to this page
                    var filteredCount = ApplyPostFilter(filterExpr, pageFound, ref pageIds, ref pageDists, ref pageAttrs);

                    if (filteredCount > 0)
                    {
                        // How many to take from this page (don't exceed requested count)
                        var toTake = Math.Min(filteredCount, requestedCount - totalPassing);

                        // Copy passing results into accumulation (output) buffers
                        AppendToAccumulator(
                            toTake, filteredCount,
                            pageIds, pageDists, pageAttrs,
                            ref outputIds, ref outputDistances, ref outputAttributes,
                            includeAttributes,
                            ref accumIdPos, totalPassing, ref accumAttrPos);

                        totalPassing += toTake;
                    }

                    if (totalPassing >= requestedCount)
                    {
                        break;
                    }
                }

                outputIds.Length = accumIdPos;
                outputDistances.Length = totalPassing * sizeof(float);
                if (includeAttributes)
                {
                    outputAttributes.Length = accumAttrPos;
                }

                outputIdFormat = VectorIdFormat.I32LengthPrefixed;

                if (quantType == VectorQuantType.XPreQ8)
                {
                    outputIdFormat = VectorIdFormat.I32LengthPrefixed;
                }

                return VectorManagerResult.OK;
            }
            finally
            {
                if (searchState != 0)
                {
                    Service.DropPagedSearchState(searchState);
                }

                pageIds.Memory?.Dispose();
                pageDists.Memory?.Dispose();
                pageAttrs.Memory?.Dispose();
            }
        }

        /// <summary>
        /// Append filtered results from a page into the accumulation output buffers.
        /// </summary>
        private static void AppendToAccumulator(
            int toTake,
            int filteredCount,
            SpanByteAndMemory pageIds,
            SpanByteAndMemory pageDists,
            SpanByteAndMemory pageAttrs,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            bool includeAttributes,
            ref int accumIdPos,
            int accumDistPos,
            ref int accumAttrPos)
        {
            var srcIdSpan = pageIds.AsReadOnlySpan();
            var srcDistSpan = MemoryMarshal.Cast<byte, float>(pageDists.AsReadOnlySpan());
            var srcAttrSpan = pageAttrs.AsReadOnlySpan();

            var srcIdPos = 0;
            var srcAttrPos = 0;

            for (var i = 0; i < toTake && i < filteredCount; i++)
            {
                // Read source ID
                var idLen = BinaryPrimitives.ReadInt32LittleEndian(srcIdSpan[srcIdPos..]);
                var idTotalLen = sizeof(int) + idLen;

                // Ensure output IDs buffer has space
                if (accumIdPos + idTotalLen > outputIds.Length)
                {
                    var newSize = Math.Max(outputIds.Length * 2, accumIdPos + idTotalLen);
                    var newBuf = MemoryPool<byte>.Shared.Rent(newSize);
                    outputIds.AsReadOnlySpan()[..accumIdPos].CopyTo(newBuf.Memory.Span);
                    outputIds.Memory?.Dispose();
                    outputIds = new SpanByteAndMemory(newBuf, newSize);
                }

                srcIdSpan.Slice(srcIdPos, idTotalLen).CopyTo(outputIds.AsSpan()[accumIdPos..]);
                accumIdPos += idTotalLen;

                // Copy distance
                var destDistSpan = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());
                destDistSpan[accumDistPos + i] = srcDistSpan[i];

                // Copy attributes if needed
                if (includeAttributes)
                {
                    var attrLen = BinaryPrimitives.ReadInt32LittleEndian(srcAttrSpan[srcAttrPos..]);
                    var attrTotalLen = sizeof(int) + attrLen;

                    if (accumAttrPos + attrTotalLen > outputAttributes.Length)
                    {
                        var newSize = Math.Max(outputAttributes.Length * 2, accumAttrPos + attrTotalLen);
                        var newBuf = MemoryPool<byte>.Shared.Rent(newSize);
                        outputAttributes.AsReadOnlySpan()[..accumAttrPos].CopyTo(newBuf.Memory.Span);
                        outputAttributes.Memory?.Dispose();
                        outputAttributes = new SpanByteAndMemory(newBuf, newSize);
                    }

                    srcAttrSpan.Slice(srcAttrPos, attrTotalLen).CopyTo(outputAttributes.AsSpan()[accumAttrPos..]);
                    accumAttrPos += attrTotalLen;
                    srcAttrPos += attrTotalLen;
                }

                srcIdPos += idTotalLen;
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
            ref SpanByteAndMemory outputAttributes)
        {
            if (numResults == 0)
            {
                return numResults;
            }

            var filterStr = Encoding.UTF8.GetString(filter);

            try
            {
                // Parse the filter expression once, then evaluate per result
                var tokens = VectorFilterTokenizer.Tokenize(filterStr);
                var filterExpr = VectorFilterParser.ParseExpression(tokens, 0, out var endIndex);

                // Ensure the entire token stream was consumed by the parser
                if (endIndex != tokens.Count)
                {
                    throw new ArgumentException("Invalid filter expression: unexpected tokens after end of expression.", nameof(filter));
                }

                return ApplyPostFilter(filterExpr, numResults, ref outputIds, ref outputDistances, ref outputAttributes);
            }
            catch (Exception ex) when (ex is ArgumentException || ex is FormatException || ex is InvalidOperationException)
            {
                throw new ArgumentException("Invalid filter expression.", nameof(filter), ex);
            }
        }

        /// <summary>
        /// Apply post-filtering using a pre-parsed filter expression.
        /// </summary>
        private int ApplyPostFilter(
            Expr filterExpr,
            int numResults,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes)
        {
            if (numResults == 0)
            {
                return numResults;
            }

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
                if (EvaluateFilter(filterExpr, attrData))
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
        /// Evaluate a pre-parsed filter expression against attribute data.
        /// Uses a fast path for simple binary comparisons (e.g., .year > 2000, .genre == "action")
        /// that avoids JsonDocument allocation by scanning with Utf8JsonReader directly.
        /// </summary>
        private static bool EvaluateFilter(Expr filterExpr, ReadOnlySpan<byte> attributeJson)
        {
            try
            {
                // Fast path for simple binary comparisons: MemberExpr op LiteralExpr
                if (filterExpr is BinaryExpr binary
                    && binary.Left is MemberExpr member
                    && binary.Right is LiteralExpr literal)
                {
                    return EvaluateSimpleComparison(attributeJson, member.Property, binary.Operator, literal.Value);
                }

                var reader = new Utf8JsonReader(attributeJson);
                using var jsonDoc = JsonDocument.ParseValue(ref reader);
                var root = jsonDoc.RootElement;
                var result = VectorFilterEvaluator.EvaluateExpression(filterExpr, root);

                return VectorFilterEvaluator.IsTruthy(result);
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException)
            {
                // If filter evaluation fails (malformed JSON or invalid expression), exclude the result
                Trace.TraceWarning("Vector filter evaluation failed: {0}", ex);
                return false;
            }
        }

        /// <summary>
        /// Fast evaluation of simple binary comparisons (MemberExpr op LiteralExpr)
        /// using Utf8JsonReader directly, avoiding JsonDocument allocation.
        /// </summary>
        private static bool EvaluateSimpleComparison(ReadOnlySpan<byte> json, string propertyName, string op, object literalValue)
        {
            var reader = new Utf8JsonReader(json);

            // Advance past the start object token
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return false;

            // Scan for the target property
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return false; // Property not found

                if (reader.TokenType != JsonTokenType.PropertyName)
                    continue;

                if (!reader.ValueTextEquals(propertyName))
                {
                    // Skip the value of this property
                    reader.Skip();
                    continue;
                }

                // Found the target property, read its value
                if (!reader.Read())
                    return false;

                if (literalValue is double literalDouble)
                {
                    // Numeric comparison
                    if (reader.TokenType != JsonTokenType.Number || !reader.TryGetDouble(out var propValue))
                        return false;

                    return op switch
                    {
                        ">" => propValue > literalDouble,
                        "<" => propValue < literalDouble,
                        ">=" => propValue >= literalDouble,
                        "<=" => propValue <= literalDouble,
                        "==" => Math.Abs(propValue - literalDouble) < 0.0001,
                        "!=" => Math.Abs(propValue - literalDouble) >= 0.0001,
                        _ => false
                    };
                }

                if (literalValue is string literalString)
                {
                    // String comparison
                    if (reader.TokenType != JsonTokenType.String)
                        return false;

                    var propValue = reader.GetString();
                    return op switch
                    {
                        "==" => propValue == literalString,
                        "!=" => propValue != literalString,
                        _ => false
                    };
                }

                return false;
            }

            return false;
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}