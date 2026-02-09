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
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Methods for managing an implementation of various vector operations.
    /// </summary>
    public sealed partial class VectorManager : IDisposable
    {
        internal const byte VectorSetRecordType = 1;

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

            Span<byte> key = stackalloc byte[1];
            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

            //key.MarkNamespace();
            //key.SetNamespaceInPayload(0);

            var data = PinnedSpanByte.FromPinnedSpan(dataSpan);

            ref var ctx = ref session.storageSession.vectorContext;

            var status = ctx.Read(key, ref data);

            if (status.IsPending)
            {
                CompletePending(ref status, ref data, ref ctx);
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
            // TODO: Move implementation over
            //throw new NotImplementedException();
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

        private static void CompletePending(ref Status status, ref PinnedSpanByte output, ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx)
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

            if (quantType == VectorQuantType.XPreQ8 && element.Length != sizeof(int))
            {
                // We know this element isn't present because of other validation constraints, bail
                return VectorManagerResult.MissingElement;
            }

            var del = Service.Remove(context, indexPtr, element);

            return del ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        internal Status TryDeleteVectorSet(StorageSession storageSession, ref PinnedSpanByte key, out GarnetStatus status)
        {
            storageSession.parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VADD, ref storageSession.parseState);

            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

            using (ReadForDeleteVectorIndex(storageSession, ref key, ref input, indexSpan, out status))
            {
                if (status != GarnetStatus.OK)
                {
                    // This can happen is something else successfully deleted before we acquired the lock
                    return Status.CreateNotFound();
                }

                ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out _);

                if (!TryMarkDeleteInProgress(ref storageSession.vectorContext, key, context))
                {
                    // We can't recover from a crash or error, so fail the delete for safety
                    return Status.CreateError();
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

                // Update the index to be delete-able
                StringInput updateToDroppableVectorSet = new(RespCommand.VADD, arg1: DeleteAfterDropArg);

                var update = storageSession.stringBasicContext.RMW(key.ReadOnlySpan, ref updateToDroppableVectorSet);
                if (!update.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                }

                // Drop the native side of the index now - we can't fault between the two unless the process is torn down
                DropIndex(indexSpan);

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

                // Actually delete the value
                var del = storageSession.stringBasicContext.Delete(key.ReadOnlySpan);
                if (!del.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to delete dropped Vector Set, this should never happen but will leave vector sets corrupted");
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

                // Cleanup incidental additional state
                if (!TryDropVectorSetReplicationKey(key, ref storageSession.stringBasicContext))
                {
                    logger?.LogCritical("Couldn't synthesize Vector Set delete operation for replication, data loss will occur");
                }

                // Schedule cleanup of element data
                CleanupDroppedIndex(ref storageSession.vectorContext, context);

                // Delete has finished, so remove the in progress metadata
                //
                // A crash or error before this will cause some work to be retried, but no correctness issues
                ClearDeleteInProgress(ref storageSession.vectorContext, key, context);

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
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
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

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
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
            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out _, out _, out _);
            var found = ReadSizeUnknown(context | DiskANNService.Attributes, element.ReadOnlySpan, ref outputAttributes);
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

                var from = asBytes.ReadOnlySpan;
                var into = MemoryMarshal.Cast<byte, float>(outputDistances.Span);

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

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}