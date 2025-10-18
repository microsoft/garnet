// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
    public sealed class VectorManager : IDisposable
    {
        internal const int IndexSizeBytes = Index.Size;
        internal const long VADDAppendLogArg = long.MinValue;
        internal const long DeleteAfterDropArg = VADDAppendLogArg + 1;
        internal const long RecreateIndexArg = DeleteAfterDropArg + 1;

        public unsafe struct VectorReadBatch : IReadArgBatch<SpanByte, VectorInput, SpanByte>
        {
            public int Count { get; }

            private ILogger logger;

            private readonly ulong context;
            private readonly SpanByte lengthPrefixedKeys;

            public readonly unsafe delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            private int currentIndex;

            private int currentLen;
            private byte* currentPtr;

            private bool hasPending;

            public VectorReadBatch(delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback, nint callbackContext, ulong context, uint keyCount, SpanByte lengthPrefixedKeys, ILogger logger = null)
            {
                this.context = context;
                this.lengthPrefixedKeys = lengthPrefixedKeys;

                this.callback = callback;
                this.callbackContext = callbackContext;

                this.logger = logger;

                currentIndex = 0;
                Count = (int)keyCount;

                if (Count <= 0)
                {
                    logger?.LogCritical("Illegal batch size {Count}", Count);
                    throw new GarnetException($"Illegal batch size {Count}");
                }

                currentPtr = this.lengthPrefixedKeys.ToPointerWithMetadata();
                currentLen = *(int*)currentPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AdvanceTo(int i)
            {
                if (i < 0 || i >= Count)
                {
                    logger?.LogCritical("Tried to advance to {i}, while Count is {Count}", i, Count);
                    throw new GarnetException("Trying to advance out of bounds");
                }

                //Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                if (i == currentIndex)
                {
                    return;
                }

                // Undo namespace mutation
                *(int*)currentPtr = currentLen;

                // Most likely case, we're going one forward
                if (i == (currentIndex + 1))
                {
                    currentPtr += currentLen + sizeof(int); // Skip length prefix too

                    {
                        var bounds = lengthPrefixedKeys.AsSpanWithMetadata();
                        var start = (byte*)Unsafe.AsPointer(ref bounds[0]);
                        var end = start + bounds.Length;
                        if (currentPtr < start || currentPtr + sizeof(int) > end)
                        {
                            logger?.LogCritical("About to read out of bounds, start = {start}, end = {end}, currentPtr={currentPtr}", (nint)start, (nint)end, (nint)currentPtr);
                            throw new GarnetException("About to access out of bounds data");
                        }
                    }
                    //Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointerWithMetadata() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                    currentLen = *currentPtr;

                    currentIndex = i;

                    return;
                }

                // Next most likely case, we're going back to the start
                currentPtr = lengthPrefixedKeys.ToPointerWithMetadata();
                currentLen = *(int*)currentPtr;
                currentIndex = 0;

                if (i == 0)
                {
                    return;
                }

                SlowPath(ref this, i);

                // For the case where we're not just scanning or rolling back to 0, just iterate
                //
                // This should basically never happen
                [MethodImpl(MethodImplOptions.NoInlining)]
                static void SlowPath(ref VectorReadBatch self, int i)
                {
                    for (var subI = 1; subI <= i; subI++)
                    {
                        self.AdvanceTo(subI);
                    }
                }
            }

            /// <inheritdoc/>
            public void GetKey(int i, out SpanByte key)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                AdvanceTo(i);

                key = SpanByte.FromPinnedPointer(currentPtr + 3, currentLen + 1);
                key.MarkNamespace();
                key.SetNamespaceInPayload((byte)context);
            }

            /// <inheritdoc/>
            public readonly void GetInput(int i, out VectorInput input)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                input = default;
                input.CallbackContext = callbackContext;
                input.Callback = callback;
                input.Index = i;
            }

            /// <inheritdoc/>
            public readonly void GetOutput(int i, out SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                // Don't care, won't be used
                Unsafe.SkipInit(out output);
            }

            /// <inheritdoc/>
            public readonly void SetOutput(int i, SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");
            }

            /// <inheritdoc/>
            public void SetStatus(int i, Status status)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                hasPending |= status.IsPending;
            }

            internal readonly void CompletePending<TContext>(ref TContext objectContext)
                where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            {
                // Undo mutations
                *(int*)currentPtr = currentLen;

                if (hasPending)
                {
                    _ = objectContext.CompletePending(wait: true);
                }
            }
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Index
        {
            internal const int Size = 52;

            [FieldOffset(0)]
            public ulong Context;
            [FieldOffset(8)]
            public ulong IndexPtr;
            [FieldOffset(16)]
            public uint Dimensions;
            [FieldOffset(20)]
            public uint ReduceDims;
            [FieldOffset(24)]
            public uint NumLinks;
            [FieldOffset(28)]
            public uint BuildExplorationFactor;
            [FieldOffset(32)]
            public VectorQuantType QuantType;
            [FieldOffset(36)]
            public Guid ProcessInstanceId;
        }

        private readonly record struct VADDReplicationState(SpanByte Key, uint Dims, uint ReduceDims, VectorValueType ValueType, SpanByte Values, SpanByte Element, VectorQuantType Quantizer, uint BuildExplorationFactor, SpanByte Attributes, uint NumLinks)
        {
        }

        /// <summary>
        /// Used to scope a shared lock and context related to a Vector Set operation.
        /// 
        /// Disposing this ends the lockable context, releases the lock, and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct ReadVectorLock : IDisposable
        {
            private readonly ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx;
            private readonly TxnKeyEntry entry;

            internal ReadVectorLock(ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx, TxnKeyEntry entry)
            {
                this.entry = entry;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(ref lockableCtx))
                {
                    return;
                }

                lockableCtx.Unlock([entry]);
                lockableCtx.EndLockable();
            }
        }

        /// <summary>
        /// Used to scope exclusive locks and a context related to a Vector Set delete operation.
        /// 
        /// Disposing this ends the lockable context, releases the locks, and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct DeleteVectorLock : IDisposable
        {
            private readonly ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx;
            private readonly ReadOnlySpan<TxnKeyEntry> entries;

            internal DeleteVectorLock(ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx, ReadOnlySpan<TxnKeyEntry> entries)
            {
                this.entries = entries;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(ref lockableCtx))
                {
                    return;
                }

                lockableCtx.Unlock(entries);
                lockableCtx.EndLockable();
            }
        }

        /// <summary>
        /// Minimum size of an id is assumed to be at least 4 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 4;

        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;

        private DiskANNService Service { get; } = new DiskANNService();

        private readonly Guid processInstanceId = Guid.NewGuid();

        private ulong nextContextValue;

        private int replicationReplayStarted;
        private long replicationReplayPendingVAdds;
        private readonly ManualResetEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;

        [ThreadStatic]
        private static StorageSession ActiveThreadSession;

        private readonly ILogger logger;
        private static ILogger StaticLogger;

        internal readonly int readLockShardCount;
        private readonly long readLockShardMask;

        public VectorManager(ILogger logger)
        {
            replicationBlockEvent = new(true);
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });

            // TODO: Pull this off a config or something
            replicationReplayTasks = new Task[Environment.ProcessorCount];
            for (var i = 0; i < replicationReplayTasks.Length; i++)
            {
                replicationReplayTasks[i] = Task.CompletedTask;
            }

            this.logger = logger;
            StaticLogger ??= logger;

            // TODO: Probably configurable?
            // For now, nearest power of 2 >= process count;
            readLockShardCount = (int)BitOperations.RoundUpToPowerOf2((uint)Environment.ProcessorCount);
            readLockShardMask = readLockShardCount - 1;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // We must drain all these before disposing, otherwise we'll leave replicationBlockEvent unset
            replicationReplayChannel.Writer.Complete();
            replicationReplayChannel.Reader.Completion.Wait();

            Task.WhenAll(replicationReplayTasks).Wait();

            replicationBlockEvent.Dispose();
        }

        /// <summary>
        /// Get a new unique context for a vector set.
        /// 
        /// This value is guaranteed to not be shared by any other vector set in the store.
        /// </summary>
        /// <returns></returns>
        private ulong NextContext()
        {
            // TODO: how do we avoid creating a context that is already present in the log?

            while (true)
            {
                var ret = Interlocked.Add(ref nextContextValue, 4);

                // 0 is special, don't return it (even if we wrap around)
                if (ret == 0)
                {
                    continue;
                }

                return ret;
            }
        }

        /// <summary>
        /// For testing purposes.
        /// </summary>
        public ulong HighestContext()
        => nextContextValue;

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void ReadCallbackUnmanaged(
            ulong context,
            uint numKeys,
            nint keysData,
            nuint keysLength,
            nint dataCallback,
            nint dataCallbackContext
        )
        {
            // Takes: index, dataCallbackContext, data pointer, data length, and returns nothing
            var dataCallbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)dataCallback;

            var enumerable = new VectorReadBatch(dataCallbackDel, dataCallbackContext, context, numKeys, SpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), StaticLogger);

            ref var ctx = ref ActiveThreadSession.vectorContext;

            ctx.ReadWithPrefetch(ref enumerable);

            enumerable.CompletePending(ref ctx);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = default;
            var valueSpan = SpanByte.FromPinnedPointer((byte*)writeData, (int)writeLength);
            SpanByte outputSpan = default;

            var status = ctx.Upsert(ref keyWithNamespace, ref input, ref valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        private static unsafe bool WriteCallbackManaged(ulong context, ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
        {
            // TODO: this whole method goes away once DiskANN is setting attributes
            Span<byte> keySpace = stackalloc byte[sizeof(int) + key.Length];
            key.CopyTo(keySpace[sizeof(int)..]);

            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, (nint)Unsafe.AsPointer(ref keySpace[sizeof(int)]), (nuint)key.Length);

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = default;
            var valueSpan = SpanByte.FromPinnedSpan(data);
            SpanByte outputSpan = default;

            var status = ctx.Upsert(ref keyWithNamespace, ref input, ref valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;

            var status = ctx.Delete(ref keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;

        tryAgain:
            VectorInput input = new();
            input.ReadDesiredSize = -1;
            fixed (byte* ptr = value.AsSpan())
            {
                SpanByte asSpanByte = new(value.Length, (nint)ptr);

                var status = ctx.Read(ref keyWithNamespace, ref input, ref asSpanByte);
                if (status.IsPending)
                {
                    CompletePending(ref status, ref asSpanByte, ref ctx);
                }

                if (!status.Found)
                {
                    value.Length = 0;
                    return false;
                }

                if (input.ReadDesiredSize > asSpanByte.Length)
                {
                    value.Memory?.Dispose();
                    var newAlloc = MemoryPool<byte>.Shared.Rent(input.ReadDesiredSize);
                    value = new(newAlloc, newAlloc.Memory.Length);
                    goto tryAgain;
                }

                value.Length = asSpanByte.Length;
                return true;
            }
        }

        /// <summary>
        /// Get a <see cref="SpanByte"/> which covers (keyData, keyLength), but has a namespace component based on <paramref name="context"/>.
        /// 
        /// Attempts to do this in place.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe SpanByte MarkDiskANNKeyWithNamespace(ulong context, nint keyData, nuint keyLength)
        {
            // DiskANN guarantees we have 4-bytes worth of unused data right before the key
            var keyPtr = (byte*)keyData;
            var keyNamespaceByte = keyPtr - 1;

            // TODO: if/when namespace can be > 4-bytes, we'll need to copy here

            var keyWithNamespace = SpanByte.FromPinnedPointer(keyNamespaceByte, (int)(keyLength + 1));
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);

            return keyWithNamespace;
        }

        private static void CompletePending<TContext>(ref Status status, ref SpanByte output, ref TContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            objectContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            Debug.Assert(!completedOutputs.Next());
            completedOutputs.Dispose();
        }

        /// <summary>
        /// Construct a new index, and stash enough data to recover it with <see cref="ReadIndex"/>.
        /// </summary>
        internal void CreateIndex(
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            ref SpanByte indexValue)
        {
            AssertHaveStorageSession();

            var context = NextContext();

            nint indexPtr;
            unsafe
            {
                indexPtr = Service.CreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
            }

            var indexSpan = indexValue.AsSpan();

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expections, {0} != {1}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expections, {indexSpan.Length} != {Index.Size}");
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.Context = context;
            asIndex.Dimensions = dimensions;
            asIndex.ReduceDims = reduceDims;
            asIndex.QuantType = quantType;
            asIndex.BuildExplorationFactor = buildExplorationFactor;
            asIndex.NumLinks = numLinks;
            asIndex.IndexPtr = (ulong)indexPtr;
            asIndex.ProcessInstanceId = processInstanceId;
        }

        /// <summary>
        /// Recreate an index that was created by a prior instance of Garnet.
        /// 
        /// This implies the index still has element data, but the pointer is garbage.
        /// </summary>
        internal void RecreateIndex(ref SpanByte indexValue)
        {
            AssertHaveStorageSession();

            var indexSpan = indexValue.AsSpan();

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expectations, {0} != {1}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expectations, {indexSpan.Length} != {Index.Size}");
            }

            ReadIndex(indexSpan, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out _, out var indexProcessInstanceId);
            Debug.Assert(processInstanceId != indexProcessInstanceId, "Should be recreating an index that matched our instance id");

            nint indexPtr;
            unsafe
            {
                indexPtr = Service.RecreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.IndexPtr = (ulong)indexPtr;
            asIndex.ProcessInstanceId = processInstanceId;
        }

        /// <summary>
        /// Drop an index previously constructed with <see cref="CreateIndex"/>.
        /// </summary>
        internal void DropIndex(ReadOnlySpan<byte> indexValue)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out var indexPtr, out var indexProcessInstanceId);

            if (indexProcessInstanceId != processInstanceId)
            {
                // We never actually spun this index up, so nothing to drop
                return;
            }

            Service.DropIndex(context, indexPtr);
        }

        internal static void ReadIndex(
            ReadOnlySpan<byte> indexValue,
            out ulong context,
            out uint dimensions,
            out uint reduceDims,
            out VectorQuantType quantType,
            out uint buildExplorationFactor,
            out uint numLinks,
            out nint indexPtr,
            out Guid processInstanceId
        )
        {
            if (indexValue.Length != Index.Size)
            {
                throw new GarnetException($"Index size is incorrect ({indexValue.Length} != {Index.Size}), implies vector set index is probably corrupted");
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            context = asIndex.Context;
            dimensions = asIndex.Dimensions;
            reduceDims = asIndex.ReduceDims;
            quantType = asIndex.QuantType;
            buildExplorationFactor = asIndex.BuildExplorationFactor;
            numLinks = asIndex.NumLinks;
            indexPtr = (nint)asIndex.IndexPtr;
            processInstanceId = asIndex.ProcessInstanceId;

            if ((context % 4) != 0)
            {
                throw new GarnetException($"Context ({context}) not as expected (% 4 == {context % 4}), vector set index is probably corrupted");
            }
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
            out ReadOnlySpan<byte> errorMsg
        )
        {
            AssertHaveStorageSession();

            errorMsg = default;

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr, out _);

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

            if (providedNumLinks != numLinks)
            {
                // Matching Redis behavior
                errorMsg = "ERR asked M value mismatch with existing vector set"u8;
                return VectorManagerResult.BadParams;
            }

            if (quantType == VectorQuantType.XPreQ8 && element.Length != sizeof(uint))
            {
                errorMsg = "ERR XPREQ8 requires 4-byte element ids"u8;
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
                // HACK HACK HACK
                // Once DiskANN is doing this, remove
                if (!attributes.IsEmpty)
                {
                    var res = WriteCallbackManaged(context | DiskANNService.Attributes, element, attributes);
                    if (!res)
                    {
                        throw new GarnetException($"Failed to insert attribute");
                    }
                }

                return VectorManagerResult.OK;
            }

            return VectorManagerResult.Duplicate;
        }

        internal VectorManagerResult TryRemove(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out var indexPtr, out _);

            if (quantType == VectorQuantType.XPreQ8 && element.Length != sizeof(int))
            {
                // We know this element isn't present because of other validation constraints, bail
                return VectorManagerResult.MissingElement;
            }

            var del = Service.Remove(context, indexPtr, element);

            return del ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
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

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr, out _);

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
                logger?.LogWarning("Error indicating response from vector service {0}", found);
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

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out var indexPtr, out _);

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
                logger?.LogWarning("Error indicating response from vector service {0}", found);
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

        internal bool TryGetEmbedding(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out _, out _, out _, out var indexPtr, out _);

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

            Span<byte> asBytesSpan = stackalloc byte[(int)dimensions];
            var asBytes = SpanByteAndMemory.FromPinnedSpan(asBytesSpan);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.FullVector, element, ref asBytes))
                {
                    return false;
                }

                var from = asBytes.AsReadOnlySpan();
                var into = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());

                for (var i = 0; i < asBytes.Length; i++)
                {
                    into[i] = from[i];
                }

                return true;
            }
            finally
            {
                asBytes.Memory?.Dispose();
            }

            // TODO: DiskANN will need to do this long term, since different quantizers may behave differently

            //return
            //    Service.TryGetEmbedding(
            //        context,
            //        indexPtr,
            //        element,
            //        MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan())
            //    );
        }

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetAddReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetAdd<TContext>(ref SpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            if (input.header.cmd != RespCommand.VADD)
            {
                throw new GarnetException($"Shouldn't be called with anything but VADD inputs, found {input.header.cmd}");
            }

            // Temp 
            if (input.SerializedLength > 1_024)
            {
                logger?.LogCritical("RawStringInput is suspiciously large, {length} - {input}", input.SerializedLength, input);
            }

            if (key.Length > 1_024)
            {
                logger?.LogCritical("Key is suspiciously large, {length} - {key}", key.Length, key);
            }

            var inputCopy = input;
            inputCopy.arg1 = VectorManager.VADDAppendLogArg;

            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0); // 0 namespace is special, only used for replication
            key.AsReadOnlySpan().CopyTo(keyWithNamespace.AsSpan());

            Span<byte> dummyBytes = stackalloc byte[4];
            var dummy = SpanByteAndMemory.FromPinnedSpan(dummyBytes);

            var res = context.RMW(ref keyWithNamespace, ref inputCopy, ref dummy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref dummy, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VADD into log, result was {0}", res);
                throw new GarnetException("Couldn't synthesize Vector Set add operation for replication, data loss will occur");
            }

            // Helper to complete read/writes during vector set synthetic op goes async
            static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref TContext context)
            {
                _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                more = completedOutputs.Next();
                Debug.Assert(!more);
                completedOutputs.Dispose();
            }
        }

        /// <summary>
        /// After an index is dropped, called to cleanup state injected by <see cref="ReplicateVectorSetAdd"/>
        /// 
        /// Amounts to delete a synthetic key in namespace 0.
        /// </summary>
        internal void DropVectorSetReplicationKey<TContext>(SpanByte key, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0);
            key.AsReadOnlySpan().CopyTo(keyWithNamespace.AsSpan());

            Span<byte> dummyBytes = stackalloc byte[4];
            var dummy = SpanByteAndMemory.FromPinnedSpan(dummyBytes);

            var res = context.Delete(ref keyWithNamespace);

            if (res.IsPending)
            {
                CompletePending(ref res, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                throw new GarnetException("Couldn't synthesize Vector Set add operation for replication, data loss will occur");
            }

            // Helper to complete read/writes during vector set synthetic op goes async
            static void CompletePending(ref Status status, ref TContext context)
            {
                _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                more = completedOutputs.Next();
                Debug.Assert(!more);
                completedOutputs.Dispose();
            }
        }

        /// <summary>
        /// Vector Set adds are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetAdd"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetAddReplication(Func<RespServerSession> obtainServerSession, ref SpanByte keyWithNamespace, ref RawStringInput input)
        {
            // Undo mangling that got replication going, but without copying
            SpanByte key;
            unsafe
            {
                key = SpanByte.FromPinnedPointer(keyWithNamespace.ToPointer(), keyWithNamespace.LengthWithoutMetadata);
            }

            var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
            var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
            var valueType = MemoryMarshal.Read<VectorValueType>(input.parseState.GetArgSliceByRef(2).Span);
            var values = input.parseState.GetArgSliceByRef(3).SpanByte;
            var element = input.parseState.GetArgSliceByRef(4).SpanByte;
            var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
            var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
            var attributes = input.parseState.GetArgSliceByRef(7).SpanByte;
            var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);

            // Spin up replication replay tasks on first use
            if (replicationReplayStarted == 0)
            {
                if (Interlocked.CompareExchange(ref replicationReplayStarted, 1, 0) == 0)
                {
                    StartReplicationReplayTasks(this, obtainServerSession);
                }
            }

            // We need a running count of pending VADDs so WaitForVectorOperationsToComplete can work
            var cur = Interlocked.Increment(ref replicationReplayPendingVAdds);
            Debug.Assert(cur > 0, "Pending VADD ops is incoherent");

            replicationBlockEvent.Reset();
            var queued = replicationReplayChannel.Writer.TryWrite(new(key, dims, reduceDims, valueType, values, element, quantizer, buildExplorationFactor, attributes, numLinks));
            if (!queued)
            {
                logger?.LogInformation("Replay of VADD against {0} dropped during shutdown", Encoding.UTF8.GetString(key.AsReadOnlySpan()));

                // Can occur if we're being Disposed
                var pending = Interlocked.Decrement(ref replicationReplayPendingVAdds);
                Debug.Assert(pending >= 0, "Pending VADD ops has fallen below 0 during shutdown");

                if (pending == 0)
                {
                    replicationBlockEvent.Set();
                }
            }

            static void StartReplicationReplayTasks(VectorManager self, Func<RespServerSession> obtainServerSession)
            {
                self.logger?.LogInformation("Starting {0} replication tasks for VADDs", self.replicationReplayTasks.Length);

                for (var i = 0; i < self.replicationReplayTasks.Length; i++)
                {
                    self.replicationReplayTasks[i] = Task.Factory.StartNew(
                        async () =>
                        {
                            try
                            {
                                var reader = self.replicationReplayChannel.Reader;

                                using var session = obtainServerSession();

                                SessionParseState reusableParseState = default;
                                reusableParseState.Initialize(9);

                                await foreach (var entry in reader.ReadAllAsync())
                                {
                                    try
                                    {
                                        try
                                        {
                                            ApplyVectorSetAdd(self, session.storageSession, entry, ref reusableParseState);
                                        }
                                        finally
                                        {
                                            var pending = Interlocked.Decrement(ref self.replicationReplayPendingVAdds);
                                            Debug.Assert(pending >= 0, "Pending VADD ops has fallen below 0 after processing op");

                                            if (pending == 0)
                                            {
                                                self.replicationBlockEvent.Set();
                                            }
                                        }
                                    }
                                    catch
                                    {
                                        unsafe
                                        {
                                            self.logger?.LogCritical("Faulting ApplyVectorSetAdd Key: {KeyWithNamespace}", *entry.KeyWithNamespace);
                                            for (var i = 0; i < entry.Input.parseState.Count; i++)
                                            {
                                                self.logger?.LogCritical("Faulting ApplySetAdd Arg #{i}: {val}", i, entry.Input.parseState.GetArgSliceByRef(i).SpanByte);
                                            }
                                        }

                                        throw;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                self.logger?.LogCritical(e, "Unexpected abort of replication replay task");
                                throw;
                            }
                        }
                    );
                }
            }

            // Actually apply a replicated VADD
            static unsafe void ApplyVectorSetAdd(VectorManager self, StorageSession storageSession, VADDReplicationState state, ref SessionParseState reusableParseState)
            {
                ref var context = ref storageSession.basicContext;

                var (key, dims, reduceDims, valueType, values, element, quantizer, buildExplorationFactor, attributes, numLinks) = state;

                Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

                var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
                var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
                var valuesArg = ArgSlice.FromPinnedSpan(values.AsReadOnlySpan());
                var elementArg = ArgSlice.FromPinnedSpan(element.AsReadOnlySpan());
                var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
                var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                var attributesArg = ArgSlice.FromPinnedSpan(attributes.AsReadOnlySpan());
                var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

                reusableParseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

                var input = new RawStringInput(RespCommand.VADD, ref reusableParseState);

                // Equivalent to VectorStoreOps.VectorSetAdd
                //
                // We still need locking here because the replays may proceed in parallel

                using (self.ReadOrCreateVectorIndex(storageSession, ref key, ref input, indexSpan, out var status))
                {
                    var addRes = self.TryAdd(indexSpan, element.AsReadOnlySpan(), valueType, values.AsReadOnlySpan(), attributes.AsReadOnlySpan(), reduceDims, quantizer, buildExplorationFactor, numLinks, out _);

                    if (addRes != VectorManagerResult.OK)
                    {
                        throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
                    }
                }
            }
        }

        /// <summary>
        /// Returns true for indexes that were created via a previous instance of <see cref="VectorManager"/>.
        /// 
        /// Such indexes still have element data, but the index pointer to the DiskANN bits are invalid.
        /// </summary>
        internal bool NeedsRecreate(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var indexProcessInstanceId);

            return indexProcessInstanceId != processInstanceId;
        }

        /// <summary>
        /// Utility method that will read an vector set index out but not create one.
        /// 
        /// It will however RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal ReadVectorLock ReadVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            PrepareReadLockHash(storageSession, ref key, out var keyHash, out var readLockHash);

            Span<TxnKeyEntry> sharedLocks = stackalloc TxnKeyEntry[1];
            scoped Span<TxnKeyEntry> exclusiveLocks = default;

            ref var readLockEntry = ref sharedLocks[0];
            readLockEntry.isObject = false;
            readLockEntry.keyHash = readLockHash;
            readLockEntry.lockType = LockType.Shared;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            while (true)
            {
                input.arg1 = 0;

                lockCtx.Lock([readLockEntry]);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock([readLockEntry]);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && NeedsRecreate(indexConfig.AsReadOnlySpan());

                if (needsRecreate)
                {
                    if (exclusiveLocks.IsEmpty)
                    {
                        exclusiveLocks = stackalloc TxnKeyEntry[readLockShardCount];
                    }

                    if (!TryAcquireExclusiveLocks(storageSession, exclusiveLocks, keyHash, readLockHash))
                    {
                        // All locks will have been released by here
                        continue;
                    }

                    input.arg1 = RecreateIndexArg;

                    GarnetStatus writeRes;
                    try
                    {
                        writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    }
                    catch
                    {
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while performing a search
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        continue;
                    }
                    else
                    {
                        status = writeRes;
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    status = readRes;
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    return default;
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, readLockEntry);
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, create one if it doesn't exist, or RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal ReadVectorLock ReadOrCreateVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            this.PrepareReadLockHash(storageSession, ref key, out var keyHash, out var readLockHash);

            Span<TxnKeyEntry> sharedLocks = stackalloc TxnKeyEntry[1];
            scoped Span<TxnKeyEntry> exclusiveLocks = default;

            ref var readLockEntry = ref sharedLocks[0];
            readLockEntry.isObject = false;
            readLockEntry.keyHash = readLockHash;
            readLockEntry.lockType = LockType.Shared;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            while (true)
            {
                input.arg1 = 0;

                lockCtx.Lock<TxnKeyEntry>(sharedLocks);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && storageSession.vectorManager.NeedsRecreate(indexSpan);
                if (readRes == GarnetStatus.NOTFOUND || needsRecreate)
                {
                    if (exclusiveLocks.IsEmpty)
                    {
                        exclusiveLocks = stackalloc TxnKeyEntry[readLockShardCount];
                    }

                    if (!TryAcquireExclusiveLocks(storageSession, exclusiveLocks, keyHash, readLockHash))
                    {
                        // All locks will have been released by here
                        continue;
                    }

                    if (needsRecreate)
                    {
                        input.arg1 = RecreateIndexArg;
                    }

                    GarnetStatus writeRes;
                    try
                    {
                        writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    }
                    catch
                    {
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        continue;
                    }
                    else
                    {
                        status = writeRes;

                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    status = readRes;
                    return default;
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, readLockEntry);
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        internal DeleteVectorLock ReadForDeleteVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, Span<TxnKeyEntry> exclusiveLocks, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Insufficient space for exclusive locks");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var keyHash = storageSession.lockableContext.GetKeyHash(key);

            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                exclusiveLocks[i].isObject = false;
                exclusiveLocks[i].lockType = LockType.Exclusive;
                exclusiveLocks[i].keyHash = (keyHash & ~readLockShardMask) | (long)i;
            }

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            lockCtx.Lock<TxnKeyEntry>(exclusiveLocks);

            // Get the index
            try
            {
                status = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
            }
            catch
            {
                lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                lockCtx.EndLockable();

                throw;
            }

            if (status != GarnetStatus.OK)
            {
                // This can happen is something else successfully deleted before we acquired the lock

                lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                lockCtx.EndLockable();
                return default;
            }

            return new(ref lockCtx, exclusiveLocks);
        }

        /// <summary>
        /// Wait until all ops passed to <see cref="HandleVectorSetAddReplication"/> have completed.
        /// </summary>
        public void WaitForVectorOperationsToComplete()
        {
            try
            {
                replicationBlockEvent.Wait();
            }
            catch (ObjectDisposedException)
            {
                // This is possible during dispose
                //
                // Dispose already takes pains to drain everything before disposing, so this is safe to ignore
            }
        }

        private void PrepareReadLockHash(StorageSession storageSession, ref SpanByte key, out long keyHash, out long readLockHash)
        {
            var id = Thread.GetCurrentProcessorId() & readLockShardMask;

            keyHash = storageSession.basicContext.GetKeyHash(ref key);
            readLockHash = (keyHash & ~readLockShardMask) | id;
        }

        private bool TryAcquireExclusiveLocks(StorageSession storageSession, Span<TxnKeyEntry> exclusiveLocks, long keyHash, long readLockHash)
        {
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Insufficient space for exclusive locks");

            // When we start, we still hold a SHARED lock on readLockHash

            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                exclusiveLocks[i].isObject = false;
                exclusiveLocks[i].lockType = LockType.Shared;
                exclusiveLocks[i].keyHash = (keyHash & ~readLockShardMask) | (long)i;
            }

            AssertSorted(exclusiveLocks);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;

            TxnKeyEntry toUnlock = default;
            toUnlock.keyHash = readLockHash;
            toUnlock.isObject = false;
            toUnlock.lockType = LockType.Shared;

            if (!lockCtx.TryLock<TxnKeyEntry>(exclusiveLocks))
            {
                // We don't hold any new locks, but still have the old SHARED lock

                lockCtx.Unlock([toUnlock]);
                return false;
            }

            // Drop down to just 1 shared lock per id
            lockCtx.Unlock([toUnlock]);

            // Attempt to promote
            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                if (!lockCtx.TryPromoteLock(exclusiveLocks[i]))
                {
                    lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                    return false;
                }

                exclusiveLocks[i].lockType = LockType.Exclusive;
            }

            return true;

            [Conditional("DEBUG")]
            static void AssertSorted(ReadOnlySpan<TxnKeyEntry> locks)
            {
                for (var i = 1; i < locks.Length; i++)
                {
                    Debug.Assert(locks[i - 1].keyHash <= locks[i].keyHash, "Locks should be naturally sorted, but weren't");
                }
            }
        }

        /// <summary>
        /// Helper to complete read/writes during vector set op replay that go async.
        /// </summary>
        private static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> context)
        {
            _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
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