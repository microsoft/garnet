// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
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

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Index
        {
            internal const int Size = 33;

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
        }

        private readonly record struct VADDReplicationState(Memory<byte> Key, uint Dims, uint ReduceDims, VectorValueType ValueType, Memory<byte> Values, Memory<byte> Element, VectorQuantType Quantizer, uint BuildExplorationFactor, Memory<byte> Attributes, uint NumLinks)
        {
        }

        /// <summary>
        /// Minimum size of an id is assumed to be at least 4 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 4;

        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, int> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;

        private VectorReadDelegate ReadCallbackDel { get; } = ReadCallbackManaged;
        private VectorWriteDelegate WriteCallbackDel { get; } = WriteCallbackManaged;
        private VectorDeleteDelegate DeleteCallbackDel { get; } = DeleteCallbackManaged;

        private IVectorService Service { get; } = new DiskANNService();

        private ulong nextContextValue;

        private int replicationReplayStarted;
        private long replicationReplayPendingVAdds;
        private readonly ManualResetEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;

        [ThreadStatic]
        private static StorageSession ActiveThreadSession;

        private readonly ILogger logger;

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
        private static unsafe int ReadCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        => ReadCallbackManaged(context, MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)keyData), (int)keyLength), MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((void*)writeData), (int)writeLength));

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        => WriteCallbackManaged(context, MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)keyData), (int)keyLength), MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)writeData), (int)writeLength)) ? (byte)1 : default;

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        => DeleteCallbackManaged(context, MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)keyData), (int)keyLength)) ? (byte)1 : default;

        private static int ReadCallbackManaged(ulong context, ReadOnlySpan<byte> key, Span<byte> value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = new();
            input.ReadDesiredSize = value.Length;
            var outputSpan = SpanByte.FromPinnedSpan(value);

            var status = ctx.Read(ref keyWithNamespace, ref input, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            if (status.Found)
            {
                return outputSpan.Length;
            }

            return 0;
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

        private static bool WriteCallbackManaged(ulong context, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = new();
            var valueSpan = SpanByte.FromPinnedSpan(value);
            SpanByte outputSpan = default;

            var status = ctx.Upsert(ref keyWithNamespace, ref input, ref valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully;
        }

        private static bool DeleteCallbackManaged(ulong context, ReadOnlySpan<byte> key)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;

            var status = ctx.Delete(ref keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully;
        }

        /// <summary>
        /// Mutate <paramref name="key"/> so that the same value with different <paramref name="context"/>'s won't clobber each other.
        /// </summary>
        public static void DistinguishVectorElementKey(ulong context, ReadOnlySpan<byte> key, ref Span<byte> distinguishedKey, out byte[] rented)
        {
            if (key.Length + sizeof(byte) > distinguishedKey.Length)
            {
                distinguishedKey = rented = ArrayPool<byte>.Shared.Rent(key.Length + sizeof(byte));
                distinguishedKey = distinguishedKey[..^(key.Length + sizeof(byte))];
            }
            else
            {
                rented = null;
                distinguishedKey = distinguishedKey[..(key.Length + sizeof(byte))];
            }

            key.CopyTo(distinguishedKey);

            var suffix = (byte)(0b1100_0000 | (byte)context);
            distinguishedKey[^1] = suffix;
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
            uint buildExplorationFactory,
            uint numLinks,
            ref SpanByte indexValue)
        {
            var context = NextContext();

            nint indexPtr;
            if (Service.UseUnmanagedCallbacks)
            {
                unsafe
                {
                    indexPtr = Service.CreateIndexUnmanaged(context, dimensions, reduceDims, quantType, buildExplorationFactory, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
                }
            }
            else
            {
                indexPtr = Service.CreateIndexManaged(context, dimensions, reduceDims, quantType, buildExplorationFactory, numLinks, ReadCallbackDel, WriteCallbackDel, DeleteCallbackDel);
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
            asIndex.BuildExplorationFactor = buildExplorationFactory;
            asIndex.NumLinks = numLinks;
            asIndex.IndexPtr = (ulong)indexPtr;
        }

        /// <summary>
        /// Drop an index previously constructed with <see cref="CreateIndex"/>.
        /// </summary>
        internal void DropIndex(StorageSession currentStorageSession, ReadOnlySpan<byte> indexValue)
        {
            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out var indexPtr);

            ActiveThreadSession = currentStorageSession;
            try
            {
                Service.DropIndex(context, indexPtr);
            }
            finally
            {
                ActiveThreadSession = null;
            }
        }

        internal static void ReadIndex(
            ReadOnlySpan<byte> indexValue,
            out ulong context,
            out uint dimensions,
            out uint reduceDims,
            out VectorQuantType quantType,
            out uint buildExplorationFactor,
            out uint numLinks,
            out nint indexPtr
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
            StorageSession currentStorageSession,
            ReadOnlySpan<byte> indexValue,
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
            errorMsg = default;

            ActiveThreadSession = currentStorageSession;
            try
            {
                ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr);

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
            finally
            {
                ActiveThreadSession = null;
            }
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ValueSimilarity(
            StorageSession currentStorageSession,
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
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            ActiveThreadSession = currentStorageSession;
            try
            {
                ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr);

                var valueDims = CalculateValueDimensions(valueType, values);
                if (dimensions != valueDims)
                {
                    return VectorManagerResult.BadParams;
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

                return VectorManagerResult.OK;
            }
            finally
            {
                ActiveThreadSession = null;
            }
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ElementSimilarity(
            StorageSession currentStorageSession,
            ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            ActiveThreadSession = currentStorageSession;
            try
            {
                ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr);

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

                return VectorManagerResult.OK;
            }
            finally
            {
                ActiveThreadSession = null;
            }
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

        internal bool TryGetEmbedding(StorageSession currentStorageSession, ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            ActiveThreadSession = currentStorageSession;
            try
            {
                ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr);

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

                return
                    Service.TryGetEmbedding(
                        context,
                        indexPtr,
                        element,
                        MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan())
                    );
            }
            finally
            {
                ActiveThreadSession = null;
            }
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
        internal void ReplicateVectorSetAdd<TContext>(SpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            if (input.header.cmd != RespCommand.VADD)
            {
                throw new GarnetException($"Shouldn't be called with anything but VADD inputs, found {input.header.cmd}");
            }

            var inputCopy = input;
            inputCopy.arg1 = VectorManager.VADDAppendLogArg;

            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0);
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
            // Undo mangling that got replication going
            var inputCopy = input;
            inputCopy.arg1 = default;
            var keyBytesArr = ArrayPool<byte>.Shared.Rent(keyWithNamespace.Length - 1);
            var keyBytes = keyBytesArr.AsMemory()[..(keyWithNamespace.Length - 1)];

            keyWithNamespace.AsReadOnlySpan().CopyTo(keyBytes.Span);

            var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
            var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
            var valueType = MemoryMarshal.Read<VectorValueType>(input.parseState.GetArgSliceByRef(2).Span);
            var values = input.parseState.GetArgSliceByRef(3).Span;
            var element = input.parseState.GetArgSliceByRef(4).Span;
            var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
            var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
            var attributes = input.parseState.GetArgSliceByRef(7).Span;
            var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);

            // We have to make copies (and they need to be on the heap) to pass to background tasks
            var valuesBytes = ArrayPool<byte>.Shared.Rent(values.Length).AsMemory()[..values.Length];
            values.CopyTo(valuesBytes.Span);

            var elementBytes = ArrayPool<byte>.Shared.Rent(element.Length).AsMemory()[..element.Length];
            element.CopyTo(elementBytes.Span);

            var attributesBytes = ArrayPool<byte>.Shared.Rent(attributes.Length).AsMemory()[..attributes.Length];
            attributes.CopyTo(attributesBytes.Span);

            // Spin up replication replay tasks on first use
            if (replicationReplayStarted == 0)
            {
                if (Interlocked.CompareExchange(ref replicationReplayStarted, 1, 0) == 0)
                {
                    StartReplicationReplayTasks(this, obtainServerSession);
                }
            }

            // We need a running count of pending VADDs so WaitForVectorOperationsToComplete can work
            _ = Interlocked.Increment(ref replicationReplayPendingVAdds);
            replicationBlockEvent.Reset();
            var queued = replicationReplayChannel.Writer.TryWrite(new(keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks));
            if (!queued)
            {
                // Can occur if we're being Disposed
                var pending = Interlocked.Decrement(ref replicationReplayPendingVAdds);
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

                                await foreach (var entry in reader.ReadAllAsync())
                                {
                                    try
                                    {
                                        ApplyVectorSetAdd(self, session.storageSession, entry);
                                    }
                                    finally
                                    {
                                        var pending = Interlocked.Decrement(ref self.replicationReplayPendingVAdds);
                                        if (pending == 0)
                                        {
                                            self.replicationBlockEvent.Set();
                                        }
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
            static unsafe void ApplyVectorSetAdd(VectorManager self, StorageSession storageSession, VADDReplicationState state)
            {
                ref var context = ref storageSession.basicContext;

                var (keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks) = state;

                try
                {
                    fixed (byte* keyPtr = keyBytes.Span)
                    fixed (byte* valuesPtr = valuesBytes.Span)
                    fixed (byte* elementPtr = elementBytes.Span)
                    fixed (byte* attributesPtr = attributesBytes.Span)
                    {
                        var key = SpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                        var values = SpanByte.FromPinnedPointer(valuesPtr, valuesBytes.Length);
                        var element = SpanByte.FromPinnedPointer(elementPtr, elementBytes.Length);
                        var attributes = SpanByte.FromPinnedPointer(attributesPtr, attributesBytes.Length);

                        Span<byte> indexBytes = stackalloc byte[128];
                        var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexBytes);

                        var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
                        var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                        var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
                        var valuesArg = ArgSlice.FromPinnedSpan(values.AsReadOnlySpan());
                        var elementArg = ArgSlice.FromPinnedSpan(element.AsReadOnlySpan());
                        var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
                        var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                        var attributesArg = ArgSlice.FromPinnedSpan(attributes.AsReadOnlySpan());
                        var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

                        var parseState = default(SessionParseState);
                        parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

                        var input = new RawStringInput(RespCommand.VADD, ref parseState);

                        // Equivalent to VectorStoreOps.VectorSetAdd
                        //
                        // We still need locking here because the replays may proceed in parallel

                        var lockCtx = storageSession.objectStoreLockableContext;

                        var loggedWarning = false;
                        var loggedCritical = false;
                        var start = Stopwatch.GetTimestamp();

                        lockCtx.BeginLockable();
                        try
                        {
                            TxnKeyEntry vectorLockEntry = new();
                            vectorLockEntry.isObject = false;
                            vectorLockEntry.keyHash = storageSession.lockableContext.GetKeyHash(key);

                            // Ensure creation of the index, leaving indexBytes populated
                            // and a Shared lock acquired by the time we exit
                            while (true)
                            {
                                vectorLockEntry.lockType = LockType.Shared;
                                lockCtx.Lock([vectorLockEntry]);

                                var readStatus = context.Read(ref key, ref input, ref indexConfig);
                                if (readStatus.IsPending)
                                {
                                    CompletePending(ref readStatus, ref indexConfig, ref context);
                                }

                                if (!readStatus.Found)
                                {
                                    if (!lockCtx.TryPromoteLock(vectorLockEntry))
                                    {
                                        // Try again
                                        lockCtx.Unlock([vectorLockEntry]);
                                        continue;
                                    }

                                    vectorLockEntry.lockType = LockType.Exclusive;

                                    // Create the vector set index
                                    var writeStatus = context.RMW(ref key, ref input);
                                    if (writeStatus.IsPending)
                                    {
                                        CompletePending(ref writeStatus, ref indexConfig, ref context);
                                    }

                                    if (!writeStatus.IsCompletedSuccessfully)
                                    {
                                        lockCtx.Unlock([vectorLockEntry]);
                                        throw new GarnetException("Fail to create a vector set index during AOF sync, this should never happen but will break all ops against this vector set if it does");
                                    }
                                }
                                else
                                {
                                    break;
                                }

                                lockCtx.Unlock([vectorLockEntry]);

                                var timeAttempting = Stopwatch.GetElapsedTime(start);
                                if (!loggedWarning && timeAttempting > TimeSpan.FromSeconds(5))
                                {
                                    self.logger?.LogWarning("Long duration {0} attempting to apply VADD", timeAttempting);
                                    loggedWarning = true;
                                }
                                else if (!loggedCritical && timeAttempting > TimeSpan.FromSeconds(30))
                                {
                                    self.logger?.LogCritical("VERY long duration {0} attempting to apply VADD", timeAttempting);
                                    loggedCritical = true;
                                }
                            }

                            if (vectorLockEntry.lockType != LockType.Shared)
                            {
                                self.logger?.LogCritical("Held exclusive lock when adding to vector set during replication, should never happen");
                                throw new GarnetException("Held exclusive lock when adding to vector set during replication, should never happen");
                            }

                            var addRes = self.TryAdd(storageSession, indexConfig.AsReadOnlySpan(), element.AsReadOnlySpan(), valueType, values.AsReadOnlySpan(), attributes.AsReadOnlySpan(), reduceDims, quantizer, buildExplorationFactor, numLinks, out _);
                            lockCtx.Unlock([vectorLockEntry]);

                            if (addRes != VectorManagerResult.OK)
                            {
                                throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
                            }
                        }
                        finally
                        {
                            lockCtx.EndLockable();
                        }
                    }
                }
                finally
                {
                    if (MemoryMarshal.TryGetArray<byte>(keyBytes, out var toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(valuesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(elementBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(attributesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }
                }
            }

            // Helper to complete read/writes during vector set op replay that go async
            static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> context)
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
        /// Wait until all ops passed to <see cref="HandleVectorSetAddReplication"/> have completed.
        /// </summary>
        internal void WaitForVectorOperationsToComplete()
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

        /// <summary>
        /// Determine the dimensions of a vector given its <see cref="VectorValueType"/> and its raw data.
        /// </summary>
        private static uint CalculateValueDimensions(VectorValueType valueType, ReadOnlySpan<byte> values)
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
    }
}