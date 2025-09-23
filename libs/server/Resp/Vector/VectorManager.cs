// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
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
        private readonly Channel<VADDReplicationState> replicationReplayChannel;

        [ThreadStatic]
        private static StorageSession ActiveThreadSession;

        public VectorManager()
        {
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            replicationReplayChannel.Writer.Complete();
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
            VectorInput input = new((byte)context);
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

        private static bool WriteCallbackManaged(ulong context, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = new((byte)context);
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

            Debug.Assert(indexSpan.Length == Index.Size, "Insufficient space for index");

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
            Debug.Assert(indexValue.Length == Index.Size, "Index size is incorrect, implies vector set index is probably corrupted");

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            context = asIndex.Context;
            dimensions = asIndex.Dimensions;
            reduceDims = asIndex.ReduceDims;
            quantType = asIndex.QuantType;
            buildExplorationFactor = asIndex.BuildExplorationFactor;
            numLinks = asIndex.NumLinks;
            indexPtr = (nint)asIndex.IndexPtr;

            Debug.Assert((context % 4) == 0, "Context not as expected, vector set index is probably corrupted");
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
                    errorMsg = Encoding.ASCII.GetBytes($"ERR Input dimension mismatch for projection - got {valueDims} but projection expects {dimensions}");
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
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances
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
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances
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
            Debug.Assert(input.header.cmd == RespCommand.VADD, "Shouldn't be called with anything but VADD inputs");

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
            var queued = replicationReplayChannel.Writer.TryWrite(new(keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks));
            Debug.Assert(queued);

            static void StartReplicationReplayTasks(VectorManager self, Func<RespServerSession> obtainServerSession)
            {
                // TODO: Pull this off a config or something
                for (var i = 0; i < Environment.ProcessorCount; i++)
                {
                    _ = Task.Factory.StartNew(
                        async () =>
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
                                    _ = Interlocked.Decrement(ref self.replicationReplayPendingVAdds);
                                }
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

                        // Equivalent to VectorStoreOps.VectorSetAdd, except with no locking or formatting
                        while (true)
                        {
                            var readStatus = context.Read(ref key, ref input, ref indexConfig);
                            if (readStatus.IsPending)
                            {
                                CompletePending(ref readStatus, ref indexConfig, ref context);
                            }

                            if (!readStatus.Found)
                            {
                                // Create the vector set index
                                var writeStatus = context.RMW(ref key, ref input);
                                if (writeStatus.IsPending)
                                {
                                    CompletePending(ref writeStatus, ref indexConfig, ref context);
                                }

                                if (!writeStatus.IsCompletedSuccessfully)
                                {
                                    throw new GarnetException("Fail to create a vector set index during AOF sync, this should never happen but will break all ops against this vector set if it does");
                                }
                            }
                            else
                            {
                                break;
                            }
                        }

                        var addRes = self.TryAdd(storageSession, indexConfig.AsReadOnlySpan(), element.AsReadOnlySpan(), valueType, values.AsReadOnlySpan(), attributes.AsReadOnlySpan(), reduceDims, quantizer, buildExplorationFactor, numLinks, out _);
                        if (addRes != VectorManagerResult.OK)
                        {
                            throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
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
            while (Interlocked.CompareExchange(ref replicationReplayPendingVAdds, 0, 0) != 0)
            {
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Determine the dimensions of a vector given its <see cref="VectorValueType"/> and its raw data.
        /// </summary>
        private static uint CalculateValueDimensions(VectorValueType valueType, ReadOnlySpan<byte> values)
        {
            if (valueType == VectorValueType.F32)
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