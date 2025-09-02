// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
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
    public sealed class VectorManager
    {
        internal const int IndexSizeBytes = Index.Size;

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

        [ThreadStatic]
        private static StorageSession ActiveThreadSession;

        /// <summary>
        /// Get a new unique context for a vector set.
        /// 
        /// This value is guaranteed to not be shared by any other vector set in the store.
        /// </summary>
        /// <returns></returns>
        private ulong NextContext()
        {
            var ret = Interlocked.Add(ref nextContextValue, 4);

            Debug.Assert(ret != 0, "0 is special, cannot use it as vector set context");

            return ret;
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
            Span<byte> distinctKey = stackalloc byte[128];
            DistinguishVectorElementKey(context, key, ref distinctKey, out var rentedBuffer);

            try
            {
                ref var ctx = ref ActiveThreadSession.vectorContext;
                var keySpan = SpanByte.FromPinnedSpan(distinctKey);
                VectorInput input = new();
                var outputSpan = SpanByte.FromPinnedSpan(value);

                var status = ctx.Read(ref keySpan, ref input, ref outputSpan);
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
            finally
            {
                if (rentedBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
        }

        private static bool WriteCallbackManaged(ulong context, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            Span<byte> distinctKey = stackalloc byte[128];
            DistinguishVectorElementKey(context, key, ref distinctKey, out var rentedBuffer);

            try
            {
                ref var ctx = ref ActiveThreadSession.vectorContext;
                var keySpan = SpanByte.FromPinnedSpan(distinctKey);
                VectorInput input = new();
                var valueSpan = SpanByte.FromPinnedSpan(value);
                SpanByte outputSpan = default;

                var status = ctx.Upsert(ref keySpan, ref input, ref valueSpan, ref outputSpan);
                if (status.IsPending)
                {
                    CompletePending(ref status, ref outputSpan, ref ctx);
                }

                return status.IsCompletedSuccessfully;
            }
            finally
            {
                if (rentedBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
        }

        private static bool DeleteCallbackManaged(ulong context, ReadOnlySpan<byte> key)
        {
            Span<byte> distinctKey = stackalloc byte[128];
            DistinguishVectorElementKey(context, key, ref distinctKey, out var rentedBuffer);

            try
            {
                ref var ctx = ref ActiveThreadSession.vectorContext;
                var keySpan = SpanByte.FromPinnedSpan(distinctKey);

                var status = ctx.Delete(ref keySpan);
                Debug.Assert(!status.IsPending, "Deletes should never go async");

                return status.IsCompletedSuccessfully;
            }
            finally
            {
                if (rentedBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
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

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue.AsSpan()));
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
            uint providedNumLinks
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

                if (providedReduceDims != 0 && providedReduceDims != reduceDims)
                {
                    return VectorManagerResult.BadParams;
                }

                if (providedQuantType != VectorQuantType.Invalid && providedQuantType != quantType)
                {
                    return VectorManagerResult.BadParams;
                }

                if (providedBuildExplorationFactor != 0 && providedBuildExplorationFactor != buildExplorationFactor)
                {
                    return VectorManagerResult.BadParams;
                }

                if (providedNumLinks != 0 && providedNumLinks != numLinks)
                {
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
        /// Returns true if the key (as found in main store) is somehow related to some Vector Set.
        /// </summary>
        internal static bool IsVectorSetRelatedKey(ReadOnlySpan<byte> keyInStore)
        => !keyInStore.IsEmpty && (keyInStore[^1] > 0b1100_0000);

        /// <summary>
        /// If a key going into the main store would be interpreted as a Vector Set (via <see cref="IsVectorSetRelatedKey"/>) key,
        /// mangles it so that it no longer will.
        /// 
        /// This is unsafe because it ASSUMES there's an extra free byte at the end
        /// of the key.
        /// </summary>
        internal static unsafe void UnsafeMangleMainKey(ref ArgSlice rawKey)
        {
            if (!IsVectorSetRelatedKey(rawKey.ReadOnlySpan))
            {
                return;
            }

            *(rawKey.ptr + rawKey.length) = 0b1100_0000;
            rawKey.length++;

            Debug.Assert(!IsVectorSetRelatedKey(rawKey.ReadOnlySpan), "Mangling did not work");
            return;
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