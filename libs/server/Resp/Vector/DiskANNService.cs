using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{

    /// <summary>
    /// For passing multiple Span-like values at once with well defined layout and offset on the native side.
    /// 
    /// Struct is 16 bytes for alignment purposes, although only 13 are used at maximum.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public readonly struct PointerLengthPair
    {
        /// <summary>
        /// Pointer to a memory chunk.
        /// </summary>
        [FieldOffset(0)]
        public readonly nint Pointer;

        /// <summary>
        /// Length of a memory chunk, in whatever units were intended.
        /// </summary>
        [FieldOffset(8)]
        public readonly uint Length;

        /// <summary>
        /// Size of an individual unit in the <see cref="PointerLengthPair"/>.
        /// For example, if we're storing bytes this is 1, floats this is 4, doubles this is 8, etc.
        /// </summary>
        [FieldOffset(12)]
        public readonly byte UnitSizeBytes;

        private unsafe PointerLengthPair(void* pointer, uint length, byte unitSize)
        {
            Pointer = (nint)pointer;
            Length = length;
        }

        /// <summary>
        /// Create a <see cref="PointerLengthPair"/> from a byte Span.
        /// </summary>
        public static unsafe PointerLengthPair From(ReadOnlySpan<byte> data)
        => new(Unsafe.AsPointer(ref MemoryMarshal.GetReference(data)), (uint)data.Length, sizeof(byte));

        /// <summary>
        /// Create a <see cref="PointerLengthPair"/> from a float Span.
        /// </summary>
        public static unsafe PointerLengthPair From(ReadOnlySpan<float> data)
        => new(Unsafe.AsPointer(ref MemoryMarshal.GetReference(data)), (uint)data.Length, sizeof(float));

        /// <summary>
        /// Convert this <see cref="PointerLengthPair"/> into a Span of bytes.
        /// </summary>
        public readonly unsafe Span<byte> AsByteSpan()
        {
            Debug.Assert(UnitSizeBytes == sizeof(byte), "Incompatible conversion");
            return MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((void*)Pointer), (int)Length);
        }

        /// <summary>
        /// Convert this <see cref="PointerLengthPair"/> into a Span of floats.
        /// </summary>
        public readonly unsafe Span<float> AsFloatSpan()
        {
            Debug.Assert(UnitSizeBytes == sizeof(float), "Incompatible conversion");
            return MemoryMarshal.CreateSpan(ref Unsafe.AsRef<float>((void*)Pointer), (int)Length);
        }
    }

    internal sealed unsafe class DiskANNService
    {
        private static readonly bool UseMultiInsertCallback = false;

        // Term types.
        internal const byte FullVector = 0;
        private const byte NeighborList = 1;
        private const byte QuantizedVector = 2;
        internal const byte Attributes = 3;

        public nint CreateIndex(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> readCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback
        )
        {
            unsafe
            {
                return NativeDiskANNMethods.create_index(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, (nint)readCallback, (nint)writeCallback, (nint)deleteCallback);
            }
        }

        public nint RecreateIndex(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> readCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback
        )
        => CreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, readCallback, writeCallback, deleteCallback);

        public void DropIndex(ulong context, nint index)
        {
            NativeDiskANNMethods.drop_index(context, index);
        }

        public bool Insert(ulong context, nint index, ReadOnlySpan<byte> id, VectorValueType vectorType, ReadOnlySpan<byte> vector, ReadOnlySpan<byte> attributes)
        {
            var id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(id));
            var id_len = id.Length;

            var vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(vector));
            int vector_len;

            if (vectorType == VectorValueType.FP32)
            {
                vector_len = vector.Length / sizeof(float);
            }
            else if (vectorType == VectorValueType.XB8)
            {
                vector_len = vector.Length;
            }
            else
            {
                throw new NotImplementedException($"{vectorType}");
            }

            var attributes_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(attributes));
            var attributes_len = attributes.Length;

            return NativeDiskANNMethods.insert(context, index, (nint)id_data, (nuint)id_len, vectorType, (nint)vector_data, (nuint)vector_len, (nint)attributes_data, (nuint)attributes_len) == 1;
        }

        public bool Remove(ulong context, nint index, ReadOnlySpan<byte> id)
        {
            var id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(id));
            var id_len = id.Length;

            return NativeDiskANNMethods.remove(context, index, (nint)id_data, (nuint)id_len) == 1;
        }

        public void MultiInsert(ulong context, nint index, ReadOnlySpan<PointerLengthPair> ids, VectorValueType vectorType, ReadOnlySpan<PointerLengthPair> vectors, ReadOnlySpan<PointerLengthPair> attributes, Span<bool> insertSuccess)
        {
            if (UseMultiInsertCallback)
            {
                var ids_data = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(ids));
                var ids_len = (nuint)ids.Length;

                nint vectors_data;
                nuint vectors_len;

                float[] rentedTempData = null;
                try
                {
                    Span<float> tempData = vectorType == VectorValueType.XB8 ? stackalloc float[128] : default;
                    Span<PointerLengthPair> temp = vectorType == VectorValueType.XB8 ? stackalloc PointerLengthPair[vectors.Length] : default;
                    if (vectorType == VectorValueType.FP32)
                    {
                        vectors_data = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(vectors));
                        vectors_len = (nuint)vectors.Length;
                    }
                    else
                    {
                        var vectorLength = vectors[0].Length;

                        // TODO: Eventually DiskANN will just take this directly, for now map to floats
                        var neededFloatSpace = (int)(ids.Length * vectorLength);
                        if (tempData.Length < neededFloatSpace)
                        {
                            rentedTempData = ArrayPool<float>.Shared.Rent(neededFloatSpace);
                            tempData = rentedTempData;
                        }

                        tempData = tempData[..neededFloatSpace];
                        var remainingTempData = tempData;

                        for (var i = 0; i < vectors.Length; i++)
                        {
                            var asBytes = vectors[i].AsByteSpan();
                            Debug.Assert(asBytes.Length == vectorLength, "All vectors should have same length for insertion");

                            var floatEquiv = remainingTempData[..asBytes.Length];
                            for (var j = 0; j < asBytes.Length; j++)
                            {
                                floatEquiv[j] = asBytes[j];
                            }

                            temp[i] = PointerLengthPair.From(floatEquiv);

                            remainingTempData = remainingTempData[asBytes.Length..];
                        }

                        vectors_data = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(temp));
                        vectors_len = (nuint)temp.Length;
                    }

                    var attributes_data = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(attributes));
                    var attributes_len = (nuint)attributes.Length;

                    // These are treated as bytes on the Rust side
                    var insert_success_data = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(insertSuccess));
                    var insert_success_len = (nuint)insertSuccess.Length;

                    NativeDiskANNMethods.multi_insert(context, index, ids_data, ids_len, vectors_data, vectors_len, attributes_data, attributes_len, insert_success_data, insert_success_len);
                }
                finally
                {
                    if (rentedTempData != null)
                    {
                        ArrayPool<float>.Shared.Return(rentedTempData);
                    }
                }
            }
            else
            {
                for (var i = 0; i < ids.Length; i++)
                {
                    insertSuccess[i] = Insert(context, index, ids[i].AsByteSpan(), vectorType, vectors[i].AsByteSpan(), attributes[i].AsByteSpan());
                }
            }
        }

        public int SearchVector(
            ulong context,
            nint index,
            VectorValueType vectorType,
            ReadOnlySpan<byte> vector,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            Span<byte> outputIds,
            Span<float> outputDistances,
            out nint continuation
        )
        {
            var vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(vector));
            int vector_len;

            if (vectorType == VectorValueType.FP32)
            {
                vector_len = vector.Length / sizeof(float);
            }
            else if (vectorType == VectorValueType.XB8)
            {
                vector_len = vector.Length;
            }
            else
            {
                throw new NotImplementedException($"{vectorType}");
            }

            var filter_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(filter));
            var filter_len = filter.Length;

            var output_ids = Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputIds));
            var output_ids_len = outputIds.Length;

            var output_distances = Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputDistances));
            var output_distances_len = outputDistances.Length;


            continuation = 0;
            ref var continuationRef = ref continuation;
            var continuationAddr = (nint)Unsafe.AsPointer(ref continuationRef);

            return NativeDiskANNMethods.search_vector(
                context,
                index,
                vectorType,
                (nint)vector_data,
                (nuint)vector_len,
                delta,
                searchExplorationFactor,
                (nint)filter_data,
                (nuint)filter_len,
                (nuint)maxFilteringEffort,
                (nint)output_ids,
                (nuint)output_ids_len,
                (nint)output_distances,
                (nuint)output_distances_len,
                continuationAddr
            );
        }

        public int SearchElement(
            ulong context,
            nint index,
            ReadOnlySpan<byte> id,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            Span<byte> outputIds,
            Span<float> outputDistances,
            out nint continuation
        )
        {
            var id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(id));
            var id_len = id.Length;

            var filter_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(filter));
            var filter_len = filter.Length;

            var output_ids = Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputIds));
            var output_ids_len = outputIds.Length;

            var output_distances = Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputDistances));
            var output_distances_len = outputDistances.Length;

            continuation = 0;
            ref var continuationRef = ref continuation;
            var continuationAddr = (nint)Unsafe.AsPointer(ref continuationRef);

            return NativeDiskANNMethods.search_element(
                context,
                index,
                (nint)id_data,
                (nuint)id_len,
                delta,
                searchExplorationFactor,
                (nint)filter_data,
                (nuint)filter_len,
                (nuint)maxFilteringEffort,
                (nint)output_ids,
                (nuint)output_ids_len,
                (nint)output_distances,
                (nuint)output_distances_len,
                continuationAddr
            );
        }

        public int ContinueSearch(ulong context, nint index, nint continuation, Span<byte> outputIds, Span<float> outputDistances, out nint newContinuation)
        {
            throw new NotImplementedException();
        }

        public bool TryGetEmbedding(ulong context, nint index, ReadOnlySpan<byte> id, Span<float> dimensions)
        {
            throw new NotImplementedException();
        }
    }

    public static partial class NativeDiskANNMethods
    {
        const string DISKANN_GARNET = "diskann_garnet";

        [LibraryImport(DISKANN_GARNET)]
        public static partial nint create_index(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            nint readCallback,
            nint writeCallback,
            nint deleteCallback
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial void drop_index(
            ulong context,
            nint index
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial byte insert(
            ulong context,
            nint index,
            nint id_data,
            nuint id_len,
            VectorValueType vector_value_type,
            nint vector_data,
            nuint vector_len,
            nint attribute_data,
            nuint attribute_len
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial byte remove(
            ulong context,
            nint index,
            nint id_data,
            nuint id_len
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial void multi_insert(
            ulong context,
            nint index,
            nint ids_data,
            nuint ids_len,
            nint vectors_data,
            nuint vectors_len,
            nint attributes_data,
            nuint attributes_len,
            nint insert_success_data,
            nuint insert_success_len
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial byte set_attribute(
            ulong context,
            nint index,
            nint id_data,
            nuint id_len,
            nint attribute_data,
            nuint attribute_len
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial int search_vector(
            ulong context,
            nint index,
            VectorValueType vector_value_type,
            nint vector_data,
            nuint vector_len,
            float delta,
            int search_exploration_factor,
            nint filter_data,
            nuint filter_len,
            nuint max_filtering_effort,
            nint output_ids,
            nuint output_ids_len,
            nint output_distances,
            nuint output_distances_len,
            nint continuation
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial int search_element(
            ulong context,
            nint index,
            nint id_data,
            nuint id_len,
            float delta,
            int search_exploration_factor,
            nint filter_data,
            nuint filter_len,
            nuint max_filtering_effort,
            nint output_ids,
            nuint output_ids_len,
            nint output_distances,
            nuint output_distances_len,
            nint continuation
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial int continue_search(
            ulong context,
            nint index,
            nint continuation,
            nint output_ids,
            nuint output_ids_len,
            nint output_distances,
            nuint output_distances_len,
            nint new_continuation
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial byte delete(
            ulong context,
            nint index,
            nint vector_data,
            nuint vector_data_len
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial ulong card(
            ulong context,
            nint index
        );
    }
}