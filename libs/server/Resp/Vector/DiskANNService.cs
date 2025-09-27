using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    internal sealed unsafe class DiskANNService : IVectorService
    {
        private static readonly bool UseMultiInsertCallback = false;

        // Term types.
        private const byte FullVector = 0;
        private const byte NeighborList = 1;
        private const byte QuantizedVector = 2;
        internal const byte Attributes = 3;

        public bool UseUnmanagedCallbacks { get; } = true;

        public nint CreateIndexManaged(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            VectorReadDelegate readCallback,
            VectorWriteDelegate writeCallback,
            VectorDeleteDelegate deleteCallback
        )
        {
            throw new NotImplementedException();
        }

        public nint CreateIndexUnmanaged(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, int> readCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback
        )
        {
            unsafe
            {
                return NativeDiskANNMethods.create_index(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, (nint)readCallback, (nint)writeCallback, (nint)deleteCallback);
            }
        }

        public void DropIndex(ulong context, nint index)
        {
            NativeDiskANNMethods.drop_index(context, index);
        }

        public bool Insert(ulong context, nint index, ReadOnlySpan<byte> id, VectorValueType vectorType, ReadOnlySpan<byte> vector, ReadOnlySpan<byte> attributes)
        {
            var id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(id));
            var id_len = id.Length;

            void* vector_data;
            int vector_len;

            Span<float> temp = vectorType == VectorValueType.XB8 ? stackalloc float[vector.Length] : default;
            if (vectorType == VectorValueType.FP32)
            {
                vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(vector));
                vector_len = vector.Length / sizeof(float);
            }
            else if (vectorType == VectorValueType.XB8)
            {
                // TODO: Eventually DiskANN will just take this directly, for now map to a float
                for (var i = 0; i < vector.Length; i++)
                {
                    temp[i] = vector[i];
                }

                vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(temp));
                vector_len = temp.Length;
            }
            else
            {
                throw new NotImplementedException($"{vectorType}");
            }

            var attributes_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(attributes));
            var attributes_len = attributes.Length;

            return NativeDiskANNMethods.insert(context, index, (nint)id_data, (nuint)id_len, (nint)vector_data, (nuint)vector_len, (nint)attributes_data, (nuint)attributes_len) == 1;
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
            void* vector_data;
            int vector_len;

            Span<float> temp = vectorType == VectorValueType.XB8 ? stackalloc float[vector.Length] : default;
            if (vectorType == VectorValueType.FP32)
            {
                vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(vector));
                vector_len = vector.Length / sizeof(float);
            }
            else if (vectorType == VectorValueType.XB8)
            {
                // TODO: Eventually DiskANN will just take this directly, for now map to a float
                for (var i = 0; i < vector.Length; i++)
                {
                    temp[i] = vector[i];
                }

                vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(temp));
                vector_len = temp.Length;
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
            return NativeDiskANNMethods.search_vector(
                context,
                index,
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
                (nuint)output_distances_len, continuation
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
            return NativeDiskANNMethods.search_vector(
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
                (nuint)output_distances_len, continuation
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
            nint vector_data,
            nuint vector_len,
            nint attribute_data,
            nuint attribute_len
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