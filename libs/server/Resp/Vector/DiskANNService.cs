using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    internal sealed unsafe class DiskANNService
    {
        // Term types.
        internal const byte FullVector = 0;
        private const byte NeighborList = 1;
        private const byte QuantizedVector = 2;
        internal const byte Attributes = 3;
        private const byte Metadata = 4;
        internal const byte InternalIdMap = 5;
        private const byte ExternalIdMap = 6;

        public nint CreateIndex(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            VectorDistanceMetricType distanceMetric,
            delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> readCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nuint, nint, nint, byte> readModifyWriteCallback
        )
        {
            // TODO: actually pass distance metric

            unsafe
            {
                return NativeDiskANNMethods.create_index(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, (nint)readCallback, (nint)writeCallback, (nint)deleteCallback, (nint)readModifyWriteCallback);
            }
        }

        public nint RecreateIndex(
            ulong context,
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            VectorDistanceMetricType distanceMetricType,
            delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> readCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> writeCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> deleteCallback,
            delegate* unmanaged[Cdecl]<ulong, nint, nuint, nuint, nint, nint, byte> readModifyWriteCallback
        )
        => CreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetricType, readCallback, writeCallback, deleteCallback, readModifyWriteCallback);

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

        public bool CheckInternalIdValid(ulong context, nint index, ReadOnlySpan<byte> internalId)
        {
            var internal_id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(internalId));
            var internal_id_len = internalId.Length;

            return NativeDiskANNMethods.check_internal_id_valid(context, index, (nint)internal_id_data, (nuint)internal_id_len) == 1;
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
            nint deleteCallback,
            nint readModifyWriteCallback
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
        public static partial ulong card(
            ulong context,
            nint index
        );

        [LibraryImport(DISKANN_GARNET)]
        public static partial byte check_internal_id_valid(
            ulong context,
            nint index,
            nint internal_id,
            nuint internal_id_len
        );
    }
}