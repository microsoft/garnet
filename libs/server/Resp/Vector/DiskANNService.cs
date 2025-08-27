using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    internal sealed unsafe class DiskANNService : IVectorService
    {
        // Term types.
        private const byte FullVector = 0;
        private const byte NeighborList = 1;
        private const byte QuantizedVector = 2;
        private const byte Attributes = 3;

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

        public bool Insert(ulong context, nint index, ReadOnlySpan<byte> id, ReadOnlySpan<float> vector, ReadOnlySpan<byte> attributes)
        {
            var id_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(id));
            var id_len = id.Length;

            var vector_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(vector));
            var vector_len = vector.Length;

            var attributes_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(attributes));
            var attributes_len = attributes.Length;

            return NativeDiskANNMethods.insert(context, index, (nint)id_data, (nuint)id_len, (nint)vector_data, (nuint)vector_len, (nint)attributes_data, (nuint)attributes_len) == 1;
        }

        public int SearchVector(
            ulong context,
            nint index,
            ReadOnlySpan<float> vector,
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
            var vector_len = vector.Length;

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
        const string DISKANN_GARNET = "diskann_garnet.dll";

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
