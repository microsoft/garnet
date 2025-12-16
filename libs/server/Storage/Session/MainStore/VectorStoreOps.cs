// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Supported quantizations of vector data.
    /// 
    /// This controls the mapping of vector elements to how they're actually stored.
    /// </summary>
    public enum VectorQuantType
    {
        Invalid = 0,

        // Redis quantiziations

        /// <summary>
        /// Provided and stored as floats (FP32).
        /// </summary>
        NoQuant,
        /// <summary>
        /// Provided as FP32, stored as binary (1 bit).
        /// </summary>
        Bin,
        /// <summary>
        /// Provided as FP32, stored as bytes (8 bits).
        /// </summary>
        Q8,

        // Extended quantizations

        /// <summary>
        /// Provided and stored as bytes (8 bits).
        /// </summary>
        XPreQ8,
    }

    /// <summary>
    /// Supported formats for Vector value data.
    /// </summary>
    public enum VectorValueType : int
    {
        Invalid = 0,

        // Redis formats

        /// <summary>
        /// Floats (FP32).
        /// </summary>
        FP32,

        // Extended formats

        /// <summary>
        /// Bytes (8 bit).
        /// </summary>
        XB8,
    }

    /// <summary>
    /// How result ids are formatted in responses from DiskANN.
    /// </summary>
    public enum VectorIdFormat : int
    {
        Invalid = 0,

        /// <summary>
        /// Has 4 bytes of unsigned length before the data.
        /// </summary>
        I32LengthPrefixed,

        /// <summary>
        /// Ids are actually 4-byte ints, no prefix.
        /// </summary>
        FixedI32
    }

    /// <summary>
    /// Implementation of Vector Set operations.
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Implement Vector Set Add - this may also create a Vector Set if one does not already exist.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetAdd(SpanByte key, int reduceDims, VectorValueType valueType, ArgSlice values, ArgSlice element, VectorQuantType quantizer, int buildExplorationFactor, ArgSlice attributes, int numLinks, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            var dims = VectorManager.CalculateValueDimensions(valueType, values.ReadOnlySpan);

            var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
            var valuesArg = values;
            var elementArg = element;
            var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            var attributesArg = attributes;
            var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

            var input = new RawStringInput(RespCommand.VADD, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadOrCreateVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    errorMsg = default;
                    return status;
                }

                // After a successful read we add the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                result = vectorManager.TryAdd(indexSpan, element.ReadOnlySpan, valueType, values.ReadOnlySpan, attributes.ReadOnlySpan, (uint)reduceDims, quantizer, (uint)buildExplorationFactor, (uint)numLinks, out errorMsg);

                if (result == VectorManagerResult.OK)
                {
                    // On successful addition, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetAdd(ref key, ref input, ref basicContext);
                }

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Implement Vector Set Remove - returns not found if the element is not present, or the vector set does not exist.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetRemove(SpanByte key, SpanByte element)
        {
            var input = new RawStringInput(RespCommand.VREM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                // After a successful read we remove the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                var res = vectorManager.TryRemove(indexSpan, element.AsReadOnlySpan());

                if (res == VectorManagerResult.OK)
                {
                    // On successful removal, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetRemove(ref key, ref element, ref input, ref basicContext);

                    return GarnetStatus.OK;
                }

                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetValueSimilarity(SpanByte key, VectorValueType valueType, ArgSlice values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            // Get the index
            var input = new RawStringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ValueSimilarity(indexSpan, valueType, values.ReadOnlySpan, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given an element that is already in the Vector Set.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetElementSimilarity(SpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ElementSimilarity(indexSpan, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get the approximate vector associated with an element, after (approximately) reversing any transformation.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetEmbedding(SpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VEMB, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                if (!vectorManager.TryGetEmbedding(indexSpan, element, ref outputDistances))
                {
                    return GarnetStatus.NOTFOUND;
                }

                return GarnetStatus.OK;
            }
        }

        [SkipLocalsInit]
        internal unsafe GarnetStatus VectorSetDimensions(SpanByte key, out int dimensions)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VDIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    dimensions = 0;
                    return status;
                }

                // After a successful read we extract metadata
                VectorManager.ReadIndex(indexSpan, out _, out var dimensionsUS, out var reducedDimensionsUS, out _, out _, out _, out _, out _);

                dimensions = (int)(reducedDimensionsUS == 0 ? dimensionsUS : reducedDimensionsUS);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Implement Vector Set Add - this may also create a Vector Set if one does not already exist.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetUpdateAttributes(SpanByte key, ArgSlice element, ArgSlice attributes, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            var dims = VectorManager.CalculateValueDimensions(valueType, values.ReadOnlySpan);

            var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
            var valuesArg = values;
            var elementArg = element;
            var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            var attributesArg = attributes;
            var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

            var input = new RawStringInput(RespCommand.VSETATTR, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    errorMsg = default;
                    return status;
                }

                // After a successful read we update the vector attributes while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                result = vectorManager.TrySetAttributes(indexSpan, element.ReadOnlySpan, attributes.ReadOnlySpan, out errorMsg);
                if (result == VectorManagerResult.OK)
                {
                    // On successful addition, we need to manually replicate the attribute update
                    vectorManager.ReplicateVectorSetAdd(ref key, ref input, ref basicContext);
                }

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get debugging information about the VectorSet
        /// </summary>
        [SkipLocalsInit]
        internal unsafe GarnetStatus VectorSetInfo(SpanByte key,
            out VectorQuantType quantType,
            out uint vectorDimensions,
            out uint reducedDimensions,
            out uint buildExplorationFactor,
            out uint numberOfLinks,
            out long size)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VINFO, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    quantType = VectorQuantType.Invalid;
                    vectorDimensions = 0;
                    reducedDimensions = 0;
                    buildExplorationFactor = 0;
                    numberOfLinks = 0;
                    size = 0;
                    return status;
                }

                // After a successful read we extract metadata
                VectorManager.ReadIndex(indexSpan, out _, out vectorDimensions, out reducedDimensions, out quantType, out buildExplorationFactor, out numberOfLinks, out _, out _);

                // TODO: fetch VectorSet size from DiskANN
                size = 0;

                return GarnetStatus.OK;
            }
        }
    }
}