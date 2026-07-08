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
    public enum VectorQuantType : int
    {
        Invalid = 0,

        // Redis quantiziations

        /// <summary>
        /// Vectors stored as is with no quantization.
        /// </summary>
        NoQuant = 1,
        /// <summary>
        /// Vectors stored as binary (1 bit).
        /// </summary>
        Bin = 2,
        /// <summary>
        /// Vectors stored as bytes (8 bits).
        /// </summary>
        Q8 = 3,

        // Extended quantizations

        /// <summary>
        /// Vectors stored as bytes (8 bits unsigned). XNoQuant_U8 is a non-Redis extension, stands for: 
        /// eXtension No Quantization Unsigned integer 8 bits
        /// 
        /// XPREQ8 aliases to this.
        /// </summary>
        XNoQuant_U8 = 4,

        /// <summary>
        /// Vectors stored as bytes (8 bits signed). XNoQuant_I8 is a non-Redis extension, stands for: 
        /// eXtension No Quantization Integer 8 bits
        /// </summary>
        XNoQuant_I8 = 5,

        /// <summary>
        /// Vectors stored as bytes (8 bits signed). XBin_I8 is a non-Redis extension, stands for: 
        /// eXtension Binary quantized Integer 8 bits
        /// </summary>
        XBin_I8 = 6,

        /// <summary>
        /// Vectors stored as bytes (8 bits unsigned). XBin_U8 is a non-Redis extension, stands for: 
        /// eXtension Binary quantized Unsigned integer 8 bits
        /// </summary>
        XBin_U8 = 7,
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
        FP32 = 1,

        // Extended formats

        /// <summary>
        /// Bytes (8 bit), unsigned.  XU8 is a non-Redis extensions, stands for:
        /// eXtension Unsigned-integer 8 bits
        /// 
        /// XB8 aliases to this.
        /// </summary>
        XU8 = 2,

        /// <summary>
        /// Bytes (8 bit), signed.
        /// </summary>
        XI8 = 3,
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
    /// Supported distance metrics for vector similarity search.
    /// Aligned with DiskANN's Metric type
    /// </summary>
    public enum VectorDistanceMetricType : int
    {
        Invalid = -1,

        /// <summary>
        /// Cosine similarity
        /// </summary>
        Cosine = 0,

        /// <summary>
        /// Inner product
        /// </summary>
        InnerProduct = 1,

        /// <summary>
        /// Squared Euclidean (L2-Squared)
        /// </summary>
        L2 = 2,

        /// <summary>
        /// Normalized Cosine Similarity.  XCosine_Normalized
        /// </summary>
        XCosine_Normalized = 3,
    }

    /// <summary>
    /// Flags associated with a Vector Set index key.
    /// </summary>
    [Flags]
    public enum VectorSetFlags : int
    {
        /// <summary>
        /// Default, no flags set.
        /// </summary>
        None = 0,

        /// <summary>
        /// A deletion of this key should not schedule cleanup for the associated data and contexts.
        /// </summary>
        SuppressCleanup = 1 << 0,
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
        public unsafe GarnetStatus VectorSetAdd(PinnedSpanByte key, int reduceDims, VectorValueType valueType, PinnedSpanByte values, PinnedSpanByte element, VectorQuantType quantizer, int buildExplorationFactor, PinnedSpanByte attributes, int numLinks, VectorDistanceMetricType distanceMetric, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            var dims =
                valueType switch
                {
                    VectorValueType.FP32 => (uint)(values.ReadOnlySpan.Length / sizeof(float)),
                    VectorValueType.XI8 or VectorValueType.XU8 => (uint)values.ReadOnlySpan.Length,
                    _ => throw new InvalidOperationException($"Unexpected VectorValueType: {valueType}"),
                };

            var dimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            var valueTypeArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
            var valuesArg = values;
            var elementArg = element;
            var quantizerArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            var attributesArg = attributes;
            var numLinksArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));
            var distanceMetricArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorDistanceMetricType, byte>(MemoryMarshal.CreateSpan(ref distanceMetric, 1)));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg, distanceMetricArg]);

            var input = new StringInput(RespCommand.VADD, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadOrCreateVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    errorMsg = default;
                    return status;
                }

                // After a successful read we add the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                result = vectorManager.TryAdd(key, indexSpan, element.ReadOnlySpan, valueType, values.ReadOnlySpan, attributes.ReadOnlySpan, (uint)reduceDims, quantizer, (uint)buildExplorationFactor, (uint)numLinks, distanceMetric, out errorMsg);

                if (result == VectorManagerResult.OK)
                {
                    // On successful addition, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetAdd(key, ref input, ref stringBasicContext);
                }

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Implement Vector Set Remove - returns not found if the element is not present, or the vector set does not exist.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetRemove(PinnedSpanByte key, PinnedSpanByte element)
        {
            var input = new StringInput(RespCommand.VREM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                // After a successful read we remove the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                var res = vectorManager.TryRemove(indexSpan, element.ReadOnlySpan);

                if (res == VectorManagerResult.OK)
                {
                    // On successful removal, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetRemove(key, element, ref input, ref stringBasicContext);

                    return GarnetStatus.OK;
                }

                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetValueSimilarity(PinnedSpanByte key, VectorValueType valueType, PinnedSpanByte values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, out ReadOnlySpan<byte> errorMsg, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        {
            parseState.InitializeWithArgument(key);

            // Get the index
            var input = new StringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    errorMsg = default;
                    return status;
                }

                result = vectorManager.ValueSimilarity(indexSpan, valueType, values.ReadOnlySpan, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, out errorMsg, ref outputDistances, ref outputAttributes, ref filterBitmap);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given an element that is already in the Vector Set.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetElementSimilarity(PinnedSpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ElementSimilarity(indexSpan, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, ref filterBitmap);
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get the approximate vector associated with an element, after (approximately) reversing any transformation.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetEmbedding(PinnedSpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VEMB, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
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
        internal unsafe GarnetStatus VectorSetDimensions(PinnedSpanByte key, out int dimensions)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VDIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    dimensions = 0;
                    return status;
                }

                // After a successful read we extract metadata
                VectorManager.ReadIndex(indexSpan, out _, out var dimensionsUS, out var reducedDimensionsUS, out _, out _, out _, out _, out _, out _);

                dimensions = (int)(reducedDimensionsUS == 0 ? dimensionsUS : reducedDimensionsUS);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get debugging information about the VectorSet
        /// </summary>
        [SkipLocalsInit]
        internal unsafe GarnetStatus VectorSetInfo(PinnedSpanByte key,
            out VectorQuantType quantType,
            out VectorDistanceMetricType distanceMetricType,
            out uint vectorDimensions,
            out uint reducedDimensions,
            out uint buildExplorationFactor,
            out uint numberOfLinks,
            out long size)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VINFO, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    quantType = VectorQuantType.Invalid;
                    distanceMetricType = VectorDistanceMetricType.Invalid;
                    vectorDimensions = 0;
                    reducedDimensions = 0;
                    buildExplorationFactor = 0;
                    numberOfLinks = 0;
                    size = 0;
                    return status;
                }

                // After a successful read we extract metadata
                VectorManager.ReadIndex(indexSpan, out var context, out vectorDimensions, out reducedDimensions, out quantType, out buildExplorationFactor, out numberOfLinks, out distanceMetricType, out _, out var indexPtr);
                size = (long)NativeDiskANNMethods.card(context, indexPtr);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get the attributes associated with an element in the VectorSet
        /// </summary>
        [SkipLocalsInit]
        internal unsafe GarnetStatus VectorSetGetAttribute(PinnedSpanByte key, PinnedSpanByte elementId, ref SpanByteAndMemory outputAttributes)
        {
            parseState.InitializeWithArgument(key);

            // Get the index
            var input = new StringInput(RespCommand.VGETATTR, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                var result = vectorManager.FetchSingleVectorElementAttributes(indexSpan, elementId, ref outputAttributes);
                return result == VectorManagerResult.OK ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
            }
        }
    }
}