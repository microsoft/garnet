// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

        /// <summary>
        /// Vectors stored as floats (32 bits). XSpherical2 is a non-Redis extension, stands for:
        /// eXtension Spherical 2-bit quantized
        /// </summary>
        XSpherical2 = 8,

        /// <summary>
        /// Vectors stored as bytes (8 bits signed). XSpherical2_I8 is a non-Redis extension, stands for:
        /// eXtension Spherical 2-bit quantized Integer 8 bits
        /// </summary>
        XSpherical2_I8 = 9,

        /// <summary>
        /// Vectors stored as bytes (8 bits unsigned). XSpherical2_U8 is a non-Redis extension, stands for:
        /// eXtension Spherical 2-bit quantized Unsigned integer 8 bits
        /// </summary>
        XSpherical2_U8 = 10,

        /// <summary>
        /// Vectors stored as floats (32 bits). XSpherical4 is a non-Redis extension, stands for:
        /// eXtension Spherical 4-bit quantized
        /// </summary>
        XSpherical4 = 11,

        /// <summary>
        /// Vectors stored as bytes (8 bits signed). XSpherical4_I8 is a non-Redis extension, stands for:
        /// eXtension Spherical 4-bit quantized Integer 8 bits
        /// </summary>
        XSpherical4_I8 = 12,

        /// <summary>
        /// Vectors stored as bytes (8 bits unsigned). XSpherical4_U8 is a non-Redis extension, stands for:
        /// eXtension Spherical 4-bit quantized Unsigned integer 8 bits
        /// </summary>
        XSpherical4_U8 = 13,
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
    /// Term types for XVIMPORT.
    /// </summary>
    public enum VectorImportTermType : uint
    {
        Invalid = uint.MaxValue,

        /// <summary>
        /// Full vector data.
        /// </summary>
        Vector = DiskANNService.FullVector,
        /// <summary>
        /// Neighbor list data.
        /// </summary>
        Neighbors = DiskANNService.NeighborList,
        /// <summary>
        /// Quantized vector data, for indexes that have quantizers.
        /// </summary>
        Quant = DiskANNService.QuantizedVector,
        /// <summary>
        /// Attribute data.
        /// </summary>
        Attrs = DiskANNService.Attributes,
        /// <summary>
        /// Internal id -&gt; external id data.
        /// </summary>
        IntMap = DiskANNService.InternalIdMap,
        /// <summary>
        /// External id -&gt; internal id data.
        /// </summary>
        ExtMap = DiskANNService.ExternalIdMap,
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

        /// <summary>
        /// Imported data is unavailable to ordinary operations until FINISH succeeds.
        /// </summary>
        ImportPending = 1 << 1,

        /// <summary>
        /// FINISH succeeded; repeated finalization does not require the native index.
        /// </summary>
        ImportCompleted = 1 << 2,

        /// <summary>
        /// FINISH failed terminally; the set must be deleted before importing again.
        /// </summary>
        ImportFailed = 1 << 3,
    }

    /// <summary>
    /// Implementation of Vector Set operations.
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <inheritdoc cref="IGarnetApi.VectorSetCreate"/>
        public GarnetStatus VectorSetCreate(PinnedSpanByte key, int dimensions, int reduceDims, VectorQuantType quantizer,
            int buildExplorationFactor, int numLinks, VectorDistanceMetricType distanceMetric, PinnedSpanByte? quantState, uint startPointId,
            out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            result = VectorManagerResult.BadParams;
            errorMsg = default;

            if (reduceDims != 0 && quantizer is VectorQuantType.XNoQuant_U8 or VectorQuantType.XNoQuant_I8 or VectorQuantType.XBin_U8 or VectorQuantType.XBin_I8 or VectorQuantType.XSpherical2_I8 or VectorQuantType.XSpherical2_U8 or VectorQuantType.XSpherical4_I8 or VectorQuantType.XSpherical4_U8)
            {
                errorMsg = "ERR REDUCE is not supported with this quantization"u8;
                result = VectorManagerResult.BadParams;
                return GarnetStatus.OK;
            }
            else if (quantState.HasValue && quantizer is VectorQuantType.NoQuant or VectorQuantType.XNoQuant_U8 or VectorQuantType.XNoQuant_I8)
            {
                errorMsg = "ERR QUANT_STATE is not supported with NOQUANT"u8;
                result = VectorManagerResult.BadParams;
                return GarnetStatus.OK;
            }

            return vectorManager.CreateEmptyVectorSet(this, key.ReadOnlySpan, (uint)dimensions, (uint)reduceDims, quantizer,
                (uint)buildExplorationFactor, (uint)numLinks, distanceMetric, quantState.HasValue, quantState.GetValueOrDefault().ReadOnlySpan, startPointId, out result, out errorMsg);
        }

        /// <inheritdoc cref="IGarnetApi.VectorSetImport"/>
        public GarnetStatus VectorSetImport(PinnedSpanByte key, VectorImportTermType termType, PinnedSpanByte id, PinnedSpanByte value,
            out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            Debug.Assert(Enum.IsDefined(termType) && termType != VectorImportTermType.Invalid, "Should have validate termType before calling");

            if (!vectorManager.IsEnabled)
            {
                errorMsg = "ERR Vector Set (preview) commands are not enabled"u8;
                result = VectorManagerResult.BadParams;
                return GarnetStatus.OK;
            }

            if (id.ReadOnlySpan.IsEmpty || value.ReadOnlySpan.IsEmpty)
            {
                errorMsg = "ERR vector set import ID and value must not be empty"u8;
                result = VectorManagerResult.BadParams;
                return GarnetStatus.OK;
            }

            result = VectorManagerResult.Invalid;
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.XVIMPORT, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out _))
            {
                if (status != GarnetStatus.OK)
                {
                    errorMsg = "ERR Vector Set not found"u8;
                    result = VectorManagerResult.BadParams;
                    return status;
                }

                if (vectorManager.ImportTerm(key, indexSpan, (uint)termType, id.ReadOnlySpan, value.ReadOnlySpan))
                {
                    errorMsg = ""u8;
                    result = VectorManagerResult.OK;
                    return GarnetStatus.OK;
                }
                else
                {
                    errorMsg = "ERR vector set import failed"u8;
                    result = VectorManagerResult.BadParams;
                    return GarnetStatus.OK;
                }
            }
        }

        /// <inheritdoc cref="IGarnetApi.VectorSetFinishImport"/>
        public GarnetStatus VectorSetFinishImport(PinnedSpanByte key, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            result = VectorManagerResult.BadParams;
            errorMsg = default;
            if (!vectorManager.IsEnabled)
            {
                errorMsg = "ERR Vector Set (preview) commands are not enabled"u8;
                return GarnetStatus.OK;
            }
            if (key.ReadOnlySpan.IsEmpty)
            {
                errorMsg = "ERR Vector Set key cannot be empty"u8;
                return GarnetStatus.OK;
            }

            result = VectorManagerResult.Invalid;
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.XVIMPORT, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out _))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                var finishResult = vectorManager.FinishImport(key, indexSpan);
                if (finishResult == NativeDiskANNMethods.DiskANNImportResult.Success)
                {
                    result = VectorManagerResult.OK;
                }
                else
                {
                    errorMsg = finishResult == NativeDiskANNMethods.DiskANNImportResult.FinishFailed
                        ? "ERR vector set import finalization failed"u8
                        : "ERR vector set import verification failed"u8;
                }
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Implement Vector Set Add - this may also create a Vector Set if one does not already exist.
        /// </summary>
        [SkipLocalsInit]
        public GarnetStatus VectorSetAdd(PinnedSpanByte key, int reduceDims, VectorValueType valueType, PinnedSpanByte values, PinnedSpanByte element, VectorQuantType quantizer, int buildExplorationFactor, PinnedSpanByte attributes, int numLinks, VectorDistanceMetricType distanceMetric, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
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
            using (vectorManager.ReadOrCreateVectorIndex(this, key, ref input, indexSpan, VectorManager.DefaultStartPointId, out var status, out var importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    result = importPending ? VectorManagerResult.ImportingPending : VectorManagerResult.Invalid;
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
        public GarnetStatus VectorSetRemove(PinnedSpanByte key, PinnedSpanByte element, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VREM, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
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
        /// Update attribute on an element in a Vector Set.
        /// 
        /// Returns <see cref="GarnetStatus.NOTFOUND"/> if Vector Set does not exist, or element is not a member.
        /// 
        /// Removing an attribute is modelled as setting an empty attribute.
        /// </summary>
        [SkipLocalsInit]
        public GarnetStatus VectorSetSetAttribute(PinnedSpanByte key, PinnedSpanByte element, PinnedSpanByte attribute, out bool importPending)
        {
            parseState.InitializeWithArguments([key, element, attribute]);

            var input = new StringInput(RespCommand.VSETATTR, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                if (vectorManager.TrySetAttribute(indexSpan, element, attribute))
                {
                    // On successful update, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetSetAttribute(key, element, attribute, ref input, ref stringBasicContext);

                    return GarnetStatus.OK;
                }

                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        [SkipLocalsInit]
        public GarnetStatus VectorSetValueSimilarity(PinnedSpanByte key, VectorValueType valueType, PinnedSpanByte values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, out ReadOnlySpan<byte> errorMsg, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        {
            parseState.InitializeWithArgument(key);

            // Get the index
            var input = new StringInput(RespCommand.VSIM, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out var importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    result = importPending ? VectorManagerResult.ImportingPending : VectorManagerResult.Invalid;
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
        public GarnetStatus VectorSetElementSimilarity(PinnedSpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VSIM, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out var importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    result = importPending ? VectorManagerResult.ImportingPending : VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ElementSimilarity(indexSpan, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, ref filterBitmap);
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get the vector associated with an element.
        /// </summary>
        [SkipLocalsInit]
        public GarnetStatus VectorSetEmbedding(PinnedSpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VEMB, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
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

        /// <summary>
        /// Get a RAW view of a quantized (or full, if no quantized version is available) vector associated with an element.
        /// </summary>
        [SkipLocalsInit]
        public GarnetStatus VectorSetRawEmbedding(PinnedSpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory quantizedValues, out VectorQuantType quantType, out double norm, out double? range, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VEMB, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    quantType = VectorQuantType.Invalid;
                    norm = double.NaN;
                    range = null;
                    return status;
                }

                if (!vectorManager.TryGetRawEmbedding(indexSpan, element, ref quantizedValues, out quantType, out norm, out range))
                {
                    return GarnetStatus.NOTFOUND;
                }

                return GarnetStatus.OK;
            }
        }

        [SkipLocalsInit]
        internal GarnetStatus VectorSetDimensions(PinnedSpanByte key, out int dimensions, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VDIM, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
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
        /// Read stored metadata without accessing DiskANN while import is pending.
        /// </summary>
        [SkipLocalsInit]
        internal GarnetStatus VectorSetInfo(PinnedSpanByte key,
            out VectorQuantType quantType,
            out VectorDistanceMetricType distanceMetricType,
            out uint vectorDimensions,
            out uint reducedDimensions,
            out uint buildExplorationFactor,
            out uint numberOfLinks,
            out long size,
            out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VINFO, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
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
                VectorManager.ReadIndex(indexSpan, out var context, out vectorDimensions, out reducedDimensions, out quantType, out buildExplorationFactor, out numberOfLinks, out distanceMetricType, out var flags, out var indexPtr);
                if (importPending)
                {
                    // Can't check cardinality if Vector Set is being imported
                    size = 0;
                    return GarnetStatus.OK;
                }

                var cardinality = NativeDiskANNMethods.card(context, indexPtr);
                size = cardinality == ulong.MaxValue ? -1 : (long)cardinality;

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get number of vectors in Vector Set.
        /// </summary>
        internal GarnetStatus VectorSetCardinality(PinnedSpanByte key, out long card, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VCARD, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    card = 0;
                    return status;
                }

                // After a successful read we extract metadata
                VectorManager.ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);
                var cardinality = NativeDiskANNMethods.card(context, indexPtr);
                card = cardinality == ulong.MaxValue ? -1 : (long)cardinality;

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Determine if an element is a member of a Vector Set.
        /// </summary>
        internal GarnetStatus VectorSetIsMember(PinnedSpanByte key, PinnedSpanByte element, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VISMEMBER, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                // After a successful read we extract metadata
                if (vectorManager.IsMember(indexSpan, element))
                {
                    return GarnetStatus.OK;
                }
                else
                {
                    return GarnetStatus.NOTFOUND;
                }
            }
        }

        /// <summary>
        /// Determine neighbors of a given element, and (optionally) the distance to each neighbor.
        /// </summary>
        internal GarnetStatus VectorSetLinks(PinnedSpanByte key, PinnedSpanByte element, ref SpanByteAndMemory idResults, ref SpanByteAndMemory distanceResults, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VLINKS, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                var res = vectorManager.GetNeighbors(indexSpan, element, ref idResults, ref distanceResults);
                return res == VectorManagerResult.OK ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Fetch random elements from the given Vector Set.
        /// 
        /// If <paramref name="count"/> is &lt; 0 we allow duplicates, if <paramref name="count"/> &gt; 0 we remove duplicates.
        /// 
        /// It is OK to fetch fewer than the requested number of elements.
        /// 
        /// On success, <paramref name="idResults"/> has length prefixed element names.
        /// </summary>
        internal GarnetStatus VectorSetRandomMembers(PinnedSpanByte key, int count, ref SpanByteAndMemory idResults, out int actualCount, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            var input = new StringInput(RespCommand.VRANDMEMBER, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
            {
                if (status != GarnetStatus.OK)
                {
                    actualCount = 0;
                    return status;
                }

                var result = vectorManager.RandomMembers(indexSpan, Math.Abs(count), allowDuplicates: count < 0, ref idResults, out actualCount);
                return result == VectorManagerResult.OK ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Get the attributes associated with an element in the VectorSet
        /// </summary>
        [SkipLocalsInit]
        internal GarnetStatus VectorSetGetAttribute(PinnedSpanByte key, PinnedSpanByte elementId, ref SpanByteAndMemory outputAttributes, out bool importPending)
        {
            parseState.InitializeWithArgument(key);

            // Get the index
            var input = new StringInput(RespCommand.VGETATTR, ref parseState);
            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];
            using (vectorManager.ReadVectorIndex(this, key, ref input, indexSpan, out var status, out importPending))
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