// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool NetworkVADD<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // VADD key [REDUCE dim] (FP32 | XB8 | VALUES num) vector element [CAS] [NOQUANT | Q8 | BIN | XPREQ8] [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
            //
            // XB8 is a non-Redis extension, stands for: eXtension Binary 8-bit values - encodes [0, 255] per dimension
            // XPREQ8 is a non-Redis extension, stands for: eXtension PREcalculated Quantization 8-bit - requests no quantization on pre-calculated [0, 255] values

            const int MinM = 4;
            const int MaxM = 4_096;

            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // key FP32|VALUES vector element
            if (parseState.Count < 4)
            {
                return AbortWithWrongNumberOfArguments("VADD");
            }

            ref var key = ref parseState.GetArgSliceByRef(0);

            var curIx = 1;

            var reduceDim = 0;
            if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("REDUCE"u8))
            {
                curIx++;
                if (!parseState.TryGetInt(curIx, out var reduceDimValue) || reduceDimValue <= 0)
                {
                    return AbortWithErrorMessage("REDUCE dimension must be > 0"u8);
                }

                reduceDim = reduceDimValue;
                curIx++;
            }

            var valueType = VectorValueType.Invalid;
            int vectorDims = 0;
            byte[] rentedValues = null;
            Span<byte> values = stackalloc byte[64 * sizeof(float)];

            try
            {
                if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("FP32"u8))
                {
                    curIx++;
                    if (curIx >= parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                    if ((asBytes.Length % sizeof(float)) != 0)
                    {
                        return AbortWithErrorMessage("ERR invalid vector specification");
                    }

                    vectorDims = asBytes.Length / sizeof(float);
                    if (vectorDims > VectorManager.MaxVectorDimensions)
                    {
                        return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                    }

                    curIx++;
                    valueType = VectorValueType.FP32;
                    values = asBytes;
                }
                else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("VALUES"u8))
                {
                    curIx++;
                    if (curIx >= parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    if (!parseState.TryGetInt(curIx, out vectorDims) || vectorDims <= 0)
                    {
                        return AbortWithErrorMessage("ERR invalid vector specification");
                    }

                    curIx++;

                    if (vectorDims > VectorManager.MaxVectorDimensions)
                    {
                        return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                    }

                    if (curIx + vectorDims > parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    if (vectorDims * sizeof(float) > values.Length)
                    {
                        values = rentedValues = ArrayPool<byte>.Shared.Rent(vectorDims * sizeof(float));
                    }
                    values = values[..(vectorDims * sizeof(float))];

                    valueType = VectorValueType.FP32;
                    var floatValues = MemoryMarshal.Cast<byte, float>(values);

                    for (var valueIx = 0; valueIx < vectorDims; valueIx++)
                    {
                        if (!parseState.TryGetFloat(curIx, out floatValues[valueIx]))
                        {
                            return AbortWithErrorMessage("ERR invalid vector specification");
                        }

                        curIx++;
                    }
                }
                else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XU8"u8) || parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XB8"u8)) // XB8 preserved for backwards compatibility, prefer XU8
                {
                    curIx++;
                    if (curIx >= parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                    curIx++;

                    vectorDims = asBytes.Length;

                    if (vectorDims > VectorManager.MaxVectorDimensions)
                    {
                        return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                    }

                    valueType = VectorValueType.XU8;
                    values = asBytes;
                }
                else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XI8"u8))
                {
                    curIx++;
                    if (curIx >= parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                    curIx++;

                    vectorDims = asBytes.Length;

                    if (vectorDims > VectorManager.MaxVectorDimensions)
                    {
                        return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                    }

                    valueType = VectorValueType.XI8;
                    values = asBytes;
                }
                else
                {
                    return AbortWithErrorMessage("ERR invalid vector specification");
                }

                if (reduceDim > vectorDims)
                {
                    return AbortWithErrorMessage("ERR REDUCE dimension must be <= vector dimensions");
                }

                if (curIx >= parseState.Count)
                {
                    return AbortWithWrongNumberOfArguments("VADD");
                }

                var element = parseState.GetArgSliceByRef(curIx);
                curIx++;

                // Order for everything after element is unspecified
                var cas = false;
                VectorQuantType? quantType = null;
                int? buildExplorationFactor = null;
                PinnedSpanByte? attributes = null;
                int? numLinks = null;
                VectorDistanceMetricType? distanceMetric = null;

                while (curIx < parseState.Count)
                {
                    // REDUCE is illegal after values, no matter how specified
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("REDUCE"u8))
                    {
                        return AbortWithErrorMessage("ERR invalid option after element");
                    }

                    // Look for CAS
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("CAS"u8))
                    {
                        if (cas)
                        {
                            return AbortWithErrorMessage("CAS specified multiple times");
                        }

                        // We ignore CAS, just remember we saw it
                        cas = true;
                        curIx++;

                        continue;
                    }

                    // Look for quantizer specs
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("NOQUANT"u8))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.NoQuant;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("Q8"u8))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.Q8;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("BIN"u8))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.Bin;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XNOQUANT_U8"u8, allowNonAlphabeticChars: true) || parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XPREQ8"u8)) // XPREQ8 kept for backwards compatability, prefer XNOQUANT_U8
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.XNoQuant_U8;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XNOQUANT_I8"u8, allowNonAlphabeticChars: true))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.XNoQuant_I8;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XBIN_I8"u8, allowNonAlphabeticChars: true))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.XBin_I8;
                        curIx++;

                        continue;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XBIN_U8"u8, allowNonAlphabeticChars: true))
                    {
                        if (quantType != null)
                        {
                            return AbortWithErrorMessage("Quantization specified multiple times");
                        }

                        quantType = VectorQuantType.XBin_U8;
                        curIx++;

                        continue;
                    }

                    // Look for build-exploration-factor
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("EF"u8))
                    {
                        if (buildExplorationFactor != null)
                        {
                            return AbortWithErrorMessage("EF specified multiple times");
                        }

                        curIx++;

                        if (curIx >= parseState.Count)
                        {
                            return AbortWithErrorMessage("ERR invalid option after element");
                        }

                        if (!parseState.TryGetInt(curIx, out var buildExplorationFactorNonNull) || buildExplorationFactorNonNull <= 0 || buildExplorationFactorNonNull > VectorManager.MaxExplorationFactor)
                        {
                            return AbortWithErrorMessage($"ERR EF must be an integer between 1 and {VectorManager.MaxExplorationFactor}");
                        }

                        buildExplorationFactor = buildExplorationFactorNonNull;
                        curIx++;
                        continue;
                    }

                    // Look for attributes
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("SETATTR"u8))
                    {
                        if (attributes != null)
                        {
                            return AbortWithErrorMessage("SETATTR specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithErrorMessage("ERR invalid option after element");
                        }

                        attributes = parseState.GetArgSliceByRef(curIx);
                        curIx++;

                        // You might think we need to validate attributes, but Redis actually lets anything through

                        continue;
                    }

                    // Look for num links
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("M"u8))
                    {
                        if (numLinks != null)
                        {
                            return AbortWithErrorMessage("M specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithErrorMessage("ERR invalid option after element");
                        }

                        if (!parseState.TryGetInt(curIx, out var numLinksNonNull) || numLinksNonNull < MinM || numLinksNonNull > MaxM)
                        {
                            return AbortWithErrorMessage($"ERR M must be an integer between {MinM} and {MaxM}");
                        }

                        numLinks = numLinksNonNull;
                        curIx++;

                        continue;
                    }

                    // Look for distance metric - this is an extension, though hopefully one Redis can be convinced to adopt
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XDISTANCE_METRIC"u8, allowNonAlphabeticChars: true))
                    {
                        if (distanceMetric != null)
                        {
                            return AbortWithErrorMessage("XDISTANCE_METRIC specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithErrorMessage("ERR invalid option after element");
                        }

                        // Look for distance metric spec
                        if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("L2"u8))
                        {
                            distanceMetric = VectorDistanceMetricType.L2;
                        }
                        else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("COSINE"u8))
                        {
                            distanceMetric = VectorDistanceMetricType.Cosine;
                        }
                        else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("IP"u8))
                        {
                            distanceMetric = VectorDistanceMetricType.InnerProduct;
                        }
                        else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("XCOSINE_NORMALIZED"u8, allowNonAlphabeticChars: true))
                        {
                            // This is an extension to the Redis protocol, thus the X prefix
                            distanceMetric = VectorDistanceMetricType.XCosine_Normalized;
                        }
                        else
                        {
                            return AbortWithErrorMessage("ERR invalid XDISTANCE_METRIC");
                        }

                        curIx++;
                        continue;
                    }

                    // Didn't recognize this option, error out
                    return AbortWithErrorMessage("ERR invalid option after element");
                }

                if (key.ReadOnlySpan.IsEmpty)
                {
                    // TODO: this is not a Redis restriction, but once that comes from Replication Keys being in the 0 namespace, we should lift it
                    return AbortWithErrorMessage("ERR Vector Set key cannot be empty"u8);
                }

                // Default unspecified options
                quantType ??= VectorQuantType.Q8;
                buildExplorationFactor ??= 200;
                attributes ??= default;
                numLinks ??= 16;
                distanceMetric ??= VectorDistanceMetricType.L2;

                // We need to reject these HERE because validation during create_index is very awkward
                GarnetStatus res;
                VectorManagerResult result;
                ReadOnlySpan<byte> customErrMsg;
                if (quantType is VectorQuantType.XBin_U8 or VectorQuantType.XBin_I8 or VectorQuantType.XNoQuant_U8 or VectorQuantType.XNoQuant_I8 && reduceDim != 0)
                {
                    result = VectorManagerResult.BadParams;
                    res = GarnetStatus.OK;
                    customErrMsg = default;
                }
                else
                {
                    if (rentedValues != null)
                    {
                        // For large enough values we have to pay for a pin
                        fixed (byte* valuesPtr = rentedValues)
                        {
                            res = storageApi.VectorSetAdd(key, reduceDim, valueType, PinnedSpanByte.FromPinnedPointer(valuesPtr, values.Length), element, quantType.Value, buildExplorationFactor.Value, attributes.Value, numLinks.Value, distanceMetric.Value, out result, out customErrMsg);
                        }
                    }
                    else
                    {
                        res = storageApi.VectorSetAdd(key, reduceDim, valueType, PinnedSpanByte.FromPinnedSpan(values), element, quantType.Value, buildExplorationFactor.Value, attributes.Value, numLinks.Value, distanceMetric.Value, out result, out customErrMsg);
                    }
                }

                if (res == GarnetStatus.OK)
                {
                    if (result == VectorManagerResult.OK)
                    {
                        if (respProtocolVersion == 3)
                        {
                            while (!RespWriteUtils.TryWriteTrue(ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            while (!RespWriteUtils.TryWriteInt32(1, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                    else if (result == VectorManagerResult.Duplicate)
                    {
                        if (respProtocolVersion == 3)
                        {
                            while (!RespWriteUtils.TryWriteFalse(ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                    else if (result == VectorManagerResult.BadParams)
                    {
                        if (customErrMsg.IsEmpty)
                        {
                            return AbortWithErrorMessage("ERR asked quantization mismatch with existing vector set"u8);
                        }

                        return AbortWithErrorMessage(customErrMsg);
                    }
                }
                else if (res == GarnetStatus.WRONGTYPE)
                {
                    return AbortVectorSetWrongType();
                }
                else
                {
                    return AbortWithErrorMessage($"Unexpected GarnetStatus: {res}");
                }

                return true;
            }
            finally
            {
                if (rentedValues != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedValues);
                }
            }
        }

        private bool NetworkVSIM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            const int DefaultResultSetSize = 64;
            const int DefaultIdSize = sizeof(ulong);
            const int DefaultAttributeSize = 32;

            // VSIM key (ELE | FP32 | XB8 | VALUES num) (vector | element) [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF search-exploration - factor] [FILTER expression][FILTER-EF max - filtering - effort] [TRUTH][NOTHREAD]
            //
            // XB8 is a non-Redis extension, stands for: eXtension Binary 8-bit values - encodes [0, 255] per dimension

            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments("VSIM");
            }

            ref var key = ref parseState.GetArgSliceByRef(0);
            var kind = parseState.GetArgSliceByRef(1);

            var curIx = 2;

            PinnedSpanByte? element;

            VectorValueType valueType = VectorValueType.Invalid;
            byte[] rentedValues = null;
            try
            {
                Span<byte> values = stackalloc byte[64 * sizeof(float)];
                if (kind.Span.EqualsUpperCaseSpanIgnoringCase("ELE"u8))
                {
                    element = parseState.GetArgSliceByRef(curIx);
                    values = default;
                    curIx++;
                }
                else
                {
                    element = default;
                    if (kind.Span.EqualsUpperCaseSpanIgnoringCase("FP32"u8))
                    {
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                        if ((asBytes.Length % sizeof(float)) != 0)
                        {
                            return AbortWithErrorMessage("FP32 values must be multiple of 4-bytes in size");
                        }

                        if (asBytes.Length / sizeof(float) > VectorManager.MaxVectorDimensions)
                        {
                            return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                        }

                        valueType = VectorValueType.FP32;
                        values = asBytes;
                        curIx++;
                    }
                    else if (kind.Span.EqualsUpperCaseSpanIgnoringCase("XU8"u8) || kind.Span.EqualsUpperCaseSpanIgnoringCase("XB8"u8)) // XB8 preserved for backwards compatibility, prefer XU8
                    {
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                        curIx++;

                        if (asBytes.Length > VectorManager.MaxVectorDimensions)
                        {
                            return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                        }

                        valueType = VectorValueType.XU8;
                        values = asBytes;
                    }
                    else if (kind.Span.EqualsUpperCaseSpanIgnoringCase("XI8"u8))
                    {
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        var asBytes = parseState.GetArgSliceByRef(curIx).Span;
                        curIx++;

                        if (asBytes.Length > VectorManager.MaxVectorDimensions)
                        {
                            return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                        }

                        valueType = VectorValueType.XI8;
                        values = asBytes;
                    }
                    else if (kind.Span.EqualsUpperCaseSpanIgnoringCase("VALUES"u8))
                    {
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out var valueCount) || valueCount <= 0)
                        {
                            return AbortWithErrorMessage("VALUES count must > 0");
                        }

                        if (valueCount > VectorManager.MaxVectorDimensions)
                        {
                            return AbortWithErrorMessage($"ERR vector exceeds maximum of {VectorManager.MaxVectorDimensions} dimensions");
                        }

                        curIx++;

                        if (valueCount * sizeof(float) > values.Length)
                        {
                            values = rentedValues = ArrayPool<byte>.Shared.Rent(valueCount * sizeof(float));
                        }
                        values = values[..(valueCount * sizeof(float))];

                        if (curIx + valueCount > parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        valueType = VectorValueType.FP32;
                        var floatValues = MemoryMarshal.Cast<byte, float>(values);

                        for (var valueIx = 0; valueIx < valueCount; valueIx++)
                        {
                            if (!parseState.TryGetFloat(curIx, out floatValues[valueIx]))
                            {
                                return AbortWithErrorMessage("VALUES value must be valid float");
                            }

                            curIx++;
                        }
                    }
                    else
                    {
                        return AbortWithErrorMessage("VSIM expected ELE, FP32, or VALUES");
                    }
                }

                bool? withScores = null;
                bool? withAttributes = null;
                int? count = null;
                float? delta = null;
                int? searchExplorationFactor = null;
                PinnedSpanByte? filter = null;
                int? maxFilteringEffort = null;
                var truth = false;
                var noThread = false;

                while (curIx < parseState.Count)
                {
                    // Check for withScores
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("WITHSCORES"u8))
                    {
                        if (withScores != null)
                        {
                            return AbortWithErrorMessage("WITHSCORES specified multiple times");
                        }

                        withScores = true;
                        curIx++;
                        continue;
                    }

                    // Check for withAttributes
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("WITHATTRIBS"u8))
                    {
                        if (withAttributes != null)
                        {
                            return AbortWithErrorMessage("WITHATTRIBS specified multiple times");
                        }

                        withAttributes = true;
                        curIx++;
                        continue;
                    }

                    // Check for count
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("COUNT"u8))
                    {
                        if (count != null)
                        {
                            return AbortWithErrorMessage("COUNT specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out var countNonNull) || countNonNull < 0 || countNonNull > VectorManager.MaxRetrieveCount)
                        {
                            return AbortWithErrorMessage($"ERR COUNT must be an integer between 0 and {VectorManager.MaxRetrieveCount}");
                        }

                        count = countNonNull;
                        curIx++;
                        continue;
                    }

                    // Check for delta
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("EPSILON"u8))
                    {
                        if (delta != null)
                        {
                            return AbortWithErrorMessage("EPSILON specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetFloat(curIx, out var deltaNonNull) || deltaNonNull <= 0)
                        {
                            return AbortWithErrorMessage("EPSILON must be float > 0");
                        }

                        delta = deltaNonNull;
                        curIx++;
                        continue;
                    }

                    // Check for search exploration factor
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("EF"u8))
                    {
                        if (searchExplorationFactor != null)
                        {
                            return AbortWithErrorMessage("EF specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out var searchExplorationFactorNonNull) || searchExplorationFactorNonNull <= 0 || searchExplorationFactorNonNull > VectorManager.MaxExplorationFactor)
                        {
                            return AbortWithErrorMessage($"ERR EF must be an integer between 1 and {VectorManager.MaxExplorationFactor}");
                        }

                        searchExplorationFactor = searchExplorationFactorNonNull;
                        curIx++;
                        continue;
                    }

                    // Check for filter
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FILTER"u8))
                    {
                        if (filter != null)
                        {
                            return AbortWithErrorMessage("FILTER specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        filter = parseState.GetArgSliceByRef(curIx);
                        curIx++;

                        // TODO: validate filter

                        continue;
                    }

                    // Check for max filtering effort
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FILTER-EF"u8))
                    {
                        if (maxFilteringEffort != null)
                        {
                            return AbortWithErrorMessage("FILTER-EF specified multiple times");
                        }

                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out var maxFilteringEffortNonNull) || maxFilteringEffortNonNull < 4 || maxFilteringEffortNonNull > VectorManager.MaxFilteringScaleFactor)
                        {
                            return AbortWithErrorMessage($"ERR FILTER-EF must be an integer between 4 and {VectorManager.MaxFilteringScaleFactor}");
                        }

                        maxFilteringEffort = maxFilteringEffortNonNull;
                        curIx++;
                        continue;
                    }

                    // Check for truth
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("TRUTH"u8))
                    {
                        if (truth)
                        {
                            return AbortWithErrorMessage("TRUTH specified multiple times");
                        }

                        // TODO: should we implement TRUTH?
                        truth = true;
                        curIx++;
                        continue;
                    }

                    // Check for no thread
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("NOTHREAD"u8))
                    {
                        if (noThread)
                        {
                            return AbortWithErrorMessage("NOTHREAD specified multiple times");
                        }

                        // We ignore NOTHREAD
                        noThread = true;
                        curIx++;
                        continue;
                    }

                    // Didn't recognize this option, error out
                    return AbortWithErrorMessage("Unknown option");
                }

                // Default unspecified options
                withScores ??= false;
                withAttributes ??= false;
                count ??= 10;
                delta ??= 2f;
                searchExplorationFactor ??= 100;
                filter ??= default;
                maxFilteringEffort ??= 16;

                // TODO: these stackallocs are dangerous, need logic to avoid stack overflow
                Span<byte> idSpace = stackalloc byte[(DefaultResultSetSize * DefaultIdSize) + (DefaultResultSetSize * sizeof(int))];
                Span<float> distanceSpace = stackalloc float[DefaultResultSetSize];
                var needFilter = filter.Value.Length > 0;
                var needAttributes = withAttributes.Value || needFilter;
                Span<byte> attributeSpace = needAttributes ? stackalloc byte[(DefaultResultSetSize * DefaultAttributeSize) + (DefaultResultSetSize * sizeof(int))] : default;

                var idResult = SpanByteAndMemory.FromPinnedSpan(idSpace);
                var distanceResult = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(distanceSpace));
                var attributeResult = SpanByteAndMemory.FromPinnedSpan(attributeSpace);
                // Bitmap: 1 bit per result. DefaultResultSetSize results = 8 bytes on stack.
                Span<byte> bitmapSpace = needFilter ? stackalloc byte[(DefaultResultSetSize + 7) >> 3] : default;
                var filterBitmapResult = SpanByteAndMemory.FromPinnedSpan(bitmapSpace);
                try
                {
                    GarnetStatus res;
                    VectorManagerResult vectorRes;
                    VectorIdFormat idFormat;
                    scoped ReadOnlySpan<byte> customErrMsg;
                    if (!element.HasValue)
                    {
                        if (rentedValues != null)
                        {
                            // For large enough values we have to pay for a pin
                            fixed (byte* valuesPtr = rentedValues)
                            {
                                res = storageApi.VectorSetValueSimilarity(key, valueType, PinnedSpanByte.FromPinnedPointer(valuesPtr, values.Length), count.Value, delta.Value, searchExplorationFactor.Value, filter.Value, maxFilteringEffort.Value, withAttributes.Value, ref idResult, out idFormat, out customErrMsg, ref distanceResult, ref attributeResult, out vectorRes, ref filterBitmapResult);
                            }
                        }
                        else
                        {
                            res = storageApi.VectorSetValueSimilarity(key, valueType, PinnedSpanByte.FromPinnedSpan(values), count.Value, delta.Value, searchExplorationFactor.Value, filter.Value, maxFilteringEffort.Value, withAttributes.Value, ref idResult, out idFormat, out customErrMsg, ref distanceResult, ref attributeResult, out vectorRes, ref filterBitmapResult);
                        }
                    }
                    else
                    {
                        res = storageApi.VectorSetElementSimilarity(key, element.Value, count.Value, delta.Value, searchExplorationFactor.Value, filter.Value, maxFilteringEffort.Value, withAttributes.Value, ref idResult, out idFormat, ref distanceResult, ref attributeResult, out vectorRes, ref filterBitmapResult);
                        customErrMsg = default;
                    }

                    if (res == GarnetStatus.NOTFOUND)
                    {
                        // Vector Set does not exist

                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (res == GarnetStatus.OK)
                    {
                        if (vectorRes == VectorManagerResult.MissingElement)
                        {
                            while (!RespWriteUtils.TryWriteError("Element not in Vector Set"u8, ref dcurr, dend))
                                SendAndReset();
                        }
                        else if (vectorRes == VectorManagerResult.OK)
                        {
                            if (respProtocolVersion == 3)
                            {
                                WriteRESP3Result(this, count.Value, idResult, distanceResult, filterBitmapResult, withAttributes.Value, withScores.Value, idFormat, attributeResult);
                            }
                            else
                            {
                                WriteRESP2Result(this, count.Value, idResult, distanceResult, filterBitmapResult, withAttributes.Value, withScores.Value, idFormat, attributeResult);
                            }
                        }
                        else if (vectorRes == VectorManagerResult.BadParams)
                        {
                            if (customErrMsg.IsEmpty)
                            {
                                return AbortWithErrorMessage("ERR asked quantization mismatch with existing vector set"u8);
                            }

                            return AbortWithErrorMessage(customErrMsg);
                        }
                        else
                        {
                            throw new GarnetException($"Unexpected {nameof(VectorManagerResult)}: {vectorRes}");
                        }
                    }
                    else if (res == GarnetStatus.WRONGTYPE)
                    {
                        return AbortVectorSetWrongType();
                    }
                    else
                    {
                        throw new GarnetException($"Unexpected {nameof(GarnetStatus)}: {res}");
                    }

                    return true;
                }
                finally
                {
                    idResult.Memory?.Dispose();
                    distanceResult.Memory?.Dispose();
                    attributeResult.Memory?.Dispose();
                    filterBitmapResult.Memory?.Dispose();
                }
            }
            finally
            {
                if (rentedValues != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedValues);
                }
            }

            // Write VSIM RESP3 result
            //
            // If not withScores and not withAttributes this is an array of matching elements in ascending order by distance
            // If withScores (and not withAttributes) this is a map where keys are bulk string elements and values are double distances
            // If withAttributes (and not withScores) this is a map where keys are bulk string elements and values are bulk string attributes
            // If both withScores and withAttributes this is a map where keys are bulk string elements and values are 2 element arrays with double distances and bulk string attributes (in that order)
            static void WriteRESP3Result(RespServerSession self, int count, SpanByteAndMemory idResult, SpanByteAndMemory distanceResult, SpanByteAndMemory filterBitmapResult, bool withAttributes, bool withScores, VectorIdFormat idFormat, SpanByteAndMemory attributeResult)
            {
                var remainingIds = idResult.ReadOnlySpan;
                var distancesSpan = MemoryMarshal.Cast<byte, float>(distanceResult.ReadOnlySpan);
                var hasFilter = filterBitmapResult.Length > 0;
                var filterBitmap = hasFilter ? filterBitmapResult.ReadOnlySpan : default;
                var remaininingAttributes = (withAttributes || hasFilter) ? attributeResult.ReadOnlySpan : default;

                var totalFound = distancesSpan.Length;

                // Compute max output count: if bitmap is present, popcount it; otherwise all results
                int outputCount;
                if (hasFilter)
                {
                    outputCount = 0;
                    for (var b = 0; b < filterBitmap.Length; b++)
                        outputCount += System.Numerics.BitOperations.PopCount(filterBitmap[b]);
                }
                else
                {
                    outputCount = totalFound;
                }

                // Limit to what is actually asked for
                outputCount = Math.Min(count, outputCount);

                if (!withAttributes && !withScores)
                {
                    // No score or attributes, simple array
                    self.WriteArrayLength(outputCount);
                }
                else
                {
                    // At least one of scores or attributes, so a map is needed
                    self.WriteMapLength(outputCount);
                }

                var writtenCount = 0;
                var resultIndex = 0;

                while (writtenCount < outputCount)
                {
                    ReadOnlySpan<byte> elementData;

                    if (idFormat == VectorIdFormat.I32LengthPrefixed)
                    {
                        if (remainingIds.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for result id length at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        var elementLen = BinaryPrimitives.ReadInt32LittleEndian(remainingIds);

                        if (remainingIds.Length < sizeof(int) + elementLen)
                        {
                            throw new GarnetException($"Insufficient bytes for result of length={elementLen} at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        elementData = remainingIds.Slice(sizeof(int), elementLen);
                        remainingIds = remainingIds[(sizeof(int) + elementLen)..];
                    }
                    else if (idFormat == VectorIdFormat.FixedI32)
                    {
                        if (remainingIds.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for result id length at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        elementData = remainingIds[..sizeof(int)];
                        remainingIds = remainingIds[sizeof(int)..];
                    }
                    else
                    {
                        throw new GarnetException($"Unexpected id format: {idFormat}");
                    }

                    // Check filter bitmap — skip results that didn't pass the filter
                    if (hasFilter && (filterBitmap[resultIndex >> 3] & (1 << (resultIndex & 7))) == 0)
                    {
                        // Advance attribute reader for skipped results (attributes are always present when bitmap exists)
                        if (!remaininingAttributes.IsEmpty)
                        {
                            var skipAttrLen = BinaryPrimitives.ReadInt32LittleEndian(remaininingAttributes);
                            remaininingAttributes = remaininingAttributes[(sizeof(int) + skipAttrLen)..];
                        }

                        resultIndex++;
                        continue;
                    }

                    // Write the element
                    self.WriteBulkString(elementData);

                    if (withScores && withAttributes)
                    {
                        // Writing both, so need wrapping array
                        self.WriteArrayLength(2);
                    }

                    if (withScores)
                    {
                        // Write score if requested
                        var distance = distancesSpan[resultIndex];

                        self.WriteDoubleNumeric(distance);
                    }

                    if (withAttributes)
                    {
                        // Write attribute if requested
                        if (remaininingAttributes.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for attribute length at resultIndex={resultIndex}: {Convert.ToHexString(attributeResult.ReadOnlySpan)}");
                        }

                        var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaininingAttributes);
                        var attr = remaininingAttributes.Slice(sizeof(int), attrLen);
                        remaininingAttributes = remaininingAttributes[(sizeof(int) + attrLen)..];

                        if (attr.IsEmpty)
                        {
                            self.WriteNull();
                        }
                        else
                        {
                            self.WriteBulkString(attr);
                        }
                    }

                    resultIndex++;
                    writtenCount++;
                }
            }

            // Write VSIM RESP2 result
            //
            // This is an array with matching elements in ascending order by distance
            // If withScores (and not withAttributes) then the array size is doubled and every 2nd element is a bulk string of the distance
            // If withAttributes (and not withScores) then the array size is doubled and every 2nd element is a bulk string of the vector's attribute (if any)
            // If both withScores and withAttributes the array size is tripled and every 2nd element is a bulk string of the distance, and every 3rd element is a bulk string of the vector's attribute (if any)
            static void WriteRESP2Result(RespServerSession self, int count, SpanByteAndMemory idResult, SpanByteAndMemory distanceResult, SpanByteAndMemory filterBitmapResult, bool withAttributes, bool withScores, VectorIdFormat idFormat, SpanByteAndMemory attributeResult)
            {
                var remainingIds = idResult.ReadOnlySpan;
                var distancesSpan = MemoryMarshal.Cast<byte, float>(distanceResult.ReadOnlySpan);
                var hasFilter = filterBitmapResult.Length > 0;
                var filterBitmap = hasFilter ? filterBitmapResult.ReadOnlySpan : default;
                var remaininingAttributes = (withAttributes || hasFilter) ? attributeResult.ReadOnlySpan : default;

                var totalFound = distancesSpan.Length;

                // Compute max output count: if bitmap is present, popcount it; otherwise all results
                int outputCount;
                if (hasFilter)
                {
                    outputCount = 0;
                    for (var b = 0; b < filterBitmap.Length; b++)
                        outputCount += System.Numerics.BitOperations.PopCount(filterBitmap[b]);
                }
                else
                {
                    outputCount = totalFound;
                }

                // Limit to what is actually asked for
                outputCount = Math.Min(count, outputCount);

                // Each flag doubles output
                var arrayItemCount = outputCount;
                if (withScores)
                {
                    arrayItemCount += outputCount;
                }
                if (withAttributes)
                {
                    arrayItemCount += outputCount;
                }

                while (!RespWriteUtils.TryWriteArrayLength(arrayItemCount, ref self.dcurr, self.dend))
                    self.SendAndReset();

                var writtenCount = 0;
                var resultIndex = 0;

                while (writtenCount < outputCount)
                {
                    ReadOnlySpan<byte> elementData;

                    if (idFormat == VectorIdFormat.I32LengthPrefixed)
                    {
                        if (remainingIds.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for result id length at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        var elementLen = BinaryPrimitives.ReadInt32LittleEndian(remainingIds);

                        if (remainingIds.Length < sizeof(int) + elementLen)
                        {
                            throw new GarnetException($"Insufficient bytes for result of length={elementLen} at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        elementData = remainingIds.Slice(sizeof(int), elementLen);
                        remainingIds = remainingIds[(sizeof(int) + elementLen)..];
                    }
                    else if (idFormat == VectorIdFormat.FixedI32)
                    {
                        if (remainingIds.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for result id length at resultIndex={resultIndex}: {Convert.ToHexString(distanceResult.ReadOnlySpan)}");
                        }

                        elementData = remainingIds[..sizeof(int)];
                        remainingIds = remainingIds[sizeof(int)..];
                    }
                    else
                    {
                        throw new GarnetException($"Unexpected id format: {idFormat}");
                    }

                    // Check filter bitmap — skip results that didn't pass the filter
                    if (hasFilter && (filterBitmap[resultIndex >> 3] & (1 << (resultIndex & 7))) == 0)
                    {
                        // Advance attribute reader for skipped results (attributes are always present when bitmap exists)
                        if (!remaininingAttributes.IsEmpty)
                        {
                            var skipAttrLen = BinaryPrimitives.ReadInt32LittleEndian(remaininingAttributes);
                            remaininingAttributes = remaininingAttributes[(sizeof(int) + skipAttrLen)..];
                        }

                        resultIndex++;
                        continue;
                    }

                    while (!RespWriteUtils.TryWriteBulkString(elementData, ref self.dcurr, self.dend))
                        self.SendAndReset();

                    if (withScores)
                    {
                        var distance = distancesSpan[resultIndex];

                        while (!RespWriteUtils.TryWriteDoubleBulkString(distance, ref self.dcurr, self.dend))
                            self.SendAndReset();
                    }

                    if (withAttributes)
                    {
                        if (remaininingAttributes.Length < sizeof(int))
                        {
                            throw new GarnetException($"Insufficient bytes for attribute length at resultIndex={resultIndex}: {Convert.ToHexString(attributeResult.ReadOnlySpan)}");
                        }

                        var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaininingAttributes);
                        var attr = remaininingAttributes.Slice(sizeof(int), attrLen);
                        remaininingAttributes = remaininingAttributes[(sizeof(int) + attrLen)..];

                        while (!RespWriteUtils.TryWriteBulkString(attr, ref self.dcurr, self.dend))
                            self.SendAndReset();
                    }
                    else if (!remaininingAttributes.IsEmpty)
                    {
                        // Attributes fetched for filtering but not requested — advance reader
                        var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaininingAttributes);
                        remaininingAttributes = remaininingAttributes[(sizeof(int) + attrLen)..];
                    }

                    resultIndex++;
                    writtenCount++;
                }
            }
        }

        private bool NetworkVEMB<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            const int DefaultResultSetSize = 64;

            // VEMB key element [RAW]

            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count < 2 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments("VEMB");
            }

            ref var key = ref parseState.GetArgSliceByRef(0);
            var elem = parseState.GetArgSliceByRef(1);

            var raw = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).Span.EqualsUpperCaseSpanIgnoringCase("RAW"u8))
                {
                    return AbortWithErrorMessage("Unexpected option to VEMB");
                }

                raw = true;
            }

            if (raw)
            {
                // Write out the vector's quantized elements
                //
                // The quantization map (which is written as first element) is as:
                //  BIN, XBIN_I8, XBIN_U8  -> bin
                //  Q8, XNOQUANT_I8, XNOQUANT_U8 -> q8
                //  NOQUANT -> fp32
                //
                // The data string (written as second element) is whatever DiskANN stored
                // under the quantized vector context, EXCEPT for the *NOQUANT* quantizers in which
                // case it's the original vector.
                //
                // L2 norm (third element) and quantization range (fourth element for Q8) are dummy values
                // for now.

                Span<float> quantizedSpace = stackalloc float[DefaultResultSetSize];
                var quantizedResult = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(quantizedSpace));

                try
                {
                    var res = storageApi.VectorSetRawEmbedding(key, elem, ref quantizedResult, out var quantType, out var norm, out var range);

                    if (res == GarnetStatus.OK)
                    {
                        // Start array
                        if (quantType == VectorQuantType.Q8)
                        {
                            WriteArrayLength(4);
                        }
                        else
                        {
                            WriteArrayLength(3);
                        }

                        // Write quant type
                        if (quantType is VectorQuantType.Bin or VectorQuantType.XBin_I8 or VectorQuantType.XBin_U8)
                        {
                            WriteSimpleString("bin"u8);
                        }
                        else if (quantType is VectorQuantType.Q8 or VectorQuantType.XNoQuant_U8 or VectorQuantType.XNoQuant_I8)
                        {
                            WriteSimpleString("q8"u8);
                        }
                        else if (quantType == VectorQuantType.NoQuant)
                        {
                            WriteSimpleString("fp32");
                        }
                        else
                        {
                            throw new GarnetException($"Unexpected quantization type: {quantType}");
                        }

                        // Write raw data
                        WriteBulkString(quantizedResult.ReadOnlySpan);

                        // Write norm
                        if (respProtocolVersion == 3)
                        {
                            WriteDoubleNumeric(norm);
                        }
                        else
                        {
                            while (!RespWriteUtils.TryWriteDoubleBulkString(norm, ref dcurr, dend))
                                SendAndReset();
                        }

                        // For Q8 only write quantization range
                        if (quantType == VectorQuantType.Q8)
                        {
                            if (respProtocolVersion == 3)
                            {
                                WriteDoubleNumeric(range.Value);
                            }
                            else
                            {
                                while (!RespWriteUtils.TryWriteDoubleBulkString(range.Value, ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                    else if (res == GarnetStatus.WRONGTYPE)
                    {
                        return AbortVectorSetWrongType();
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }

                    return true;
                }
                finally
                {
                    quantizedResult.Dispose();
                }
            }
            else
            {
                // Write out the vector's elements
                //
                // In Redis this is reconstructed from quantized data, but with DiskANN we just have the real original values

                Span<float> distanceSpace = stackalloc float[DefaultResultSetSize];

                var distanceResult = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(distanceSpace));

                try
                {
                    var res = storageApi.VectorSetEmbedding(key, elem, ref distanceResult);

                    if (res == GarnetStatus.OK)
                    {
                        var distanceSpan = MemoryMarshal.Cast<byte, float>(distanceResult.ReadOnlySpan);

                        while (!RespWriteUtils.TryWriteArrayLength(distanceSpan.Length, ref dcurr, dend))
                            SendAndReset();

                        if (respProtocolVersion == 3)
                        {
                            for (var i = 0; i < distanceSpan.Length; i++)
                            {
                                while (!RespWriteUtils.TryWriteDoubleNumeric(distanceSpan[i], ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                        else
                        {
                            for (var i = 0; i < distanceSpan.Length; i++)
                            {
                                while (!RespWriteUtils.TryWriteDoubleBulkString(distanceSpan[i], ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                    else if (res == GarnetStatus.WRONGTYPE)
                    {
                        return AbortVectorSetWrongType();
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }

                    return true;
                }
                finally
                {
                    distanceResult.Dispose();
                }
            }
        }

        private bool NetworkVCARD<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVDIM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments("VDIM");

            var key = parseState.GetArgSliceByRef(0);

            var res = storageApi.VectorSetDimensions(key, out var dimensions);

            if (res == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteError("ERR Key not found"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else if (res == GarnetStatus.WRONGTYPE)
            {
                return AbortVectorSetWrongType();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt32(dimensions, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkVGETATTR<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("VGETATTR");
            }

            var key = parseState.GetArgSliceByRef(0);
            var element = parseState.GetArgSliceByRef(1);

            // Here we reserve some stack buffer to try to avoid allocations if the attributes are small
            // However, if it's not enough, VectorSetGetAttribute will allocate and replace attributesOutput
            // and attach a Memory to it - So we need to make sure to dispose of that if it happens
            Span<byte> attributesBuffer = stackalloc byte[256];
            SpanByteAndMemory attributesOutput = SpanByteAndMemory.FromPinnedSpan(attributesBuffer);

            try
            {
                var res = storageApi.VectorSetGetAttribute(key, element, ref attributesOutput);
                if (res != GarnetStatus.OK)
                {
                    if (res == GarnetStatus.NOTFOUND)
                    {
                        WriteNull();
                        return true;
                    }
                    else if (res == GarnetStatus.WRONGTYPE)
                    {
                        return AbortVectorSetWrongType();
                    }

                    return AbortWithErrorMessage($"Unexpected GarnetStatus: {res}");
                }

                WriteBulkString(attributesOutput.ReadOnlySpan);
                return true;
            }
            finally
            {
                attributesOutput.Memory?.Dispose();
            }
        }

        private bool NetworkVINFO<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("VINFO");
            }

            var key = parseState.GetArgSliceByRef(0);
            var res = storageApi.VectorSetInfo(key, out VectorQuantType quantType, out var distanceMetricType, out var vectorDimensions, out var reducedDimensions, out var buildExplorationFactor, out var numLinks, out var size);
            if (res != GarnetStatus.OK)
            {
                if (res == GarnetStatus.NOTFOUND)
                {
                    WriteNullArray();
                    return true;
                }
                else if (res == GarnetStatus.WRONGTYPE)
                {
                    return AbortVectorSetWrongType();
                }

                return AbortWithErrorMessage($"Unexpected GarnetStatus: {res}");
            }

            var quantTypeSpan = quantType switch
            {
                VectorQuantType.NoQuant => "f32"u8,
                VectorQuantType.Bin => "bin"u8,
                VectorQuantType.Q8 => "q8"u8,
                VectorQuantType.XNoQuant_U8 => "xnoquant_u8"u8,
                VectorQuantType.XNoQuant_I8 => "xnoquant_i8"u8,
                VectorQuantType.XBin_I8 => "xbin_i8"u8,
                VectorQuantType.XBin_U8 => "xbin_u8"u8,
                _ => throw new GarnetException($"Invalid VectorQuantType: {quantType}"),
            };

            var distanceMetricTypeSpan = distanceMetricType switch
            {
                VectorDistanceMetricType.Cosine => "cosine"u8,
                VectorDistanceMetricType.InnerProduct => "inner-product"u8,
                VectorDistanceMetricType.L2 => "l2"u8,
                VectorDistanceMetricType.XCosine_Normalized => "cosine-normalized"u8,
                _ => throw new GarnetException($"Invalid VectorDistanceMetricType: {distanceMetricType}"),
            };

            WriteArrayLength(14);
            WriteSimpleString("quant-type"u8);
            WriteSimpleString(quantTypeSpan);
            WriteSimpleString("distance-metric"u8);
            WriteSimpleString(distanceMetricTypeSpan);
            WriteSimpleString("input-vector-dimensions"u8);
            WriteInt32AsBulkString((int)vectorDimensions);
            WriteSimpleString("reduced-dimensions"u8);
            WriteInt32AsBulkString((int)reducedDimensions);
            WriteSimpleString("build-exploration-factor"u8);
            WriteInt32AsBulkString((int)buildExplorationFactor);
            WriteSimpleString("num-links"u8);
            WriteInt32AsBulkString((int)numLinks);
            WriteSimpleString("size"u8);
            WriteInt64AsBulkString(size);
            return true;
        }

        private bool NetworkVISMEMBER<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVLINKS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVRANDMEMBER<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVREM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("VREM");

            var key = parseState.GetArgSliceByRef(0);
            var elem = parseState.GetArgSliceByRef(1);

            var res = storageApi.VectorSetRemove(key, elem);

            if (res == GarnetStatus.WRONGTYPE)
            {
                return AbortVectorSetWrongType();
            }
            else
            {
                var resp = res == GarnetStatus.OK ? 1 : 0;

                while (!RespWriteUtils.TryWriteInt32(resp, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkVSETATTR<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storageSession.vectorManager.IsEnabled)
            {
                return AbortWithErrorMessage("ERR Vector Set (preview) commands are not enabled");
            }

            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool AbortVectorSetWrongType()
        {
            // Matches Redis behavior - doesn't indicate the type involved
            while (!RespWriteUtils.TryWriteError("WRONGTYPE Operation against a key holding the wrong kind of value"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}