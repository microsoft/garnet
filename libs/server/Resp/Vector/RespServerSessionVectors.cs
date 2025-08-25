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
            // VADD key [REDUCE dim] (FP32 | VALUES num) vector element [CAS] [NOQUANT | Q8 | BIN] [EF build-exploration-factor] [SETATTR attributes] [M numlinks]

            const int MinM = 4;
            const int MaxM = 4_096;

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

            float[] rentedValues = null;
            Span<float> values = stackalloc float[64];

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

                    values = MemoryMarshal.Cast<byte, float>(asBytes);
                }
                else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("VALUES"u8))
                {
                    curIx++;
                    if (curIx >= parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    if (!parseState.TryGetInt(curIx, out var valueCount) || valueCount <= 0)
                    {
                        return AbortWithErrorMessage("ERR invalid vector specification");
                    }
                    curIx++;

                    if (valueCount > values.Length)
                    {
                        values = rentedValues = ArrayPool<float>.Shared.Rent(valueCount);
                    }
                    values = values[..valueCount];

                    if (curIx + valueCount > parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments("VADD");
                    }

                    for (var valueIx = 0; valueIx < valueCount; valueIx++)
                    {
                        if (!parseState.TryGetFloat(curIx, out values[valueIx]))
                        {
                            return AbortWithErrorMessage("ERR invalid vector specification");
                        }

                        curIx++;
                    }
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
                ArgSlice? attributes = null;
                int? numLinks = null;

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

                        if (!parseState.TryGetInt(curIx, out var buildExplorationFactorNonNull) || buildExplorationFactorNonNull <= 0)
                        {
                            return AbortWithErrorMessage("ERR invalid EF");
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

                        // TODO: Validate attributes

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
                            return AbortWithErrorMessage("ERR invalid M");
                        }

                        numLinks = numLinksNonNull;
                        curIx++;

                        continue;
                    }

                    // Didn't recognize this option, error out
                    return AbortWithErrorMessage("ERR invalid option after element");
                }

                // Default unspecified options
                quantType ??= VectorQuantType.Q8;
                buildExplorationFactor ??= 200;
                attributes ??= default;
                numLinks ??= 16;

                var res = storageApi.VectorSetAdd(key, reduceDim, values, element, quantType.Value, buildExplorationFactor.Value, attributes.Value, numLinks.Value, out var result);

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
                        return AbortWithErrorMessage("ERR asked quantization mismatch with existing vector set"u8);
                    }
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
                    ArrayPool<float>.Shared.Return(rentedValues);
                }
            }
        }

        private bool NetworkVSIM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            const int DefaultResultSetSize = 64;
            const int DefaultIdSize = sizeof(ulong);

            // VSIM key (ELE | FP32 | VALUES num) (vector | element) [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF search-exploration - factor] [FILTER expression][FILTER-EF max - filtering - effort] [TRUTH][NOTHREAD]

            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments("VSIM");
            }

            ref var key = ref parseState.GetArgSliceByRef(0);
            var kind = parseState.GetArgSliceByRef(1);

            var curIx = 2;

            ReadOnlySpan<byte> element;

            float[] rentedValues = null;
            try
            {
                Span<float> values = stackalloc float[64];
                if (kind.Span.EqualsUpperCaseSpanIgnoringCase("ELE"u8))
                {
                    element = parseState.GetArgSliceByRef(curIx).ReadOnlySpan;
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

                        values = MemoryMarshal.Cast<byte, float>(asBytes);
                        curIx++;
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
                        curIx++;

                        if (valueCount > values.Length)
                        {
                            values = rentedValues = ArrayPool<float>.Shared.Rent(valueCount);
                        }
                        values = values[..valueCount];

                        if (curIx + valueCount > parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        for (var valueIx = 0; valueIx < valueCount; valueIx++)
                        {
                            if (!parseState.TryGetFloat(curIx, out values[valueIx]))
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
                ArgSlice? filter = null;
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

                        if (!parseState.TryGetInt(curIx, out var countNonNull) || countNonNull < 0)
                        {
                            return AbortWithErrorMessage("COUNT must be integer >= 0");
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

                        if (!parseState.TryGetInt(curIx, out var searchExplorationFactorNonNull) || searchExplorationFactorNonNull < 0)
                        {
                            return AbortWithErrorMessage("EF must be >= 0");
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

                        if (!parseState.TryGetInt(curIx, out var maxFilteringEffortNonNull) || maxFilteringEffortNonNull < 0)
                        {
                            return AbortWithErrorMessage("FILTER-EF must be >= 0");
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
                maxFilteringEffort ??= count.Value * 100;

                Span<byte> idSpace = stackalloc byte[(DefaultResultSetSize * DefaultIdSize) + (DefaultResultSetSize * sizeof(int))];
                Span<float> distanceSpace = stackalloc float[DefaultResultSetSize];

                SpanByteAndMemory idResult = SpanByteAndMemory.FromPinnedSpan(idSpace);
                SpanByteAndMemory distanceResult = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(distanceSpace));
                try
                {

                    GarnetStatus res;
                    VectorManagerResult vectorRes;
                    if (element.IsEmpty)
                    {
                        res = storageApi.VectorSetValueSimilarity(key, values, count.Value, delta.Value, searchExplorationFactor.Value, filter.Value.ReadOnlySpan, maxFilteringEffort.Value, ref idResult, ref distanceResult, out vectorRes);
                    }
                    else
                    {
                        res = storageApi.VectorSetElementSimilarity(key, element, count.Value, delta.Value, searchExplorationFactor.Value, filter.Value.ReadOnlySpan, maxFilteringEffort.Value, ref idResult, ref distanceResult, out vectorRes);
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
                                // TODO: this is rather complicated, so punt for now
                                throw new NotImplementedException();
                            }
                            else
                            {
                                var remainingIds = idResult.AsReadOnlySpan();
                                var distancesSpan = MemoryMarshal.Cast<byte, float>(distanceResult.AsReadOnlySpan());

                                var arrayItemCount = distancesSpan.Length;
                                if (withScores.Value)
                                {
                                    arrayItemCount += distancesSpan.Length;
                                }
                                if (withAttributes.Value)
                                {
                                    throw new NotImplementedException();
                                }

                                while (!RespWriteUtils.TryWriteArrayLength(arrayItemCount, ref dcurr, dend))
                                    SendAndReset();

                                for (var resultIndex = 0; resultIndex < distancesSpan.Length; resultIndex++)
                                {
                                    var elementLen = BinaryPrimitives.ReadInt32LittleEndian(remainingIds);
                                    var elementData = remainingIds.Slice(sizeof(int), elementLen);
                                    remainingIds = remainingIds[(sizeof(int) + elementLen)..];

                                    while (!RespWriteUtils.TryWriteBulkString(elementData, ref dcurr, dend))
                                        SendAndReset();

                                    if (withScores.Value)
                                    {
                                        var distance = distancesSpan[resultIndex];

                                        while (!RespWriteUtils.TryWriteDoubleBulkString(distance, ref dcurr, dend))
                                            SendAndReset();
                                    }

                                    if (withAttributes.Value)
                                    {
                                        throw new NotImplementedException();
                                    }
                                }
                            }
                        }
                        else
                        {
                            throw new GarnetException($"Unexpected {nameof(VectorManagerResult)}: {vectorRes}");
                        }
                    }
                    else
                    {
                        throw new GarnetException($"Unexpected {nameof(GarnetStatus)}: {res}");
                    }

                    return true;
                }
                finally
                {
                    if (!idResult.IsSpanByte)
                    {
                        idResult.Memory.Dispose();
                    }

                    if (!distanceResult.IsSpanByte)
                    {
                        distanceResult.Memory.Dispose();
                    }
                }
            }
            finally
            {
                if (rentedValues != null)
                {
                    ArrayPool<float>.Shared.Return(rentedValues);
                }
            }
        }

        private bool NetworkVEMB<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            const int DefaultResultSetSize = 64;

            // VEMB key element [RAW]

            if (parseState.Count < 2 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments("VEMB");
            }

            ref var key = ref parseState.GetArgSliceByRef(0);
            var elem = parseState.GetArgSliceByRef(1).ReadOnlySpan;

            var raw = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).Span.EqualsUpperCaseSpanIgnoringCase("RAW"u8))
                {
                    return AbortWithErrorMessage("Unexpected option to VSIM");
                }

                raw = true;
            }

            // TODO: what do we do here?
            if (raw)
            {
                throw new NotImplementedException();
            }

            Span<float> distanceSpace = stackalloc float[DefaultResultSetSize];

            var distanceResult = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(distanceSpace));

            try
            {
                var res = storageApi.VectorSetEmbedding(key, elem, ref distanceResult);

                if (res == GarnetStatus.OK)
                {
                    var distanceSpan = MemoryMarshal.Cast<byte, float>(distanceResult.AsReadOnlySpan());

                    while (!RespWriteUtils.TryWriteArrayLength(distanceSpan.Length, ref dcurr, dend))
                        SendAndReset();

                    for (var i = 0; i < distanceSpan.Length; i++)
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(distanceSpan[i], ref dcurr, dend))
                            SendAndReset();
                    }
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
                if (!distanceResult.IsSpanByte)
                {
                    distanceResult.Memory.Dispose();
                }
            }
        }

        private bool NetworkVCARD<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVDIM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
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
                while (!RespWriteUtils.TryWriteError("ERR Not a Vector Set"u8, ref dcurr, dend))
                    SendAndReset();
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
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVINFO<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVLINKS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVRANDMEMBER<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVREM<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkVSETATTR<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}
