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
                        return AbortWithErrorMessage("FP32 values must be multiple of 4-bytes in size");
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
                        return AbortWithWrongNumberOfArguments("VADD");
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

                if (curIx >= parseState.Count)
                {
                    return AbortWithWrongNumberOfArguments("VADD");
                }

                var element = parseState.GetArgSliceByRef(curIx);
                curIx++;

                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("CAS"u8))
                    {
                        // We ignore CAS
                        curIx++;
                    }
                }

                VectorQuantType quantType;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("NOQUANT"u8))
                    {
                        quantType = VectorQuantType.NoQuant;
                        curIx++;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("Q8"u8))
                    {
                        quantType = VectorQuantType.Q8;
                        curIx++;
                    }
                    else if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("BIN"u8))
                    {
                        quantType = VectorQuantType.Bin;
                        curIx++;
                    }
                    else
                    {
                        return AbortWithErrorMessage("Unrecogized quantization"u8);
                    }
                }
                else
                {
                    quantType = VectorQuantType.Invalid;
                }

                var buildExplorationFactor = 0;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("EF"u8))
                    {
                        curIx++;

                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VADD");
                        }

                        if (!parseState.TryGetInt(curIx, out buildExplorationFactor) || buildExplorationFactor <= 0)
                        {
                            return AbortWithErrorMessage("EF must be > 0");
                        }

                        curIx++;
                    }
                }

                ArgSlice attributes = default;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("SETATTR"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VADD");
                        }

                        attributes = parseState.GetArgSliceByRef(curIx);
                        curIx++;

                        // TODO: Validate attributes
                    }
                }

                var numLinks = 0;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).Span.EqualsUpperCaseSpanIgnoringCase("M"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VADD");
                        }

                        if (!parseState.TryGetInt(curIx, out numLinks) || numLinks <= 0)
                        {
                            return AbortWithErrorMessage("M must be > 0");
                        }

                        curIx++;
                    }
                }

                if (parseState.Count != curIx)
                {
                    return AbortWithWrongNumberOfArguments("VADD");
                }

                var res = storageApi.VectorSetAdd(key, reduceDim, values, element, quantType, buildExplorationFactor, attributes, numLinks, out var result);

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
                        while (!RespWriteUtils.TryWriteError("VADD parameters did not match Vector Set construction parameters"u8, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError($"Unexpected GarnetStatus: {res}", ref dcurr, dend))
                        SendAndReset();
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

                var withScores = false;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("WITHSCORES"u8))
                    {
                        withScores = true;
                        curIx++;
                    }
                }

                var withAttributes = false;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("WITHATTRIBS"u8))
                    {
                        withAttributes = true;
                        curIx++;
                    }
                }

                var count = 0;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("COUNT"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out count) || count < 0)
                        {
                            return AbortWithErrorMessage("COUNT must be integer >= 0");
                        }
                        curIx++;
                    }
                }

                var delta = 0f;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("EPSILON"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetFloat(curIx, out delta) || delta <= 0)
                        {
                            return AbortWithErrorMessage("EPSILON must be float > 0");
                        }
                        curIx++;
                    }
                }

                var searchExplorationFactor = 0;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("EF"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out searchExplorationFactor) || searchExplorationFactor < 0)
                        {
                            return AbortWithErrorMessage("EF must be >= 0");
                        }
                        curIx++;
                    }
                }

                ReadOnlySpan<byte> filter = default;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FILTER"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        filter = parseState.GetArgSliceByRef(curIx).ReadOnlySpan;
                        curIx++;

                        // TODO: validate filter
                    }
                }

                var maxFilteringEffort = 0;
                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FILTER-EF"u8))
                    {
                        curIx++;
                        if (curIx >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("VSIM");
                        }

                        if (!parseState.TryGetInt(curIx, out maxFilteringEffort) || maxFilteringEffort < 0)
                        {
                            return AbortWithErrorMessage("FILTER-EF must be >= 0");
                        }
                        curIx++;
                    }
                }

                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("TRUTH"u8))
                    {
                        // TODO: should we implement TRUTH?
                        curIx++;
                    }
                }

                if (curIx < parseState.Count)
                {
                    if (parseState.GetArgSliceByRef(curIx).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("NOTHREAD"u8))
                    {
                        // We ignore NOTHREAD
                        curIx++;
                    }
                }

                if (curIx != parseState.Count)
                {
                    return AbortWithWrongNumberOfArguments("VSIM");
                }

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
                        res = storageApi.VectorSetValueSimilarity(key, values, count, delta, searchExplorationFactor, filter, maxFilteringEffort, ref idResult, ref distanceResult, out vectorRes);
                    }
                    else
                    {
                        res = storageApi.VectorSetElementSimilarity(key, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, ref idResult, ref distanceResult, out vectorRes);
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
                                if (withScores)
                                {
                                    arrayItemCount += distancesSpan.Length;
                                }
                                if (withAttributes)
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

                                    if (withScores)
                                    {
                                        var distance = distancesSpan[resultIndex];

                                        while (!RespWriteUtils.TryWriteDoubleBulkString(distance, ref dcurr, dend))
                                            SendAndReset();
                                    }

                                    if (withAttributes)
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
                var res = storageApi.VectorEmbedding(key, elem, ref distanceResult);

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
            // TODO: implement!

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

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
