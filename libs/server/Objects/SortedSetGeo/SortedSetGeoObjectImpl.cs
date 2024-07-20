// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sorted Set - RESP specific operations for GEO Commands
    /// </summary>
    public unsafe partial class SortedSetObject : GarnetObjectBase
    {
        /// <summary>
        /// Use this struct for the reply of GEOSEARCH command
        /// </summary>
        private struct GeoSearchData
        {
            public Byte[] Member;
            public double Distance;
            public string GeoHashCode;
            public (double Latitude, double Longitude) Coordinates;
        }

        /// <summary>
        /// Small struct to store options for GEOSEARCH command
        /// </summary>
        private struct GeoSearchOptions
        {
            public bool FromMember { get; set; }
            public bool FromLonLat { get; set; }
            public bool ByRadius { get; set; }
            public bool ByBox { get; set; }
            public bool SortDescending { get; set; }
            public bool WithCount { get; set; }
            public int WithCountValue { get; set; }
            public bool WithCountAny { get; set; }
            public bool WithCoord { get; set; }
            public bool WithDist { get; set; }
            public bool WithHash { get; set; }
        }

        private void GeoAdd(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var count = input.count;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            // By default add new elements but do not update the ones already in the set
            var nx = true;
            var ch = false;

            // Read the options
            var optsCount = count % 3;
            if (optsCount > 0 && optsCount <= 2)
            {
                // Is NX or XX, if not nx then use XX
                if (!RespReadUtils.TrySliceWithLengthHeader(out var byteOptions, ref input_currptr, input_endptr))
                    return;
                nx = byteOptions.EqualsUpperCaseSpanIgnoringCase("NX"u8);
                if (optsCount == 2)
                {
                    // Read CH option
                    if (!RespReadUtils.TrySliceWithLengthHeader(out byteOptions, ref input_currptr, input_endptr))
                        return;
                    ch = byteOptions.EqualsUpperCaseSpanIgnoringCase("CH"u8);
                }
                count -= optsCount;
            }

            int elementsChanged = 0;

            for (int c = 0; c < count / 3; c++)
            {
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var longitude, out var parsed, ref input_currptr, input_endptr))
                    return;
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var latitude, out parsed, ref input_currptr, input_endptr))
                    return;
                if (!RespReadUtils.TrySliceWithLengthHeader(out var member, ref input_currptr, input_endptr))
                    return;

                if (parsed)
                {
                    var score = server.GeoHash.GeoToLongValue(latitude, longitude);
                    if (score != -1)
                    {
                        var memberByteArray = member.ToArray();
                        if (!sortedSetDict.TryGetValue(memberByteArray, out var scoreStored))
                        {
                            if (nx)
                            {
                                sortedSetDict.Add(memberByteArray, score);
                                sortedSet.Add((score, memberByteArray));
                                _output->result1++;

                                this.UpdateSize(member);
                                elementsChanged++;
                            }
                        }
                        else if (!nx && scoreStored != score)
                        {
                            sortedSetDict[memberByteArray] = score;
                            var success = sortedSet.Remove((scoreStored, memberByteArray));
                            Debug.Assert(success);
                            success = sortedSet.Add((score, memberByteArray));
                            Debug.Assert(success);
                            elementsChanged++;
                        }
                    }
                }
                _output->result1 = ch ? elementsChanged : _output->result1;
            }
        }

        private void GeoHash(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.count;
            var countDone = 0;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (count == 0)
                {
                    while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                while (countDone < count)
                {
                    // Read member
                    if (!RespReadUtils.TrySliceWithLengthHeader(out var member, ref input_currptr, input_endptr))
                        break;

                    countDone++;
                    _output.result1++;

                    // Write output length when we have at least one item to report
                    if (countDone == 1)
                    {
                        while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    if (sortedSetDict.TryGetValue(member.ToArray(), out var value52Int))
                    {
                        var geoHash = server.GeoHash.GetGeoHashCode((long)value52Int);
                        while (!RespWriteUtils.WriteAsciiBulkString(geoHash, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoDistance(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.count;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // Read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member1ByteArray, ref input_currptr, input_endptr))
                    return;

                // Read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member2ByteArray, ref input_currptr, input_endptr))
                    return;

                var units = "M"u8;

                // Read units
                if (count > 2)
                {
                    if (!RespReadUtils.TrySliceWithLengthHeader(out units, ref input_currptr, input_endptr))
                        return;
                }

                var countDone = 0;
                if (sortedSetDict.TryGetValue(member1ByteArray, out var scoreMember1) && sortedSetDict.TryGetValue(member2ByteArray, out var scoreMember2))
                {
                    var first = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);
                    var second = server.GeoHash.GetCoordinatesFromLong((long)scoreMember2);

                    var distance = server.GeoHash.Distance(first.Latitude, first.Longitude, second.Latitude, second.Longitude);

                    var distanceValue = (units.Length == 1 && AsciiUtils.ToUpper(units[0]) == (byte)'M') ?
                        distance : server.GeoHash.ConvertMetersToUnits(distance, units);

                    while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    countDone = count;
                }
                else
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    // There was no operation done but tokens were processed
                    countDone = count;
                }
                _output.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoPosition(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.count;
            var countDone = 0;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                if (count == 0)
                {
                    while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                while (countDone < count)
                {
                    // read member
                    if (!RespReadUtils.TrySliceWithLengthHeader(out var memberBytes, ref input_currptr, input_endptr))
                        break;

                    countDone++;
                    _output.result1++;

                    // Write output length when we have at least one item to report
                    if (countDone == 1)
                    {
                        while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    if (sortedSetDict.TryGetValue(memberBytes.ToArray(), out var scoreMember1))
                    {
                        var (lat, lon) = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);

                        // write array of 2 values
                        while (!RespWriteUtils.WriteArrayLength(2, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(lon, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(lat, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNullArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoSearch(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.count;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var opts = new GeoSearchOptions();
                byte[] fromMember = null;
                var byBoxUnits = "M"u8;
                var byRadiusUnits = byBoxUnits;
                double width = 0, height = 0;
                var countValue = 0;

                // Read the options
                while (count > 0)
                {
                    // Read token
                    if (!RespReadUtils.TrySliceWithLengthHeader(out var tokenBytes, ref input_currptr, input_endptr))
                        return;

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMMEMBER"u8))
                    {
                        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out fromMember, ref input_currptr, input_endptr))
                            return;
                        opts.FromMember = true;
                        --count;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                    {
                        // Read coordinates
                        if (!RespReadUtils.ReadDoubleWithLengthHeader(out var longitude, out var parsed, ref input_currptr, input_endptr) ||
                            !RespReadUtils.ReadDoubleWithLengthHeader(out var latitude, out parsed, ref input_currptr, input_endptr))
                        {
                            return;
                        }
                        count -= 2;
                        opts.FromLonLat = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                    {
                        // Read radius and units
                        if (!RespReadUtils.ReadDoubleWithLengthHeader(out var radius, out var parsed, ref input_currptr, input_endptr) ||
                            !RespReadUtils.TrySliceWithLengthHeader(out byRadiusUnits, ref input_currptr, input_endptr))
                        {
                            return;
                        }
                        count -= 2;
                        opts.ByRadius = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        // Read width, height & units
                        if (!RespReadUtils.ReadDoubleWithLengthHeader(out width, out var parsed, ref input_currptr, input_endptr) ||
                            !RespReadUtils.ReadDoubleWithLengthHeader(out height, out parsed, ref input_currptr, input_endptr) ||
                            !RespReadUtils.TrySliceWithLengthHeader(out byBoxUnits, ref input_currptr, input_endptr))
                        {
                            return;
                        }
                        count -= 3;
                        opts.ByBox = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ASC"u8)) opts.SortDescending = false;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("DESC"u8)) opts.SortDescending = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("COUNT"u8))
                    {
                        opts.WithCount = true;
                        if (!RespReadUtils.ReadIntWithLengthHeader(out countValue, ref input_currptr, input_endptr))
                            return;
                        count -= 1;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHCOORD"u8)) opts.WithCoord = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHDIST"u8)) opts.WithDist = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHHASH"u8)) opts.WithHash = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ANY"u8)) opts.WithCountAny = true;

                    --count;
                }

                // Check that we have the mandatory options
                if (!((opts.FromMember || opts.FromLonLat) && (opts.ByRadius || opts.ByBox)))
                {
                    while (!RespWriteUtils.WriteError("ERR required parameters are missing."u8, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    count = 0;
                }

                // Get the results
                // FROMMEMBER
                if (opts.FromMember && sortedSetDict.TryGetValue(fromMember, out var centerPointScore))
                {
                    var (lat, lon) = server.GeoHash.GetCoordinatesFromLong((long)centerPointScore);

                    if (opts.ByRadius)
                    {
                        // Not supported in Garnet: ByRadius
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        var responseData = new List<GeoSearchData>();
                        foreach (var point in sortedSet)
                        {
                            var coorInItem = server.GeoHash.GetCoordinatesFromLong((long)point.Item1);
                            double distance = 0;
                            if (opts.ByBox)
                            {
                                if (server.GeoHash.GetDistanceWhenInRectangle(server.GeoHash.ConvertValueToMeters(width, byBoxUnits), server.GeoHash.ConvertValueToMeters(height, byBoxUnits), lat, lon, coorInItem.Item1, coorInItem.Item2, ref distance))
                                {
                                    // The item is inside the shape
                                    responseData.Add(new GeoSearchData()
                                    {
                                        Member = point.Item2,
                                        Distance = distance,
                                        GeoHashCode = server.GeoHash.GetGeoHashCode((long)point.Item1),
                                        Coordinates = server.GeoHash.GetCoordinatesFromLong((long)point.Item1)
                                    });

                                    if (opts.WithCount && responseData.Count == countValue)
                                        break;
                                }
                            }
                        }

                        // Write results 
                        while (!RespWriteUtils.WriteArrayLength(responseData.Count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        foreach (var item in responseData)
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Member, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            var distanceValue = (byBoxUnits.Length == 1 && (byBoxUnits[0] == (int)'M' || byBoxUnits[0] == (int)'m')) ? item.Distance
                                                : server.GeoHash.ConvertMetersToUnits(item.Distance, byBoxUnits);

                            while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            // Write array of 2 values
                            while (!RespWriteUtils.WriteArrayLength(2, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            while (!RespWriteUtils.TryWriteDoubleBulkString(item.Coordinates.Longitude, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            while (!RespWriteUtils.TryWriteDoubleBulkString(item.Coordinates.Latitude, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                    }
                }

                // Not supported options in Garnet: FROMLONLAT BYBOX BYRADIUS 
                if (opts.FromLonLat)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                _output.result1 = input.count - count;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}