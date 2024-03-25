// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
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
            public (double, double) Coords;
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
            public bool SortOrder { get; set; }
            public bool WithCount { get; set; }
            public int WithCountValue { get; set; }
            public bool WithCountAny { get; set; }
            public bool WithCoord { get; set; }
            public bool WithDist { get; set; }
            public bool WithHash { get; set; }
        }

        private void GeoAdd(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            int count = _input->count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            // By default add new elements but do not update the ones already in the set
            bool nx = true;

            bool ch = false;

            // Read the options
            var optsCount = count % 3;
            if (optsCount > 0 && optsCount <= 2)
            {
                // Is NX or XX, if not nx then use XX
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var byteOptions, ref input_currptr, input + length))
                    return;
                nx = (byteOptions.Length == 2 && (byteOptions[0] == (int)'N' && byteOptions[1] == (int)'X') || (byteOptions[0] == (int)'n' && byteOptions[1] == (int)'x'));
                if (optsCount == 2)
                {
                    // Read CH option
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byteOptions, ref input_currptr, input + length))
                        return;
                    ch = (byteOptions.Length == 2 && (byteOptions[0] == (int)'C' && byteOptions[1] == (int)'H') || (byteOptions[0] == (int)'c' && byteOptions[1] == (int)'h'));
                }
                count -= optsCount;
            }

            int elementsChanged = 0;

            for (int c = 0; c < count / 3; c++)
            {
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var longitude, out var parsed, ref input_currptr, input + length))
                    return;
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var latitude, out parsed, ref input_currptr, input + length))
                    return;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref input_currptr, input + length))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                if (parsed)
                {
                    var score = server.GeoHash.GeoToLongValue(latitude, longitude);
                    if (score != -1)
                    {
                        if (!sortedSetDict.TryGetValue(member, out double scoreStored))
                        {
                            if (nx)
                            {
                                sortedSetDict.Add(member, score);
                                sortedSet.Add((score, member));
                                _output->opsDone++;

                                this.UpdateSize(member);
                            }
                        }
                        else if (!nx && scoreStored != score)
                        {
                            sortedSetDict[member] = score;
                            var success = sortedSet.Remove((scoreStored, member));
                            Debug.Assert(success);
                            success = sortedSet.Add((score, member));
                            Debug.Assert(success);
                            elementsChanged++;
                        }
                    }
                }
                _output->opsDone = ch ? elementsChanged : _output->opsDone;
            }

            // Write output
            _output->bytesDone = (int)(input_currptr - input_startptr);
        }

        private void GeoHash(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int prevDone = _input->done; // how many were previously done
            int count = _input->count;
            int countDone = 0;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

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
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref input_currptr, input + length))
                        break;

                    countDone++;
                    if (countDone <= prevDone) // skip processing previously done entries
                        continue;

                    _output.countDone++;

                    // Write output length when we have at least one item to report
                    if (countDone == 1)
                    {
                        while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    if (sortedSetDict.TryGetValue(member, out var value52Int))
                    {
                        var geohash = Encoding.ASCII.GetBytes(server.GeoHash.GetGeoHashCode((long)value52Int));
                        while (!RespWriteUtils.WriteBulkString(geohash, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoDistance(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int prevDone = _input->done; // how many were previously done
            int count = _input->count;
            int countDone = 0;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // Read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member1ByteArray, ref input_currptr, input + length))
                    return;

                // Read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member2ByteArray, ref input_currptr, input + length))
                    return;

                Byte[] units = "M"u8.ToArray();

                // Read units
                if (count > 2)
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out units, ref input_currptr, input + length))
                        return;
                }

                if (sortedSetDict.TryGetValue(member1ByteArray, out double scoreMember1) && sortedSetDict.TryGetValue(member2ByteArray, out double scoreMember2))
                {

                    (double lat, double lon) = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);
                    (double lat, double lon) cord2 = server.GeoHash.GetCoordinatesFromLong((long)scoreMember2);

                    var distance = server.GeoHash.Distance(lat, lon, cord2.lat, cord2.lon);

                    var distanceValue = (units.Length == 1 && (units[0] == (int)'M' || units[0] == (int)'m')) ?
                                        distance :
                                        server.GeoHash.ConvertMetersToUnits(distance, units);

                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(distanceValue.ToString()), ref curr, end))
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

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoPosition(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int prevDone = _input->done;
            int count = _input->count;
            int countDone = 0;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

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
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var memberByteArray, ref input_currptr, input + length))
                        break;

                    countDone++;
                    if (countDone <= prevDone)  // skip previously processed entries
                        continue;

                    _output.countDone++;

                    // Write output length when we have at least one item to report
                    if (countDone == 1)
                    {
                        while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    if (sortedSetDict.TryGetValue(memberByteArray, out double scoreMember1))
                    {
                        (double lat, double lon) = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);

                        // write array of 2 values
                        while (!RespWriteUtils.WriteArrayLength(2, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(lon.ToString()), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(lat.ToString()), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNullArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoSearch(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int prevDone = _input->done;
            int count = _input->count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var opts = new GeoSearchOptions();
                var opsStr = "FROMMEMBERFROMLONLATBYRADIUSBYBOXASCDESCCOUNTANYWITHCOORDWITHDISTWITHHASH";
                byte[] fromMember = null;
                byte[] byBoxUnits = "M"u8.ToArray();
                var byRadiusUnits = byBoxUnits;
                double width = 0, height = 0;
                int countValue = 0;

                // Read the options
                while (count > 0)
                {
                    // Read token
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var tokenByteArray, ref input_currptr, input + length))
                        return;
                    var stringToken = Encoding.ASCII.GetString(tokenByteArray).ToUpperInvariant();
                    if (opsStr.IndexOf(stringToken) != -1)
                    {
                        switch (stringToken)
                        {
                            case "FROMMEMBER":
                                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out fromMember, ref input_currptr, input + length))
                                    return;
                                opts.FromMember = true;
                                --count;
                                break;
                            case "FROMLONLAT":
                                // Read two coord
                                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var longitude, out var parsed, ref input_currptr, input + length))
                                    return;
                                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var latitude, out parsed, ref input_currptr, input + length))
                                    return;
                                count -= 2;
                                opts.FromLonLat = true;
                                break;
                            case "BYRADIUS":
                                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var radius, out parsed, ref input_currptr, input + length))
                                    return;
                                // Read units
                                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byRadiusUnits, ref input_currptr, input + length))
                                    return;
                                count -= 2;
                                opts.ByRadius = true;
                                break;
                            case "BYBOX":
                                if (!RespReadUtils.ReadDoubleWithLengthHeader(out width, out parsed, ref input_currptr, input + length))
                                    return;
                                if (!RespReadUtils.ReadDoubleWithLengthHeader(out height, out parsed, ref input_currptr, input + length))
                                    return;
                                // Read units
                                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byBoxUnits, ref input_currptr, input + length))
                                    return;
                                count -= 3;
                                opts.ByBox = true;
                                break;
                            case "ASC":
                                opts.SortOrder = false;
                                break;
                            case "DESC":
                                opts.SortOrder = true;
                                break;
                            case "COUNT":
                                opts.WithCount = true;
                                if (!RespReadUtils.ReadIntWithLengthHeader(out countValue, ref input_currptr, input + length))
                                    return;
                                count -= 1;
                                break;
                            case "WITHCOORD":
                                opts.WithCoord = true;
                                break;
                            case "WITHDIST":
                                opts.WithDist = true;
                                break;
                            case "WITHHASH":
                                opts.WithHash = true;
                                break;
                            case "ANY":
                                opts.WithCountAny = true;
                                break;
                            default:
                                break;
                        }
                        --count;
                    }
                }

                // Check that we have the mandatory options
                if (!((opts.FromMember || opts.FromLonLat) && (opts.ByRadius || opts.ByBox)))
                {
                    var errorMessage = Encoding.ASCII.GetBytes($"-ERR required parameters are missing.\r\n");
                    while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _input->count = 0;
                    count = 0;
                }

                // Get the results
                // FROMMEMBER
                if (opts.FromMember && sortedSetDict.TryGetValue(fromMember, out var centerPointScore))
                {
                    (double lat, double lon) = server.GeoHash.GetCoordinatesFromLong((long)centerPointScore);

                    if (opts.ByRadius)
                    {
                        // Not supported in Garnet: ByRadius
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref curr, end))
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
                                        Coords = server.GeoHash.GetCoordinatesFromLong((long)point.Item1)
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

                            while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(distanceValue.ToString()), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            // Write array of 2 values
                            while (!RespWriteUtils.WriteArrayLength(2, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(item.Coords.Item2.ToString()), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(item.Coords.Item1.ToString()), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }

                    }
                }

                // Not supported options in Garnet: FROMLONLAT BYBOX BYRADIUS 
                if (opts.FromLonLat)
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = _input->count - count;
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