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

        private void GeoAdd(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            // By default, add new elements but do not update the ones already in the set
            var nx = true;
            var ch = false;

            var count = input.parseState.Count;
            var currTokenIdx = 0;

            ObjectOutputHeader _output = default;
            try
            {
                // Read the options
                var optsCount = count % 3;
                if (optsCount > 0 && optsCount <= 2)
                {
                    // Is NX or XX, if not nx then use XX
                    var byteOptions = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                    nx = byteOptions.EqualsUpperCaseSpanIgnoringCase("NX"u8);
                    if (optsCount == 2)
                    {
                        // Read CH option
                        byteOptions = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                        ch = byteOptions.EqualsUpperCaseSpanIgnoringCase("CH"u8);
                    }
                }

                var elementsAdded = 0;
                var elementsChanged = 0;

                while (currTokenIdx < count)
                {
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out var longitude) ||
                        !input.parseState.TryGetDouble(currTokenIdx++, out var latitude))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                ref end);
                        return;
                    }

                    var member = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

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
                                elementsAdded++;

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

                while (!RespWriteUtils.WriteInteger(ch ? elementsChanged : elementsAdded, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoHash(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.WriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    // Read member
                    var member = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (sortedSetDict.TryGetValue(member, out var value52Int))
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
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // Read 1st member
                var member1 = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

                // Read 2nd member
                var member2 = input.parseState.GetArgSliceByRef(1).SpanByte.ToByteArray();

                // Read units
                var units = input.parseState.Count > 2
                    ? input.parseState.GetArgSliceByRef(2).ReadOnlySpan
                    : "M"u8;

                if (sortedSetDict.TryGetValue(member1, out var scoreMember1) && sortedSetDict.TryGetValue(member2, out var scoreMember2))
                {
                    var first = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);
                    var second = server.GeoHash.GetCoordinatesFromLong((long)scoreMember2);

                    var distance = server.GeoHash.Distance(first.Latitude, first.Longitude, second.Latitude, second.Longitude);

                    var distanceValue = (units.Length == 1 && AsciiUtils.ToUpper(units[0]) == (byte)'M') ?
                        distance : server.GeoHash.ConvertMetersToUnits(distance, units);

                    while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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

        private void GeoPosition(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.WriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    // read member
                    var member = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (sortedSetDict.TryGetValue(member, out var scoreMember1))
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
                double width = 0, height = 0;
                var countValue = 0;

                ReadOnlySpan<byte> errorMessage = default;
                var argNumError = false;

                var currTokenIdx = 0;

                // Read the options
                while (currTokenIdx < input.parseState.Count)
                {
                    // Read token
                    var tokenBytes = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMMEMBER"u8))
                    {
                        if (input.parseState.Count - currTokenIdx == 0)
                        {
                            argNumError = true;
                            break;
                        }

                        fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                        opts.FromMember = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                    {
                        if (input.parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read coordinates
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out _) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out _))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        opts.FromLonLat = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                    {
                        if (input.parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read radius and units
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out _))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        opts.ByRadius = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        if (input.parseState.Count - currTokenIdx < 3)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read width, height
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out width) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out height))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        // Read units
                        byBoxUnits = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                        opts.ByBox = true;
                    }
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ASC"u8)) opts.SortDescending = false;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("DESC"u8)) opts.SortDescending = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("COUNT"u8))
                    {
                        if (input.parseState.Count - currTokenIdx == 0)
                        {
                            argNumError = true;
                            break;
                        }

                        if (!input.parseState.TryGetInt(currTokenIdx++, out countValue))
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                            break;
                        }

                        opts.WithCount = true;
                    }
                    else if (input.header.SortedSetOp == SortedSetOperation.GEOSEARCH && tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHCOORD"u8)) opts.WithCoord = true;
                    else if ((input.header.SortedSetOp == SortedSetOperation.GEOSEARCH && tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHDIST"u8)) ||
                             (input.header.SortedSetOp == SortedSetOperation.GEOSEARCHSTORE && tokenBytes.EqualsUpperCaseSpanIgnoringCase("STOREDIST"u8))) opts.WithDist = true;
                    else if (input.header.SortedSetOp == SortedSetOperation.GEOSEARCH && tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHHASH"u8)) opts.WithHash = true;
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ANY"u8)) opts.WithCountAny = true;
                    else
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        break;
                    }
                }

                // Check that we have the mandatory options
                if (errorMessage == default && !((opts.FromMember || opts.FromLonLat) && (opts.ByRadius || opts.ByBox)))
                    argNumError = true;

                // Check if we have a wrong number of arguments
                if (argNumError)
                {
                    errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.GEOSEARCH)));
                }

                // Check if we encountered an error while checking the parse state
                if (errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // Not supported options in Garnet: WITHHASH
                if (opts.WithHash)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
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

                        if (responseData.Count == 0)
                        {
                            while (!RespWriteUtils.WriteInteger(0, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            var innerArrayLength = 1;
                            if (opts.WithDist)
                            {
                                innerArrayLength++;
                            }
                            if (opts.WithHash)
                            {
                                innerArrayLength++;
                            }
                            if (opts.WithCoord)
                            {
                                innerArrayLength++;
                            }

                            // Write results 
                            while (!RespWriteUtils.WriteArrayLength(responseData.Count, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            foreach (var item in responseData)
                            {
                                if (innerArrayLength > 1)
                                {
                                    while (!RespWriteUtils.WriteArrayLength(innerArrayLength, ref curr, end))
                                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                }

                                while (!RespWriteUtils.WriteBulkString(item.Member, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                                if (opts.WithDist)
                                {
                                    var distanceValue = (byBoxUnits.Length == 1 && (byBoxUnits[0] == (int)'M' || byBoxUnits[0] == (int)'m')) ? item.Distance
                                                        : server.GeoHash.ConvertMetersToUnits(item.Distance, byBoxUnits);

                                    while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                }

                                if (opts.WithCoord)
                                {
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
                    }
                }

                // Not supported options in Garnet: FROMLONLAT BYBOX BYRADIUS 
                if (opts.FromLonLat)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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
    }
}