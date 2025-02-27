// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        /// Type of GeoSearch
        /// </summary>
        internal enum GeoSearchType
        {
            /// <summary>
            /// No defined order.
            /// </summary>
            Undefined,

            /// <summary>
            /// Search inside circular area
            /// </summary>
            ByRadius,

            /// <summary>
            /// Search inside an axis-aligned rectangle
            /// </summary>
            ByBox,
        }

        /// <summary>
        /// The direction in which to sequence elements.
        /// </summary>
        internal enum GeoOrder
        {
            /// <summary>
            /// No defined order.
            /// </summary>
            None,

            /// <summary>
            /// Order from low values to high values.
            /// </summary>
            Ascending,

            /// <summary>
            /// Order from high values to low values.
            /// </summary>
            Descending,
        }

        internal enum GeoOriginType
        {
            /// <summary>
            /// Not defined.
            /// </summary>
            Undefined,

            /// <summary>
            /// From explicit lon lat coordinates.
            /// </summary>
            FromLonLat,

            /// <summary>
            /// From member key
            /// </summary>
            FromMember
        }

        /// <summary>
        /// Small struct to store options for GEOSEARCH command
        /// </summary>
        private ref struct GeoSearchOptions
        {
            internal GeoSearchType searchType;
            internal ReadOnlySpan<byte> unit;
            internal double radius;
            internal double boxWidth;
            internal double boxHeight
            {
                get
                {
                    return radius;
                }
                set
                {
                    radius = value;
                }
            }

            internal int countValue;

            internal GeoOriginType origin;

            internal byte[] fromMember;
            internal double lon, lat;

            internal bool withCoord;
            internal bool withHash;
            //internal bool withCountAny;
            internal bool withDist;
            internal GeoOrder sort;
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
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
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

                while (!RespWriteUtils.TryWriteInt32(ch ? elementsChanged : elementsAdded, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
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
                while (!RespWriteUtils.TryWriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    // Read member
                    var member = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (sortedSetDict.TryGetValue(member, out var value52Int))
                    {
                        var geoHash = server.GeoHash.GetGeoHashCode((long)value52Int);
                        while (!RespWriteUtils.TryWriteAsciiBulkString(geoHash, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
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
                    while (!RespWriteUtils.TryWriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
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
                while (!RespWriteUtils.TryWriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    // read member
                    var member = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (sortedSetDict.TryGetValue(member, out var scoreMember1))
                    {
                        var (lat, lon) = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);

                        // write array of 2 values
                        while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(lon, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(lat, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteNullArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GeoSearch(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var cmtOpt = (SortedSetGeoOpts)input.arg2;
            var byRadiusCmd = (cmtOpt & SortedSetGeoOpts.ByRadius) != 0;
            var readOnlyCmd = (cmtOpt & SortedSetGeoOpts.ReadOnly) != 0;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var opts = new GeoSearchOptions()
                {
                    unit = "M"u8
                };

                ReadOnlySpan<byte> errorMessage = default;
                var argNumError = false;
                var currTokenIdx = 0;

                if (byRadiusCmd)
                {
                    // Read coordinates, note we already checked the number of arguments earlier.
                    if ((cmtOpt & SortedSetGeoOpts.ByMember) != 0)
                    {
                        // From Member
                        opts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                        opts.origin = GeoOriginType.FromMember;
                    }
                    else
                    {
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out var lon) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out var lat) ||
                            (Math.Abs(lon) > 180) || (Math.Abs(lat) > 90))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;

                            while (!RespWriteUtils.TryWriteError(errorMessage, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }

                        opts.origin = GeoOriginType.FromLonLat;
                        opts.lon = lon;
                        opts.lat = lat;
                    }

                    // Radius
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out var Radius) || (Radius < 0))
                    {
                        errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;

                        while (!RespWriteUtils.TryWriteError(errorMessage, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    opts.searchType = GeoSearchType.ByRadius;
                    opts.radius = Radius;
                    opts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                }

                // Read the options
                while (currTokenIdx < input.parseState.Count)
                {
                    // Read token
                    var tokenBytes = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                    if (!byRadiusCmd)
                    {
                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMMEMBER"u8))
                        {
                            if (opts.origin != GeoOriginType.Undefined)
                            {
                                errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                                break;
                            }

                            if (input.parseState.Count - currTokenIdx == 0)
                            {
                                argNumError = true;
                                break;
                            }

                            opts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                            opts.origin = GeoOriginType.FromMember;
                            continue;
                        }

                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                        {
                            if (opts.origin != GeoOriginType.Undefined)
                            {
                                errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                                break;
                            }

                            if (input.parseState.Count - currTokenIdx < 2)
                            {
                                argNumError = true;
                                break;
                            }

                            // Read coordinates
                            if (!input.parseState.TryGetDouble(currTokenIdx++, out var lon) ||
                                !input.parseState.TryGetDouble(currTokenIdx++, out var lat) ||
                                (Math.Abs(lon) > 180) || (Math.Abs(lat) > 90))
                            {
                                errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                                break;
                            }

                            opts.origin = GeoOriginType.FromLonLat;
                            opts.lon = lon;
                            opts.lat = lat;
                            continue;
                        }

                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                        {
                            if (opts.searchType != GeoSearchType.Undefined)
                            {
                                errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                                break;
                            }

                            if (input.parseState.Count - currTokenIdx < 2)
                            {
                                argNumError = true;
                                break;
                            }

                            // Read radius and units
                            if (!input.parseState.TryGetDouble(currTokenIdx++, out opts.radius))
                            {
                                errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                                break;
                            }

                            if (opts.radius < 0)
                            {
                                errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                                break;
                            }

                            opts.searchType = GeoSearchType.ByRadius;
                            opts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                            continue;
                        }

                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                        {
                            if (opts.searchType != GeoSearchType.Undefined)
                            {
                                errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                                break;
                            }

                            if (input.parseState.Count - currTokenIdx < 3)
                            {
                                argNumError = true;
                                break;
                            }

                            // Read width, height
                            if (!input.parseState.TryGetDouble(currTokenIdx++, out opts.boxWidth) ||
                                !input.parseState.TryGetDouble(currTokenIdx++, out var height))
                            {
                                errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                                break;
                            }
                            opts.boxHeight = height;

                            if (opts.boxWidth < 0 || opts.boxHeight < 0)
                            {
                                errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                                break;
                            }

                            // Read units
                            opts.searchType = GeoSearchType.ByBox;
                            opts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                            continue;
                        }
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ASC"u8))
                    {
                        opts.sort = GeoOrder.Ascending;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("DESC"u8))
                    {
                        opts.sort = GeoOrder.Descending;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                    {
                        if (input.parseState.Count - currTokenIdx == 0)
                        {
                            argNumError = true;
                            break;
                        }

                        if (!input.parseState.TryGetInt(currTokenIdx++, out var countValue))
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                            break;
                        }

                        if (countValue <= 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                            break;
                        }

                        opts.countValue = countValue;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("WITHCOORD"u8))
                    {
                        opts.withCoord = true;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ANY"u8))
                    {
                        //geoSearchOpt.withCountAny = true;
                        continue;
                    }

                    // When GEORADIUS STORE support is added we'll need to account for it.
                    // For now we'll act as if all radius commands are readonly.
                    if (readOnlyCmd || byRadiusCmd)
                    {
                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHDIST))
                        {
                            opts.withDist = true;
                            continue;
                        }

                        if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHHASH))
                        {
                            opts.withHash = true;
                            continue;
                        }
                    }
                    // GEOSEARCHSTORE
                    else if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        opts.withDist = true;
                        continue;
                    }

                    errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                    break;
                }

                // Check that we have the mandatory options
                if (errorMessage.IsEmpty && ((opts.origin == 0) || (opts.searchType == 0)))
                    argNumError = true;

                // Check if we have a wrong number of arguments
                if (argNumError)
                {
                    errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                        nameof(RespCommand.GEOSEARCH)));
                }

                // Check if we encountered an error while checking the parse state
                if (!errorMessage.IsEmpty)
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                #region act
                // Not supported options in Garnet: WITHHASH
                if (opts.withHash)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // FROMLONLAT
                bool hasLonLat = opts.origin == GeoOriginType.FromLonLat;
                // FROMMEMBER
                if (opts.origin == GeoOriginType.FromMember && sortedSetDict.TryGetValue(opts.fromMember, out var centerPointScore))
                {
                    (opts.lat, opts.lon) = server.GeoHash.GetCoordinatesFromLong((long)centerPointScore);
                    hasLonLat = true;
                }

                // Get the results
                if (hasLonLat)
                {
                    var responseData = new List<GeoSearchData>();
                    foreach (var point in sortedSet)
                    {
                        var coorInItem = server.GeoHash.GetCoordinatesFromLong((long)point.Score);
                        double distance = 0;

                        if (opts.searchType == GeoSearchType.ByBox)
                        {
                            if (!server.GeoHash.GetDistanceWhenInRectangle(
                                 server.GeoHash.ConvertValueToMeters(opts.boxWidth, opts.unit),
                                 server.GeoHash.ConvertValueToMeters(opts.boxHeight, opts.unit),
                                 opts.lat, opts.lon, coorInItem.Latitude, coorInItem.Longitude, ref distance))
                            {
                                continue;
                            }
                        }
                        else /* byRadius == true */
                        {
                            if (!server.GeoHash.GetDistanceWhenInCircle(
                                 server.GeoHash.ConvertValueToMeters(opts.radius, opts.unit),
                                 opts.lat, opts.lon, coorInItem.Latitude, coorInItem.Longitude, ref distance))
                            {
                                continue;
                            }
                        }

                        // The item is inside the shape
                        responseData.Add(new GeoSearchData()
                        {
                            Member = point.Element,
                            Distance = distance,
                            GeoHashCode = server.GeoHash.GetGeoHashCode((long)point.Score),
                            Coordinates = server.GeoHash.GetCoordinatesFromLong((long)point.Score)
                        });

                        if (responseData.Count == opts.countValue)
                            break;
                    }

                    if (responseData.Count == 0)
                    {
                        while (!RespWriteUtils.TryWriteInt32(0, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        var innerArrayLength = 1;
                        if (opts.withDist)
                        {
                            innerArrayLength++;
                        }
                        if (opts.withHash)
                        {
                            innerArrayLength++;
                        }
                        if (opts.withCoord)
                        {
                            innerArrayLength++;
                        }

                        // Write results 
                        while (!RespWriteUtils.TryWriteArrayLength(responseData.Count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        var q = responseData.AsQueryable();
                        if (opts.sort == GeoOrder.Ascending)
                            q = q.OrderBy(i => i.Distance);
                        else if (opts.sort == GeoOrder.Descending)
                            q = q.OrderByDescending(i => i.Distance);

                        foreach (var item in q)
                        {
                            if (innerArrayLength > 1)
                            {
                                while (!RespWriteUtils.TryWriteArrayLength(innerArrayLength, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }

                            while (!RespWriteUtils.TryWriteBulkString(item.Member, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            if (opts.withDist)
                            {
                                var distanceValue = opts.searchType switch
                                {
                                    GeoSearchType.ByBox => server.GeoHash.ConvertMetersToUnits(item.Distance, opts.unit),

                                    // byRadius
                                    _ => server.GeoHash.ConvertMetersToUnits(item.Distance, opts.unit),
                                };

                                while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }

                            if (opts.withCoord)
                            {
                                // Write array of 2 values
                                while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                                while (!RespWriteUtils.TryWriteDoubleBulkString(item.Coordinates.Longitude, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                                while (!RespWriteUtils.TryWriteDoubleBulkString(item.Coordinates.Latitude, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                    }
                }
                #endregion
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}