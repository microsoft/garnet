// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
            public long GeoHash;
            public string GeoHashCode;
            public (double Latitude, double Longitude) Coordinates;
        }

        private void GeoAdd(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            DeleteExpiredItems();

            // By default, add new elements and update the ones already in the set
            var options = (GeoAddOptions)input.arg1;

            var count = input.parseState.Count;
            var currTokenIdx = 0;

            ObjectOutputHeader _output = default;
            try
            {
                // Read the members
                var elementsAdded = 0;
                var elementsChanged = 0;

                while (currTokenIdx < count)
                {
                    var res = input.parseState.TryGetGeoLonLat(currTokenIdx, out var longitude, out var latitude, out _);
                    Debug.Assert(res);
                    currTokenIdx += 2;
                    var member = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                    var score = server.GeoHash.GeoToLongValue(latitude, longitude);
                    if (score != -1)
                    {
                        var memberByteArray = member.ToArray();
                        if (!sortedSetDict.TryGetValue(memberByteArray, out var scoreStored))
                        {
                            if ((options & GeoAddOptions.XX) == 0)
                            {
                                sortedSetDict.Add(memberByteArray, score);
                                sortedSet.Add((score, memberByteArray));
                                elementsAdded++;

                                UpdateSize(member);
                                elementsChanged++;
                            }
                        }
                        else if (((options & GeoAddOptions.NX) == 0) && scoreStored != score)
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

                while (!RespWriteUtils.TryWriteInt32((options & GeoAddOptions.CH) == 0 ?
                                                      elementsAdded : elementsChanged, ref curr, end))
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
                var units = GeoDistanceUnitType.M;

                if (input.parseState.Count > 2)
                {
                    var validUnit = input.parseState.TryGetGeoDistanceUnit(2, out units);
                    Debug.Assert(validUnit);
                }

                if (sortedSetDict.TryGetValue(member1, out var scoreMember1) && sortedSetDict.TryGetValue(member2, out var scoreMember2))
                {
                    var first = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);
                    var second = server.GeoHash.GetCoordinatesFromLong((long)scoreMember2);

                    var distance = server.GeoHash.Distance(first.Latitude, first.Longitude, second.Latitude, second.Longitude);

                    var distanceValue = server.GeoHash.ConvertMetersToUnits(distance, units);

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

        internal void GeoSearch(ref ObjectInput input,
                                ref SpanByteAndMemory output,
                                ref GeoSearchOptions opts,
                                bool readOnly)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;

            Debug.Assert(opts.searchType != default);

            try
            {
                // FROMMEMBER
                if (opts.origin == GeoOriginType.FromMember)
                {
                    if (!sortedSetDict.TryGetValue(opts.fromMember, out var centerPointScore))
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_ZSET_MEMBER, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    (opts.lat, opts.lon) = server.GeoHash.GetCoordinatesFromLong((long)centerPointScore);
                }

                // Get the results
                var responseData = new List<GeoSearchData>(
                    opts.withCountAny && opts.countValue > 0 && opts.countValue < sortedSet.Count ?
                    opts.countValue :
                    sortedSet.Count);
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
                        if (!server.GeoHash.IsPointWithinRadius(
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
                        GeoHash = (long)point.Score,
                        GeoHashCode = server.GeoHash.GetGeoHashCode((long)point.Score),
                        Coordinates = server.GeoHash.GetCoordinatesFromLong((long)point.Score)
                    });

                    if (opts.withCountAny && (responseData.Count == opts.countValue))
                        break;
                }

                if (responseData.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
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

                    var q = responseData.AsQueryable();
                    switch (opts.sort)
                    {
                        case GeoOrder.Descending:
                            q = q.OrderByDescending(i => i.Distance);
                            break;
                        case GeoOrder.Ascending:
                            q = q.OrderBy(i => i.Distance);
                            break;
                        case GeoOrder.None:
                            if (!opts.withCountAny && opts.countValue > 0)
                                q = q.OrderBy(i => i.Distance);
                            break;
                    }

                    // Write results 
                    if (opts.countValue > 0 && opts.countValue < responseData.Count)
                    {
                        q = q.Take(opts.countValue);
                        while (!RespWriteUtils.TryWriteArrayLength(opts.countValue, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteArrayLength(responseData.Count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

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

                        if (opts.withHash)
                        {
                            if (readOnly)
                            {
                                while (!RespWriteUtils.TryWriteInt64(item.GeoHash, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                            else
                            {
                                while (!RespWriteUtils.TryWriteArrayItem(item.GeoHash, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
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
            finally
            {
                ObjectOutputHeader _output = default;
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}