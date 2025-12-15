// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    public partial class SortedSetObject : GarnetObjectBase
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

        private void GeoAdd(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            // By default, add new elements and update the ones already in the set
            var options = (GeoAddOptions)input.arg1;

            var count = input.parseState.Count;
            var currTokenIdx = 0;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

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

            writer.WriteInt32((options & GeoAddOptions.CH) == 0 ? elementsAdded : elementsChanged);
        }

        private void GeoHash(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                // Read member
                var member = input.parseState.GetArgSliceByRef(i).ToArray();

                if (sortedSetDict.TryGetValue(member, out var value52Int))
                {
                    var geoHash = server.GeoHash.GetGeoHashCode((long)value52Int);
                    writer.WriteAsciiBulkString(geoHash);
                }
                else
                {
                    writer.WriteNull();
                }
            }
        }

        private void GeoDistance(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // Read 1st member
            var member1 = input.parseState.GetArgSliceByRef(0).ToArray();

            // Read 2nd member
            var member2 = input.parseState.GetArgSliceByRef(1).ToArray();

            // Read units
            var units = GeoDistanceUnitType.M;

            if (input.parseState.Count > 2)
            {
                var validUnit = input.parseState.TryGetGeoDistanceUnit(2, out units);
                Debug.Assert(validUnit);
            }

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (sortedSetDict.TryGetValue(member1, out var scoreMember1) && sortedSetDict.TryGetValue(member2, out var scoreMember2))
            {
                var first = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);
                var second = server.GeoHash.GetCoordinatesFromLong((long)scoreMember2);

                var distance = server.GeoHash.Distance(first.Latitude, first.Longitude, second.Latitude, second.Longitude);

                var distanceValue = server.GeoHash.ConvertMetersToUnits(distance, units);

                writer.WriteDoubleBulkString(distanceValue);
            }
            else
            {
                writer.WriteNull();
            }
        }

        private void GeoPosition(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                // read member
                var member = input.parseState.GetArgSliceByRef(i).ToArray();

                if (sortedSetDict.TryGetValue(member, out var scoreMember1))
                {
                    var (lat, lon) = server.GeoHash.GetCoordinatesFromLong((long)scoreMember1);

                    // write array of 2 values
                    writer.WriteArrayLength(2);
                    writer.WriteDoubleNumeric(lon);
                    writer.WriteDoubleNumeric(lat);
                }
                else
                {
                    writer.WriteNullArray();
                }
            }
        }

        internal void GeoSearch(ref ObjectInput input,
                                ref SpanByteAndMemory spam,
                                byte respProtocolVersion,
                                ref GeoSearchOptions opts,
                                bool readOnly)
        {
            Debug.Assert(opts.searchType != default);

            using var writer = new RespMemoryWriter(respProtocolVersion, ref spam);

            // FROMMEMBER
            if (opts.origin == GeoOriginType.FromMember)
            {
                if (!sortedSetDict.TryGetValue(opts.fromMember, out var centerPointScore))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_ZSET_MEMBER);
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
                writer.WriteEmptyArray();
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
                    writer.WriteArrayLength(opts.countValue);
                }
                else
                {
                    writer.WriteArrayLength(responseData.Count);
                }

                foreach (var item in q)
                {
                    if (innerArrayLength > 1)
                    {
                        writer.WriteArrayLength(innerArrayLength);
                    }

                    writer.WriteBulkString(item.Member);

                    if (opts.withDist)
                    {
                        var distanceValue = opts.searchType switch
                        {
                            GeoSearchType.ByBox => server.GeoHash.ConvertMetersToUnits(item.Distance, opts.unit),

                            // byRadius
                            _ => server.GeoHash.ConvertMetersToUnits(item.Distance, opts.unit),
                        };

                        writer.WriteDoubleBulkString(distanceValue);
                    }

                    if (opts.withHash)
                    {
                        if (readOnly)
                        {
                            writer.WriteInt64(item.GeoHash);
                        }
                        else
                        {
                            writer.WriteArrayItem(item.GeoHash);
                        }
                    }

                    if (opts.withCoord)
                    {
                        // Write array of 2 values
                        writer.WriteArrayLength(2);
                        writer.WriteDoubleNumeric(item.Coordinates.Longitude);
                        writer.WriteDoubleNumeric(item.Coordinates.Latitude);
                    }
                }
            }
        }
    }
}