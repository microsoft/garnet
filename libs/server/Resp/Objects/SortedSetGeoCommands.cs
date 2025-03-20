// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
        /// Data is stored into the key as a sorted set.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoAdd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // validate the number of parameters
            if (parseState.Count < 4)
            {
                return AbortWithWrongNumberOfArguments("GEOADD");
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.GEOADD };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.GeoAdd(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var paramsRequiredInCommand = 0;
            var cmd = nameof(command);

            switch (command)
            {
                case RespCommand.GEODIST:
                    paramsRequiredInCommand = 3;
                    break;
                case RespCommand.GEOHASH:
                    paramsRequiredInCommand = 1;
                    break;
                case RespCommand.GEOPOS:
                    paramsRequiredInCommand = 1;
                    break;
            }

            if (parseState.Count < paramsRequiredInCommand)
            {
                return AbortWithWrongNumberOfArguments(cmd);
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var op =
                command switch
                {
                    RespCommand.GEOHASH => SortedSetOperation.GEOHASH,
                    RespCommand.GEODIST => SortedSetOperation.GEODIST,
                    RespCommand.GEOPOS => SortedSetOperation.GEOPOS,
                    RespCommand.GEOSEARCH => SortedSetOperation.GEOSEARCH,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };

            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.GeoCommands(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    switch (op)
                    {
                        case SortedSetOperation.GEODIST:
                            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                SendAndReset();
                            break;
                        default:
                            var inputCount = parseState.Count - 1;
                            while (!RespWriteUtils.TryWriteArrayLength(inputCount, ref dcurr, dend))
                                SendAndReset();
                            for (var i = 0; i < inputCount; i++)
                            {
                                while (!RespWriteUtils.TryWriteNullArray(ref dcurr, dend))
                                    SendAndReset();
                            }
                            break;
                    }

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// GEOSEARCHSTORE: Store the the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// GEORADIUS: Return or store the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUS_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUSBYMEMBER: Return or store the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// GEORADIUSBYMEMBER_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoSearchCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var paramsRequiredInCommand = 0;
            var searchOpts = new GeoSearchOptions();

            int sourceIdx = 0, destIdx = -1;
            switch (command)
            {
                case RespCommand.GEORADIUS:
                    paramsRequiredInCommand = 5;
                    break;
                case RespCommand.GEORADIUS_RO:
                    paramsRequiredInCommand = 5;
                    searchOpts.readOnly = true;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                    paramsRequiredInCommand = 4;
                    break;
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    paramsRequiredInCommand = 4;
                    searchOpts.readOnly = true;
                    break;
                case RespCommand.GEOSEARCH:
                    paramsRequiredInCommand = 6;
                    searchOpts.readOnly = true;
                    break;
                case RespCommand.GEOSEARCHSTORE:
                    paramsRequiredInCommand = 7;
                    destIdx = 0;
                    sourceIdx = 1;
                    break;
                default:
                    throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}");
            }

            if (parseState.Count < paramsRequiredInCommand)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key for the Sorted Set
            var sourceKey = parseState.GetArgSliceByRef(sourceIdx);

            // Prepare input and call the storage layer
            var input = new ObjectInput(new RespInputHeader(GarnetObjectType.SortedSet)
            {
                SortedSetOp = SortedSetOperation.GEOSEARCH
            }, ref parseState, startIdx: sourceIdx + 1);
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            #region parse
            ReadOnlySpan<byte> errorMessage = default;
            var argNumError = false;
            var storeDist = false;
            var currTokenIdx = 0;

            if (command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUS_RO ||
                command == RespCommand.GEORADIUSBYMEMBER || command == RespCommand.GEORADIUSBYMEMBER_RO)
            {
                // Read coordinates, note we already checked the number of arguments earlier.
                if (command == RespCommand.GEORADIUSBYMEMBER || command == RespCommand.GEORADIUSBYMEMBER_RO)
                {
                    // From Member
                    searchOpts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                    searchOpts.origin = GeoOriginType.FromMember;
                }
                else
                {
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lon) ||
                        !input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lat))
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    if ((searchOpts.lon < GeoHash.LongitudeMin) ||
                        (searchOpts.lat < GeoHash.LatitudeMin) ||
                        (searchOpts.lon > GeoHash.LongitudeMax) ||
                        (searchOpts.lat > GeoHash.LatitudeMax))
                    {
                        errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrLonLat,
                                                               searchOpts.lon, searchOpts.lat));
                        while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    searchOpts.origin = GeoOriginType.FromLonLat;
                }

                // Radius
                if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_RADIUS, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (searchOpts.radius < 0)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                searchOpts.searchType = GeoSearchType.ByRadius;
                if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            // Read the options
            while (currTokenIdx < input.parseState.Count)
            {
                // Read token
                var tokenBytes = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (command == RespCommand.GEOSEARCH || command == RespCommand.GEOSEARCHSTORE)
                {
                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FROMMEMBER))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                            break;
                        }

                        if (input.parseState.Count == currTokenIdx)
                        {
                            argNumError = true;
                            break;
                        }

                        searchOpts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                        searchOpts.origin = GeoOriginType.FromMember;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
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
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lon) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lat)
                           )
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        if ((searchOpts.lon < GeoHash.LongitudeMin) ||
                            (searchOpts.lat < GeoHash.LatitudeMin) ||
                            (searchOpts.lon > GeoHash.LongitudeMax) ||
                            (searchOpts.lat > GeoHash.LatitudeMax))
                        {
                            errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrLonLat,
                                                                   searchOpts.lon, searchOpts.lat));
                            while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }

                        searchOpts.origin = GeoOriginType.FromLonLat;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
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
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_RADIUS;
                            break;
                        }

                        if (searchOpts.radius < 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                            break;
                        }

                        searchOpts.searchType = GeoSearchType.ByRadius;
                        if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
                        {
                            errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                            break;
                        }
                        searchOpts.searchType = GeoSearchType.ByBox;

                        if (input.parseState.Count - currTokenIdx < 3)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read width, height
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.boxWidth))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_WIDTH;
                            break;
                        }

                        if (!input.parseState.TryGetDouble(currTokenIdx++, out var height))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_HEIGHT;
                            break;
                        }

                        searchOpts.boxHeight = height;

                        if (searchOpts.boxWidth < 0 || searchOpts.boxHeight < 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE;
                            break;
                        }

                        // Read units
                        if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                        continue;
                    }
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ASC"u8))
                {
                    searchOpts.sort = GeoOrder.Ascending;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("DESC"u8))
                {
                    searchOpts.sort = GeoOrder.Descending;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    if (input.parseState.Count == currTokenIdx)
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
                        errorMessage = CmdStrings.RESP_ERR_COUNT_IS_NOT_POSITIVE;
                        break;
                    }

                    searchOpts.countValue = countValue;

                    if (input.parseState.Count > currTokenIdx)
                    {
                        var peekArg = input.parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan;
                        if (peekArg.EqualsUpperCaseSpanIgnoringCase("ANY"u8))
                        {
                            searchOpts.withCountAny = true;
                            currTokenIdx++;
                            continue;
                        }
                    }

                    continue;
                }

                if (!searchOpts.readOnly)
                {
                    if ((command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUSBYMEMBER)
                        && tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE))
                    {
                        if (input.parseState.Count == currTokenIdx)
                        {
                            argNumError = true;
                            break;
                        }

                        destIdx = ++currTokenIdx;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        if ((command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUSBYMEMBER))
                        {
                            if (input.parseState.Count == currTokenIdx)
                            {
                                argNumError = true;
                                break;
                            }

                            destIdx = ++currTokenIdx;
                        }

                        storeDist = true;
                        continue;
                    }
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHCOORD))
                {
                    searchOpts.withCoord = true;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHDIST))
                {
                    searchOpts.withDist = true;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHHASH))
                {
                    searchOpts.withHash = true;
                    continue;
                }

                errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                break;
            }

            // Check that we have the mandatory options
            if (errorMessage.IsEmpty && ((searchOpts.origin == 0) || (searchOpts.searchType == 0)))
                argNumError = true;

            // Check if we have a wrong number of arguments
            if (argNumError)
            {
                errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                                                       command.ToString()));
            }

            if (destIdx != -1)
            {
                if ((searchOpts.withDist || searchOpts.withCoord || searchOpts.withHash))
                {
                    errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrStoreCommand,
                                                           command.ToString()));
                }
                searchOpts.withDist = storeDist;
            }
            else
            {
                searchOpts.readOnly = true;
            }

            // Check if we encountered an error while checking the parse state
            if (!errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // On storing to ZSET, we need to use either dist or hash as score.
            if (!searchOpts.readOnly && !searchOpts.withDist && !searchOpts.withHash)
            {
                searchOpts.withHash = true;
            }
            #endregion

            GarnetStatus status;
            if (destIdx != -1)
            {
                var destinationKey = parseState.GetArgSliceByRef(destIdx);
                status = storageApi.GeoSearchStore(sourceKey, destinationKey,
                                                   ref searchOpts, ref input, ref output);

                if (status == GarnetStatus.OK)
                {
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;

                    return true;
                }
            }
            else
            {
                status = storageApi.GeoSearch(sourceKey, ref searchOpts, ref input, ref output);

                if (status == GarnetStatus.OK)
                {
                    ProcessOutputWithHeader(output);
                    return true;
                }
            }

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;

                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }
    }
}