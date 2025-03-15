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
                return AbortWithWrongNumberOfArguments(nameof(command));
            }

            SortedSetOperation op;

            switch (command)
            {
                case RespCommand.GEOHASH:
                    op = SortedSetOperation.GEOHASH;
                    break;
                case RespCommand.GEODIST:
                    op = SortedSetOperation.GEODIST;
                    break;
                case RespCommand.GEOPOS:
                    op = SortedSetOperation.GEOPOS;
                    break;
                default:
                    throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}");
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            // Prepare input and call the storage layer
            var input = new ObjectInput(new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op },
                                        ref parseState, startIdx: 1);
            var outputFooter = new GarnetObjectStoreOutput
            {
                SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr))
            };
            var status = storageApi.GeoCommands(sbKey, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    return true;
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
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoSearchCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            int GetDestIdx()
            {
                var idx = 1;
                while (true)
                {
                    if (idx >= parseState.Count - 1)
                        break;

                    var argSpan = parseState.GetArgSliceByRef(idx++).ReadOnlySpan;

                    if (argSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE) ||
                        argSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        return idx;
                    }
                }

                return -1;
            }

            var paramsRequiredInCommand = 0;
            var byRadius = false;
            var byMember = false;
            var searchOpts = new GeoSearchOptions()
            {
                unit = "M"u8
            };

            int sourceIdx = 0, destIdx = -1;
            switch (command)
            {
                case RespCommand.GEORADIUS:
                    paramsRequiredInCommand = 5;
                    byRadius = true;
                    destIdx = GetDestIdx();
                    searchOpts.readOnly = destIdx == -1;
                    break;
                case RespCommand.GEORADIUS_RO:
                    paramsRequiredInCommand = 5;
                    byRadius = true;
                    searchOpts.readOnly = true;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                    paramsRequiredInCommand = 4;
                    byMember = true;
                    byRadius = true;
                    destIdx = GetDestIdx();
                    searchOpts.readOnly = destIdx == -1;
                    break;
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    paramsRequiredInCommand = 4;
                    byMember = true;
                    byRadius = true;
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
                return AbortWithWrongNumberOfArguments(nameof(command));
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
            var currTokenIdx = 0;

            if (byRadius)
            {
                // Read coordinates, note we already checked the number of arguments earlier.
                if (byMember)
                {
                    // From Member
                    searchOpts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                    searchOpts.origin = GeoOriginType.FromMember;
                }
                else
                {
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out var lon) ||
                        !input.parseState.TryGetDouble(currTokenIdx++, out var lat) ||
                        (Math.Abs(lon) > 180) || (Math.Abs(lat) > 90))
                    {
                        errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;

                        while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    searchOpts.origin = GeoOriginType.FromLonLat;
                    searchOpts.lon = lon;
                    searchOpts.lat = lat;
                }

                // Radius
                if (!input.parseState.TryGetDouble(currTokenIdx++, out var Radius) || (Radius < 0))
                {
                    errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;

                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                searchOpts.searchType = GeoSearchType.ByRadius;
                searchOpts.radius = Radius;
                searchOpts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
            }

            // Read the options
            while (currTokenIdx < input.parseState.Count)
            {
                // Read token
                var tokenBytes = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (!byRadius)
                {
                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FROMMEMBER))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                            break;
                        }

                        if (input.parseState.Count - currTokenIdx == 0)
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
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out var lon) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out var lat) ||
                            (Math.Abs(lon) > 180) || (Math.Abs(lat) > 90))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        searchOpts.origin = GeoOriginType.FromLonLat;
                        searchOpts.lon = lon;
                        searchOpts.lat = lat;
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
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }

                        if (searchOpts.radius < 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                            break;
                        }

                        searchOpts.searchType = GeoSearchType.ByRadius;
                        searchOpts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
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
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.boxWidth) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out var height))
                        {
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                            break;
                        }
                        searchOpts.boxHeight = height;

                        if (searchOpts.boxWidth < 0 || searchOpts.boxHeight < 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                            break;
                        }

                        // Read units
                        searchOpts.searchType = GeoSearchType.ByBox;
                        searchOpts.unit = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
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

                    searchOpts.countValue = countValue;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ANY"u8))
                {
                    searchOpts.withCountAny = true;
                    continue;
                }

                if (!searchOpts.readOnly)
                {
                    if (byRadius && tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE))
                    {
                        if (byRadius)
                            currTokenIdx++;
                        continue;
                    }
                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        if (byRadius)
                            currTokenIdx++;
                        searchOpts.withDist = true;
                        continue;
                    }
                }
                else
                {
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
                    nameof(RespCommand.GEOSEARCH)));
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
                status = storageApi.GeoSearchStore(sourceKey, destinationKey, searchOpts, ref input, ref output);

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
                status = storageApi.GeoSearch(sourceKey, searchOpts, ref input, ref output);

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