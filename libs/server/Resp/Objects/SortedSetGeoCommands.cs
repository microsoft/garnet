// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

            GeoAddOptions addOption = 0;

            var currTokenIdx = 0;
            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(currTokenIdx++);

            while (currTokenIdx < parseState.Count)
            {
                var addOptionSpan = parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan;

                if (addOptionSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CH))
                {
                    addOption |= GeoAddOptions.CH;
                }
                else if (addOptionSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NX))
                {
                    addOption |= GeoAddOptions.NX;
                }
                else if (addOptionSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.XX))
                {
                    addOption |= GeoAddOptions.XX;
                }
                else
                {
                    break;
                }

                ++currTokenIdx;
            }

            if (((addOption & GeoAddOptions.NX) != 0) && ((addOption & GeoAddOptions.XX) != 0))
            {
                //return AbortWithErrorMessage(CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE);
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var memberStart = currTokenIdx;

            // We need at least one member
            do
            {
                if (currTokenIdx > parseState.Count - 3)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                if (!parseState.TryGetGeoLonLat(currTokenIdx, out _, out _, out var error))
                {
                    return AbortWithErrorMessage(error);
                }

                // move past lonlat and skip member
                currTokenIdx += 3;
            }
            while (currTokenIdx < parseState.Count);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.GEOADD };
            var input = new ObjectInput(header, ref parseState, startIdx: memberStart, arg1: (int)addOption);

            var output = GetObjectOutput();

            var status = storageApi.GeoAdd(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
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
            var key = parseState.GetArgSliceByRef(0);

            SortedSetOperation op;

            switch (command)
            {
                case RespCommand.GEOHASH:
                    op = SortedSetOperation.GEOHASH;
                    break;
                case RespCommand.GEOPOS:
                    op = SortedSetOperation.GEOPOS;
                    break;
                case RespCommand.GEODIST:
                    op = SortedSetOperation.GEODIST;
                    if (parseState.Count > 3 && !parseState.TryGetGeoDistanceUnit(3, out _))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT);
                    }
                    break;
                default:
                    throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}");
            }

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };

            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var output = GetObjectOutput();

            var status = storageApi.GeoCommands(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    switch (op)
                    {
                        case SortedSetOperation.GEODIST:
                            WriteNull();
                            break;
                        default:
                            var inputCount = parseState.Count - 1;
                            while (!RespWriteUtils.TryWriteArrayLength(inputCount, ref dcurr, dend))
                                SendAndReset();
                            for (var i = 0; i < inputCount; i++)
                            {
                                WriteNullArray();
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

            int sourceIdx = 0;
            switch (command)
            {
                case RespCommand.GEORADIUS:
                    paramsRequiredInCommand = 5;
                    break;
                case RespCommand.GEORADIUS_RO:
                    paramsRequiredInCommand = 5;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                    paramsRequiredInCommand = 4;
                    break;
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    paramsRequiredInCommand = 4;
                    break;
                case RespCommand.GEOSEARCH:
                    paramsRequiredInCommand = 6;
                    break;
                case RespCommand.GEOSEARCHSTORE:
                    paramsRequiredInCommand = 7;
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
            }, ref parseState, startIdx: sourceIdx + 1, arg1: (int)command);
            var output = SpanByteAndMemory.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            if (!input.parseState.TryGetGeoSearchOptions(command, out var searchOpts, out var destIdx, out var errorMessage))
            {
                return AbortWithErrorMessage(errorMessage);
            }

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
                status = storageApi.GeoSearchReadOnly(sourceKey, ref searchOpts, ref input, ref output);

                if (status == GarnetStatus.OK)
                {
                    ProcessOutput(output);
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