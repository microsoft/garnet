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
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            if (!input.parseState.TryGetGeoSearchOptions(command, out var searchOpts, out var destIdx, out var errorMessage))
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
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