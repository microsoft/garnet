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
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi, bool @readonly = true)
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
                case RespCommand.GEOSEARCH:
                    paramsRequiredInCommand = 6;
                    break;
                case RespCommand.GEORADIUS:
                case RespCommand.GEORADIUS_RO:
                    paramsRequiredInCommand = 5;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    paramsRequiredInCommand = 4;
                    break;
            }

            if (parseState.Count < paramsRequiredInCommand)
            {
                return AbortWithWrongNumberOfArguments(cmd);
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            SortedSetOperation op;
            SortedSetGeoOpts opts = 0;
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
                case RespCommand.GEORADIUS:
                    op = SortedSetOperation.GEOSEARCH;
                    opts = SortedSetGeoOpts.ByRadius | SortedSetGeoOpts.Store;
                    break;
                case RespCommand.GEORADIUS_RO:
                    op = SortedSetOperation.GEOSEARCH;
                    opts = SortedSetGeoOpts.ByRadius;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                    op = SortedSetOperation.GEOSEARCH;
                    opts = SortedSetGeoOpts.ByRadius | SortedSetGeoOpts.Store | SortedSetGeoOpts.ByMember;
                    break;
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    op = SortedSetOperation.GEOSEARCH;
                    opts = SortedSetGeoOpts.ByRadius | SortedSetGeoOpts.ByMember;
                    break;
                case RespCommand.GEOSEARCH:
                    op = SortedSetOperation.GEOSEARCH;
                    break;
                default:
                    throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}");
            }

            if (@readonly)
                opts &= ~SortedSetGeoOpts.Store;

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };

            var input = new ObjectInput(header, ref parseState, startIdx: 1, arg2: (int)opts);

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
                        case SortedSetOperation.GEOSEARCH:
                            while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
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
        /// GEOSEARCHSTORE: Store the the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="cmd"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoSearchStore<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
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

            SortedSetGeoOpts opt;
            int destIdx, sourceIdx;

            switch (cmd)
            {
                case RespCommand.GEORADIUS:
                    // GERADIUS src lon lat 0 km
                    if (parseState.Count < 5)
                    {
                        return AbortWithWrongNumberOfArguments(nameof(RespCommand.GEORADIUS));
                    }
                    opt = SortedSetGeoOpts.Store | SortedSetGeoOpts.ByRadius;
                    sourceIdx = 0;
                    destIdx = GetDestIdx();
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                    // GERADIUSBYMEMBER src member 0 km
                    if (parseState.Count < 4)
                    {
                        return AbortWithWrongNumberOfArguments(nameof(RespCommand.GEORADIUSBYMEMBER));
                    }
                    opt = SortedSetGeoOpts.Store | SortedSetGeoOpts.ByRadius | SortedSetGeoOpts.ByMember;
                    sourceIdx = 0;
                    destIdx = GetDestIdx();
                    break;
                case RespCommand.GEOSEARCHSTORE:
                default:
                    // GEOSEARCHSTORE dst src FROMEMBER key BYRADIUS 0 m
                    if (parseState.Count < 7)
                    {
                        return AbortWithWrongNumberOfArguments(nameof(RespCommand.GEOSEARCHSTORE));
                    }
                    opt = SortedSetGeoOpts.Store;
                    destIdx = 0;
                    sourceIdx = 1;
                    break;
            }

            if (destIdx == -1)
                return GeoCommands(cmd, ref storageApi, true);

            var destinationKey = parseState.GetArgSliceByRef(destIdx);
            var sourceKey = parseState.GetArgSliceByRef(sourceIdx);

            var input = new ObjectInput(new RespInputHeader
            {
                type = GarnetObjectType.SortedSet,
                SortedSetOp = SortedSetOperation.GEOSEARCH,
            }, ref parseState, startIdx: sourceIdx + 1, arg2: (int)opt);

            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.GeoSearchStore(sourceKey, destinationKey, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;
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