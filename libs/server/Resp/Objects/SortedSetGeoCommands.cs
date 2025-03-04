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
                case RespCommand.GEOSEARCH:
                    paramsRequiredInCommand = 6;
                    break;
                case RespCommand.GEORADIUS:
                case RespCommand.GEORADIUS_RO:
                    paramsRequiredInCommand = 4;
                    break;
                case RespCommand.GEORADIUSBYMEMBER:
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    paramsRequiredInCommand = 3;
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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoSearchStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // GEOSEARCHSTORE dst src FROMEMBER key BYRADIUS 0 m
            if (parseState.Count < 7)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.GEOSEARCHSTORE));
            }

            var destinationKey = parseState.GetArgSliceByRef(0);
            var sourceKey = parseState.GetArgSliceByRef(1);

            var input = new ObjectInput(new RespInputHeader
            {
                type = GarnetObjectType.SortedSet,
                SortedSetOp = SortedSetOperation.GEOSEARCH,
            }, ref parseState, startIdx: 2, arg2: (int)SortedSetGeoOpts.Store);

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