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
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool GeoAdd<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            // validate the number of parameters
            if (count < 5)
            {
                return AbortWithWrongNumberOfArguments("GEOADD", count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                var inputCount = count - 2;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.SortedSetOp = SortedSetOperation.GEOADD;
                inputPtr->count = inputCount;
                inputPtr->done = zaddDoneCount;

                var status = storageApi.GeoAdd(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

                //restore input buffer
                *inputPtr = save;

                zaddDoneCount += output.countDone;
                zaddAddCount += output.opsDone;

                // return if command is only partially done
                if (zaddDoneCount < (inputCount / 3))
                    return false;

                //update pointers
                ptr += output.bytesDone;
                while (!RespWriteUtils.WriteInteger(zaddAddCount, ref dcurr, dend))
                    SendAndReset();
            }

            //reset sesion counters
            zaddDoneCount = zaddAddCount = 0;

            //update read pointers
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        private unsafe bool GeoCommands<TGarnetApi>(int count, byte* ptr, SortedSetOperation op, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            int paramsRequiredInCommand = 0;
            string cmd = string.Empty;
            var responseWhenNotFound = CmdStrings.RESP_EMPTYLIST;
            switch (op)
            {
                case SortedSetOperation.GEODIST:
                    ptr += 13;
                    paramsRequiredInCommand = 4;
                    cmd = "GEODIST";
                    responseWhenNotFound = CmdStrings.RESP_ERRNOTFOUND;
                    break;
                case SortedSetOperation.GEOHASH:
                    paramsRequiredInCommand = 2;
                    ptr += 13;
                    cmd = "GEOHASH";
                    break;
                case SortedSetOperation.GEOPOS:
                    paramsRequiredInCommand = 2;
                    ptr += 12;
                    cmd = "GEOPOS";
                    break;
                case SortedSetOperation.GEOSEARCH:
                    paramsRequiredInCommand = 4;
                    ptr += 15;
                    cmd = "GEOSEARCH";
                    break;
            }

            if (count < paramsRequiredInCommand)
            {
                zaddDoneCount = zaddAddCount = 0;
                return AbortWithWrongNumberOfArguments(cmd, count);
            }
            else
            {
                // Get the key for the Sorted Set
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                var inputCount = count - 2;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.SortedSetOp = op;
                inputPtr->count = inputCount;

                //take into account the ones already processed
                inputPtr->done = zaddDoneCount;

                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.GeoCommands(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        zaddDoneCount += objOutputHeader.countDone;
                        zaddAddCount += objOutputHeader.opsDone;
                        //command partially done
                        if (zaddDoneCount < inputCount)
                            return false;
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteResponse(responseWhenNotFound, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Reset session counters
            zaddAddCount = zaddDoneCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}