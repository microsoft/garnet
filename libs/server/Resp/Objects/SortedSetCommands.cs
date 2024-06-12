// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sorted set methods with network layer access
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Session counter of number of ZADD entries partially done
        /// </summary>
        int zaddDoneCount;

        /// <summary>
        /// Session counter of number of ZADD adds partially done
        /// </summary>
        int zaddAddCount;

        static ReadOnlySpan<byte> withscores => "WITHSCORES"u8;

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetAdd<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 3)
            {
                return AbortWithWrongNumberOfArguments("ZADD", count);
            }

            if (count % 2 != 1)
            {
                zaddDoneCount = zaddAddCount = 0;

                return AbortWithErrorMessage(count, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            // Get the key for SortedSet
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            int inputCount = (count - 1) / 2;

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = SortedSetOperation.ZADD;
            inputPtr->count = inputCount;
            inputPtr->done = zaddDoneCount;

            var status = storageApi.SortedSetAdd(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;

                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    zaddDoneCount += output.countDone;
                    zaddAddCount += output.opsDone;

                    // Reset buffer and return if command is only partially done
                    if (zaddDoneCount < inputCount)
                        return false;
                    while (!RespWriteUtils.WriteInteger(zaddAddCount, ref dcurr, dend))
                        SendAndReset();

                    // Move head, write result to output, reset session counters
                    ptr += output.bytesDone;
                    break;
            }

            readHead = (int)(ptr - recvBufferPtr);
            zaddDoneCount = zaddAddCount = 0;

            return true;
        }

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                zaddDoneCount = zaddAddCount = 0;
                return AbortWithWrongNumberOfArguments("ZREM", count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    return true;
                }

                int inputCount = count - 1;

                // Prepare input
                var rmwInput = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *rmwInput;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)rmwInput);

                // Prepare header in input buffer
                rmwInput->header.type = GarnetObjectType.SortedSet;
                rmwInput->header.flags = 0;
                rmwInput->header.SortedSetOp = SortedSetOperation.ZREM;
                rmwInput->count = inputCount;
                rmwInput->done = zaddDoneCount;

                var status = storageApi.SortedSetRemove(key, new ArgSlice((byte*)rmwInput, inputLength), out ObjectOutputHeader rmwOutput);

                // Reset input buffer
                *rmwInput = save;

                if (status != GarnetStatus.OK)
                {
                    // This checks if we get the whole request,
                    // Otherwise it needs to return false
                    if (ReadLeftToken(count - 1, ref ptr) < count - 1)
                        return false;
                }

                switch (status)
                {
                    case GarnetStatus.OK:
                        zaddDoneCount += rmwOutput.countDone;
                        zaddAddCount += rmwOutput.opsDone;

                        // Reset buffer and return if ZREM is only partially done
                        if (zaddDoneCount < inputCount)
                            return false;

                        ptr += rmwOutput.bytesDone;
                        rmwOutput = default;
                        while (!RespWriteUtils.WriteInteger(zaddAddCount, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Reset session counters
            zaddAddCount = zaddDoneCount = 0;

            //update readHead
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 1)
            {
                zaddDoneCount = zaddAddCount = 0;
                return AbortWithWrongNumberOfArguments("ZCARD", count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZCARD;
                inputPtr->count = 1;
                inputPtr->done = 0;

                var status = storageApi.SortedSetLength(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                // Reset input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process output
                        while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
        /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRange<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            if (count < 3)
            {
                zaddDoneCount = zaddAddCount = 0;
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZRANGE), count);
            }

            // Get the key for the Sorted Set
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key,
                ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, true))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            SortedSetOperation op =
                command switch
                {
                    RespCommand.ZRANGE => SortedSetOperation.ZRANGE,
                    RespCommand.ZREVRANGE => SortedSetOperation.ZREVRANGE,
                    RespCommand.ZRANGEBYSCORE => SortedSetOperation.ZRANGEBYSCORE,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = op;
            inputPtr->count = count - 1;
            inputPtr->done = 0;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRange(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            if (status != GarnetStatus.OK)
            {
                var tokens = ReadLeftToken(count - 1, ref ptr);
                if (tokens < count - 1)
                    return false;
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    // Return if ZRANGE is only partially done
                    if (objOutputHeader.bytesDone == 0)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // reset session counters
            zaddDoneCount = zaddAddCount = 0;

            //update readHead
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScore<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("ZSCORE", count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                // Read score key
                byte* scoreKeyPtr = null;
                int scoreKeySize = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref scoreKeyPtr, ref scoreKeySize, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(scoreKeyPtr - sizeof(ObjectInputHeader));

                //save values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZSCORE;
                inputPtr->count = scoreKeySize;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.SortedSetScore(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScores<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (count < 2)
            {
                // send error to output
                return AbortWithWrongNumberOfArguments("ZMSCORE", count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                //save values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);
                int inputCount = count - 1;

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZMSCORE;
                inputPtr->count = inputCount;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.SortedSetScores(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                switch (status)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteArrayWithNullElements(inputCount, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetPop<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    return true;
                }

                var popCount = 1;

                if (count == 2)
                {
                    // Read count
                    if (!RespReadUtils.ReadIntWithLengthHeader(out popCount, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                //save values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                SortedSetOperation op =
                    command switch
                    {
                        RespCommand.ZPOPMIN => SortedSetOperation.ZPOPMIN,
                        RespCommand.ZPOPMAX => SortedSetOperation.ZPOPMAX,
                        _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                    };

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = op;
                inputPtr->count = popCount;
                inputPtr->done = zaddDoneCount;

                // Prepare output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

                var status = storageApi.SortedSetPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        zaddDoneCount += objOutputHeader.countDone;
                        zaddAddCount += objOutputHeader.opsDone;
                        if (zaddDoneCount < zaddAddCount)
                            return false;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // reset session counters
            zaddDoneCount = zaddAddCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetCount<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZCOUNT", count);
            }
            else
            {
                // Get the key for the Sorted Set
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZCOUNT;
                inputPtr->count = 0;
                inputPtr->done = 0;

                var status = storageApi.SortedSetCount(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

                //restore input buffer
                *inputPtr = save;

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                    {
                        //command partially executed
                        return false;
                    }
                }

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process response
                        if (output.countDone == int.MaxValue)
                        {
                            // Error in arguments
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref dcurr, dend))
                                SendAndReset();
                        }
                        else if (output.countDone == int.MinValue)  // command partially executed
                            return false;
                        else
                            while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                                SendAndReset();
                        ptr += output.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            //reset session counters
            zaddAddCount = zaddDoneCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// ZLEXCOUNT: Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score,
        /// this command forces lexicographical ordering.
        /// ZREMRANGEBYLEX: Removes all elements in the sorted set between the
        /// lexicographical range specified by min and max.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLengthByValue<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                zaddDoneCount = zaddAddCount = 0;
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }
            else
            {
                // Get the key
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, command != RespCommand.ZREMRANGEBYLEX))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                SortedSetOperation op =
                    command switch
                    {
                        RespCommand.ZREMRANGEBYLEX => SortedSetOperation.ZREMRANGEBYLEX,
                        RespCommand.ZLEXCOUNT => SortedSetOperation.ZLEXCOUNT,
                        _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                    };

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = op;
                inputPtr->count = 0;
                inputPtr->done = 0;

                var status = op == SortedSetOperation.ZREMRANGEBYLEX ?
                             storageApi.SortedSetRemoveRangeByLex(key, new ArgSlice((byte*)inputPtr, inputLength), out var output) :
                             storageApi.SortedSetLengthByValue(key, new ArgSlice((byte*)inputPtr, inputLength), out output);

                //restore input buffer
                *inputPtr = save;

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process response
                        if (output.countDone == Int32.MaxValue)
                        {
                            // Error in arguments
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref dcurr, dend))
                                SendAndReset();
                        }
                        else if (output.countDone == Int32.MinValue)  // command partially executed
                            return false;
                        else
                            while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                                SendAndReset();
                        ptr += output.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            //reset session counters
            zaddAddCount = zaddDoneCount = 0;
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIncrement<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            //validation of required args
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZINCRBY", count);
            }
            else
            {
                // Get the key for the Sorted Set
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZINCRBY;
                inputPtr->count = count - 1;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.SortedSetIncrement(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                ReadOnlySpan<byte> errorMessage = default;
                switch (status)
                {
                    case GarnetStatus.NOTFOUND:
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        //check for partial execution
                        if (objOutputHeader.countDone == int.MinValue)
                            return false;
                        if (objOutputHeader.countDone == int.MaxValue)
                            errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.WRONGTYPE:
                        errorMessage = CmdStrings.RESP_ERR_WRONG_TYPE;
                        break;
                }

                if (errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRank<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            // TODO: WITHSCORE
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }
            else
            {
                // Get the key for SortedSet
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                SortedSetOperation op =
                    command switch
                    {
                        RespCommand.ZRANK => SortedSetOperation.ZRANK,
                        RespCommand.ZREVRANK => SortedSetOperation.ZREVRANK,
                        _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                    };

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = op;
                inputPtr->count = count;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.SortedSetRank(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                // Reset input buffer
                *inputPtr = save;
                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        if (objOutputHeader.bytesDone == 0)
                            return false;
                        break;

                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemoveRange<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }
            else
            {
                // Get the key
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                SortedSetOperation op =
                    command switch
                    {
                        RespCommand.ZREMRANGEBYRANK => SortedSetOperation.ZREMRANGEBYRANK,
                        RespCommand.ZREMRANGEBYSCORE => SortedSetOperation.ZREMRANGEBYSCORE,
                        _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                    };

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = op;
                inputPtr->count = 0;
                inputPtr->done = 0;

                var status = storageApi.SortedSetRemoveRange(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

                //restore input buffer
                *inputPtr = save;

                if (status != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (output.countDone == int.MaxValue)
                        {
                            var errorMessage = command == RespCommand.ZREMRANGEBYRANK ?
                                CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER :
                                CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT;

                            // Error in arguments
                            while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                                SendAndReset();
                        }
                        else if (output.countDone == int.MinValue)  // command partially executed
                            return false;
                        else
                            while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                                SendAndReset();
                        ptr += output.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns a random element from the sorted set key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRandomMember<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 3)
            {
                return AbortWithWrongNumberOfArguments("ZRANDMEMBER", count);
            }
            else
            {
                // Get the key for the Sorted Set
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    return true;
                }

                var paramCount = 0;
                bool includeWithScores = false;
                bool includedCount = false;

                if (count >= 2)
                {
                    // Read count
                    if (!RespReadUtils.ReadIntWithLengthHeader(out paramCount, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    includedCount = true;

                    // Read withscores
                    if (count == 3)
                    {
                        if (!RespReadUtils.TrySliceWithLengthHeader(out var withScoreBytes, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        includeWithScores = withScoreBytes.SequenceEqual("WITHSCORES"u8);
                    }
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.SortedSet;
                inputPtr->header.flags = 0;
                inputPtr->header.SortedSetOp = SortedSetOperation.ZRANDMEMBER;
                inputPtr->count = count == 1 ? 1 : paramCount;
                inputPtr->done = includeWithScores ? 1 : 0;

                GarnetStatus status = GarnetStatus.NOTFOUND;
                GarnetObjectStoreOutput outputFooter = default;

                // This prevents going to the backend if ZRANDMEMBER is called with a count of 0
                if (inputPtr->count != 0)
                {
                    // Prepare GarnetObjectStore output
                    outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                    status = storageApi.SortedSetRandomMember(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
                }

                //restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;

                        break;
                    case GarnetStatus.NOTFOUND:
                        var respBytes = includedCount ? CmdStrings.RESP_EMPTYLIST : CmdStrings.RESP_ERRNOTFOUND;
                        while (!RespWriteUtils.WriteDirect(respBytes, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        ///  Computes a difference operation  between the first and all successive sorted sets
        ///  and returns the result to the client.
        ///  The total number of input keys is specified.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private unsafe bool SortedSetDifference<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZDIFF", count);
            }
            else
            {
                //number of keys
                if (!RespReadUtils.ReadIntWithLengthHeader(out var nKeys, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                ArgSlice key = default;

                // Read first key
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref key.ptr, ref key.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                bool withscoresInclude = false;

                // Read all the keys
                if (count <= 2)
                {
                    //return empty array
                    while (!RespWriteUtils.WriteArrayLength(0, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    ArgSlice[] keys = new ArgSlice[nKeys];
                    keys[0] = key;

                    var i = nKeys - 1;
                    do
                    {
                        keys[i] = default;
                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        --i;
                    } while (i > 0);

                    if (count - 1 > nKeys)
                    {
                        ArgSlice withscore = default;
                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref withscore.ptr, ref withscore.length, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        if (withscore.ReadOnlySpan.SequenceEqual(withscores))
                            withscoresInclude = true;
                    }

                    if (NetworkKeyArraySlotVerify(ref keys, true))
                    {
                        return true;
                    }

                    var status = storageApi.SortedSetDifference(keys, out var result);

                    switch (status)
                    {
                        case GarnetStatus.WRONGTYPE:
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                                SendAndReset();
                            break;
                        default:
                            // write the size of the array reply
                            int resultCount = result == null ? 0 : result.Count;
                            while (!RespWriteUtils.WriteArrayLength(withscoresInclude ? resultCount * 2 : resultCount, ref dcurr, dend))
                                SendAndReset();

                            if (result != null)
                            {
                                foreach (var (element, score) in result)
                                {
                                    while (!RespWriteUtils.WriteBulkString(element, ref dcurr, dend))
                                        SendAndReset();

                                    if (withscoresInclude)
                                    {
                                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref dcurr, dend))
                                            SendAndReset();
                                    }
                                }
                            }
                            break;
                    }
                }
            }
            // update read pointers
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}