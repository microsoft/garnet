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
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetAdd<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 3)
            {
                return AbortWithWrongNumberOfArguments("ZADD", count);
            }

            if (count % 2 != 1)
            {
                return AbortWithErrorMessage(count, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
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
            inputPtr->arg1 = inputCount;

            var status = storageApi.SortedSetAdd(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemove<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZREM", count);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
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
            rmwInput->arg1 = inputCount;

            var status = storageApi.SortedSetRemove(keyBytes, new ArgSlice((byte*)rmwInput, inputLength), out ObjectOutputHeader rmwOutput);

            // Reset input buffer
            *rmwInput = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(rmwOutput.result1, ref dcurr, dend))
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

            return true;
        }

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLength<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments("ZCARD", count);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
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
            inputPtr->arg1 = 1;

            var status = storageApi.SortedSetLength(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
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

            return true;
        }

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
        /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRange<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            if (count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZRANGE), count);
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
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
            inputPtr->arg1 = count - 1;
            inputPtr->arg2 = respProtocolVersion;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRange(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScore<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("ZSCORE", count);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Read score key
            var sbScoreKey = parseState.GetArgSliceByRef(1).SpanByte;
            var scoreKeyPtr = sbScoreKey.ToPointer();
            var scoreKeySize = sbScoreKey.Length;

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(scoreKeyPtr - sizeof(ObjectInputHeader));

            // Save values
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = SortedSetOperation.ZSCORE;
            inputPtr->arg1 = scoreKeySize;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScore(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScores<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZMSCORE", count);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
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
            inputPtr->arg1 = inputCount;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScores(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            //restore input
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetPop<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }
            else
            {
                // Get the key for SortedSet
                var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
                var keyBytes = sbKey.ToByteArray();

                var ptr = sbKey.ToPointer() + sbKey.Length + 2;

                if (NetworkSingleKeySlotVerify(keyBytes, false))
                {
                    return true;
                }

                var popCount = 1;

                if (count == 2)
                {
                    // Read count
                    var popCountSlice = parseState.GetArgSliceByRef(1);

                    if (!NumUtils.TryParse(popCountSlice.ReadOnlySpan, out popCount))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }

                    var sbPopCount = popCountSlice.SpanByte;
                    ptr = sbPopCount.ToPointer() + sbPopCount.Length + 2;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                var op =
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
                inputPtr->arg1 = popCount;

                // Prepare output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

                var status = storageApi.SortedSetPop(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                // Restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetCount<TGarnetApi>(int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZCOUNT", count);
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = SortedSetOperation.ZCOUNT;
            inputPtr->arg1 = 0;

            var status = storageApi.SortedSetCount(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process response
                    if (output.result1 == int.MaxValue)
                    {
                        // Error in arguments
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                        while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
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
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLengthByValue<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, command != RespCommand.ZREMRANGEBYLEX))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            var op =
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
            inputPtr->arg1 = 0;

            var status = op == SortedSetOperation.ZREMRANGEBYLEX ?
                storageApi.SortedSetRemoveRangeByLex(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output) :
                storageApi.SortedSetLengthByValue(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out output);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process response
                    if (output.result1 == int.MaxValue)
                    {
                        // Error in arguments
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (output.result1 == int.MinValue)  // command partially executed
                        return false;
                    else
                        while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
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

            return true;
        }

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIncrement<TGarnetApi>(int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            //validation of required args
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZINCRBY", count);
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input
            var save = *inputPtr;

            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = SortedSetOperation.ZINCRBY;
            inputPtr->arg1 = count - 1;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetIncrement(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input
            *inputPtr = save;

            ReadOnlySpan<byte> errorMessage = default;
            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                case GarnetStatus.OK:
                    //process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    //check for partial execution
                    if (objOutputHeader.result1 == int.MinValue)
                        return false;
                    if (objOutputHeader.result1 == int.MaxValue)
                        errorMessage = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
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

            return true;
        }

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRank<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var includeWithScore = false;

            // Read WITHSCORE
            if (count == 3)
            {
                var withScoreSlice = parseState.GetArgSliceByRef(2);

                if (!withScoreSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORE))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                includeWithScore = true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            var op =
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
            inputPtr->arg1 = count;
            inputPtr->arg2 = includeWithScore ? 1 : 0;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRank(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;
            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemoveRange<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            var op =
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
            inputPtr->arg1 = 0;

            var status = storageApi.SortedSetRemoveRange(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    if (output.result1 == int.MaxValue)
                    {
                        var errorMessage = command == RespCommand.ZREMRANGEBYRANK ?
                            CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER :
                            CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT;

                        // Error in arguments
                        while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (output.result1 == int.MinValue)  // command partially executed
                        return false;
                    else
                        while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
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
            return true;
        }

        /// <summary>
        /// Returns a random element from the sorted set key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRandomMember<TGarnetApi>(int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 3)
            {
                return AbortWithWrongNumberOfArguments("ZRANDMEMBER", count);
            }

            // Get the key for the Sorted Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var paramCount = 1;
            var includeWithScores = false;
            var includedCount = false;

            if (count >= 2)
            {
                // Read count
                var countSlice = parseState.GetArgSliceByRef(1);

                if (!NumUtils.TryParse(countSlice.ReadOnlySpan, out paramCount))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                var sbCount = countSlice.SpanByte;
                ptr = sbCount.ToPointer() + sbCount.Length + 2;

                includedCount = true;

                // Read withscores
                if (count == 3)
                {
                    var withScoresSlice = parseState.GetArgSliceByRef(2);

                    if (!withScoresSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORES))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }

                    var sbWithScores = withScoresSlice.SpanByte;
                    ptr = sbWithScores.ToPointer() + sbWithScores.Length + 2;

                    includeWithScores = true;
                }
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save input buffer
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Create a random seed
            var seed = RandomGen.Next();

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = SortedSetOperation.ZRANDMEMBER;
            inputPtr->arg1 = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (includeWithScores ? 1 : 0);
            inputPtr->arg2 = seed;

            var status = GarnetStatus.NOTFOUND;
            GarnetObjectStoreOutput outputFooter = default;

            // This prevents going to the backend if ZRANDMEMBER is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare GarnetObjectStore output
                outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                status = storageApi.SortedSetRandomMember(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
            }

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
            return true;
        }

        /// <summary>
        ///  Computes a difference operation  between the first and all successive sorted sets
        ///  and returns the result to the client.
        ///  The total number of input keys is specified.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private unsafe bool SortedSetDifference<TGarnetApi>(int count, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZDIFF", count);
            }

            //number of keys
            var sbNumKeys = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            if (!NumUtils.TryParse(sbNumKeys, out int nKeys))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (count - 1 != nKeys && count - 1 != nKeys + 1)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var includeWithScores = false;

            // Read all the keys
            if (count <= 2)
            {
                //return empty array
                while (!RespWriteUtils.WriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var keys = new ArgSlice[nKeys];

            for (var i = 1; i < nKeys + 1; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            if (count - 1 > nKeys)
            {
                var withScores = parseState.GetArgSliceByRef(count - 1).ReadOnlySpan;

                if (!withScores.SequenceEqual(CmdStrings.WITHSCORES))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                includeWithScores = true;
            }

            if (NetworkMultiKeySlotVerify(readOnly: true, firstKey: 1, lastKey: 1 + nKeys))
                return true;

            var status = storageApi.SortedSetDifference(keys, out var result);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    // write the size of the array reply
                    var resultCount = result?.Count ?? 0;
                    while (!RespWriteUtils.WriteArrayLength(includeWithScores ? resultCount * 2 : resultCount, ref dcurr, dend))
                        SendAndReset();

                    if (result != null)
                    {
                        foreach (var (element, score) in result)
                        {
                            while (!RespWriteUtils.WriteBulkString(element, ref dcurr, dend))
                                SendAndReset();

                            if (includeWithScores)
                            {
                                while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                    break;
            }

            return true;
        }
    }
}