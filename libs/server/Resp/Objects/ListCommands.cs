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
        /// Session counter of number of List entries(PUSH,POP etc.) partially done
        /// </summary>
        int listItemsDoneCount;

        /// <summary>
        /// Session counter of number of List operations partially done
        /// </summary>
        int listOpsCount;

        /// <summary>
        /// LPUSH key element[element...]
        /// RPUSH key element [element ...]
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool ListPush<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var sskey, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(sskey, false))
            {
                if (!DrainCommands(count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            var inputCount = count - 1;
            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            ListOperation lop =
                command switch
                {
                    RespCommand.LPUSH => ListOperation.LPUSH,
                    RespCommand.LPUSHX => ListOperation.LPUSHX,
                    RespCommand.RPUSH => ListOperation.RPUSH,
                    RespCommand.RPUSHX => ListOperation.RPUSHX,
                    _ => throw new Exception($"Unexpected {nameof(ListOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = lop;
            inputPtr->count = inputCount;
            inputPtr->done = listItemsDoneCount;

            var input = new ArgSlice((byte*)inputPtr, inputLength);

            ObjectOutputHeader output;
            output = default;

            var status = GarnetStatus.OK;

            if (command == RespCommand.LPUSH || command == RespCommand.LPUSHX)
                status = storageApi.ListLeftPush(sskey, input, out output);
            else
                status = storageApi.ListRightPush(sskey, input, out output);

            //restore input buffer
            *inputPtr = save;

            listItemsDoneCount += output.countDone;
            listOpsCount += output.opsDone;

            // Return if command is only partially done
            if (output.countDone == Int32.MinValue && listOpsCount < inputCount)
                return false;

            // FIXME: Need to use ptr += output.bytesDone; instead of ReadLeftToken

            // Skip the element tokens on the input buffer
            var tokens = ReadLeftToken(count - 1, ref ptr);
            if (tokens < count - 1)
                return false;

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Write result to output
                while (!RespWriteUtils.WriteInteger(listItemsDoneCount, ref dcurr, dend))
                    SendAndReset();
            }

            // Reset session counters
            listItemsDoneCount = listOpsCount = 0;

            // Move head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// LPOP key [count]
        /// RPOP key [count]
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool ListPop<TGarnetApi>(RespCommand command, int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                if (!DrainCommands(count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));
            int popCount = 1;

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            if (count == 2)
            {
                // Read count
                if (!RespReadUtils.ReadIntWithLengthHeader(out popCount, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            ListOperation lop =
                command switch
                {
                    RespCommand.LPOP => ListOperation.LPOP,
                    RespCommand.RPOP => ListOperation.RPOP,
                    _ => throw new Exception($"Unexpected {nameof(ListOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = lop;
            inputPtr->done = 0;
            inputPtr->count = popCount;

            GarnetStatus statusOp = GarnetStatus.NOTFOUND;

            if (command == RespCommand.LPOP)
                statusOp = storageApi.ListLeftPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
            else
                statusOp = storageApi.ListRightPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (statusOp)
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

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// LLEN key
        /// Gets the length of the list stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments("LLEN", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // save old values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - ptr) + sizeof(ObjectInputHeader);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LLEN;
                inputPtr->count = count;
                inputPtr->done = 0;

                var status = storageApi.ListLength(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                //restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                    default:
                        // Process output
                        while (!RespWriteUtils.WriteInteger(output.countDone, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        /// <summary>
        /// LTRIM key start stop
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListTrim<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("LTRIM", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Read the parameters(start and stop) from LTRIM
                if (!RespReadUtils.ReadIntWithLengthHeader(out var start, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Read the parameters(start and stop) from LTRIM
                if (!RespReadUtils.ReadIntWithLengthHeader(out var stop, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LTRIM;
                inputPtr->count = start;
                inputPtr->done = stop;

                var status = storageApi.ListTrim(key, new ArgSlice((byte*)inputPtr, inputLength));

                //restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                    default:
                        //GarnetStatus.OK or NOTFOUND have same result
                        // no need to process output, just send OK
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// LRANGE key start stop
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListRange<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("LRANGE", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Read count start and stop params for LRANGE
                if (!RespReadUtils.ReadIntWithLengthHeader(out int start, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int end, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LRANGE;
                inputPtr->count = start;
                inputPtr->done = end;

                var statusOp = storageApi.ListRange(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                // Reset input buffer
                *inputPtr = save;

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the element at index.
        /// LINDEX key index
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListIndex<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("LINDEX", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Read index param
                if (!RespReadUtils.ReadIntWithLengthHeader(out int index, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LINDEX;
                inputPtr->count = index;
                inputPtr->done = 0;

                var statusOp = storageApi.ListIndex(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                ReadOnlySpan<byte> error = default;

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        if (objOutputHeader.opsDone == -1)
                            error = CmdStrings.RESP_ERRNOTFOUND;
                        break;
                    case GarnetStatus.NOTFOUND:
                        error = CmdStrings.RESP_ERRNOTFOUND;
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }

                if (error != default)
                {
                    while (!RespWriteUtils.WriteDirect(error, ref dcurr, dend))
                        SendAndReset();
                }
            }

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// LINSERT key BEFORE|AFTER pivot element
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListInsert<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 4)
            {
                return AbortWithWrongNumberOfArguments("LINSERT", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LINSERT;
                inputPtr->done = 0;
                inputPtr->count = 0;

                var statusOp = storageApi.ListInsert(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                //restore input buffer
                *inputPtr = save;

                if (statusOp != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //check for partial execution
                        if (output.countDone == int.MinValue)
                            return false;
                        //process output
                        ptr += output.bytesDone;
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

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// LREM key count element
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListRemove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            // if params are missing return error
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("LREM", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Get count parameter
                if (!RespReadUtils.ReadIntWithLengthHeader(out int nCount, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LREM;
                inputPtr->count = nCount;
                inputPtr->done = 0;

                var statusOp = storageApi.ListRemove(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);
                //restore input buffer
                *inputPtr = save;

                if (statusOp != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 2, ref ptr);
                    if (tokens < count - 2)
                        return false;
                }

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //check for partial execution
                        if (output.countDone == int.MinValue)
                            return false;
                        //process output
                        ptr += output.bytesDone;
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
            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }


        /// <summary>
        /// LMOVE source destination [LEFT | RIGHT] [LEFT | RIGHT]
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListMove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (count != 4)
            {
                return AbortWithWrongNumberOfArguments("LMOVE", count);
            }

            ArgSlice sourceKey = default, destinationKey = default, param1 = default, param2 = default;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref param1.ptr, ref param1.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref param2.ptr, ref param2.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var sourceDirection = GetOperationDirection(param1);
            var destinationDirection = GetOperationDirection(param2);

            if (sourceDirection == OperationDirection.Unknown || destinationDirection == OperationDirection.Unknown)
            {
                return AbortWithErrorMessage(count, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            if (!ListMove(count, sourceKey, destinationKey, sourceDirection, destinationDirection, out var node,
                    ref storageApi, out var garnetStatus))
                return false;

            switch (garnetStatus)
            {
                case GarnetStatus.OK:
                    if (node != null)
                    {
                        while (!RespWriteUtils.WriteBulkString(node, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                            SendAndReset();
                    }

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// RPOPLPUSH source destination
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool ListRightPopLeftPush<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("RPOPLPUSH", count);
            }

            ArgSlice sourceKey = default, destinationKey = default;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!ListMove(count, sourceKey, destinationKey, OperationDirection.Right, OperationDirection.Left,
                    out var node, ref storageApi, out var garnetStatus))
                return false;

            switch (garnetStatus)
            {
                case GarnetStatus.OK:
                    if (node != null)
                    {
                        while (!RespWriteUtils.WriteBulkString(node, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                            SendAndReset();
                    }

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // update read pointers
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
        /// RPOPLPUSH source destination
        /// </summary>
        /// <param name="count">Number of tokens in input</param>
        /// <param name="sourceKey"></param>
        /// <param name="destinationKey"></param>
        /// <param name="sourceDirection"></param>
        /// <param name="destinationDirection"></param>
        /// <param name="node"></param>
        /// <param name="storageApi"></param>
        /// <param name="garnetStatus"></param>
        /// <returns></returns>
        private bool ListMove<TGarnetApi>(int count, ArgSlice sourceKey, ArgSlice destinationKey,
            OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] node,
            ref TGarnetApi storageApi, out GarnetStatus garnetStatus)
            where TGarnetApi : IGarnetApi
        {
            garnetStatus = GarnetStatus.OK;
            var keys = new[] { sourceKey, destinationKey };
            node = null;

            if (NetworkKeyArraySlotVerify(ref keys, false))
            {
                // check for non crosslot error
                if (!DrainCommands(count))
                {
                    return false;
                }
                return true;
            }

            garnetStatus =
                storageApi.ListMove(sourceKey, destinationKey, sourceDirection, destinationDirection, out node);
            return true;
        }

        /// <summary>
        /// Sets the list element at index to element
        /// LSET key index element
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        public bool ListSet<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("LSET", count);
            }
            else
            {
                // Get the key for List
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, true))
                {
                    if (!DrainCommands(count))
                        return false;
                    return true;
                }

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values for possible revert
                var save = *inputPtr;

                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.List;
                inputPtr->header.flags = 0;
                inputPtr->header.ListOp = ListOperation.LSET;
                inputPtr->count = 0;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var statusOp = storageApi.ListSet(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                if (statusOp != GarnetStatus.OK)
                {
                    var tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                }

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}