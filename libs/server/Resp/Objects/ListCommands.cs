// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
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
        /// <param name="lop"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool ListPush<TGarnetApi>(int count, byte* ptr, ListOperation lop, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            ptr += (lop == ListOperation.LPUSH || lop == ListOperation.RPUSH) ? 11 : 12;

            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var sskey, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(sskey, false))
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
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.ListOp = lop;
            inputPtr->count = inputCount;
            inputPtr->done = listItemsDoneCount;

            var input = new ArgSlice((byte*)inputPtr, inputLength);

            ObjectOutputHeader output;
            output = default;

            var status = GarnetStatus.OK;

            if (lop == ListOperation.LPUSH || lop == ListOperation.LPUSHX)
                status = storageApi.ListLeftPush(sskey, input, out output);
            else
                status = storageApi.ListRightPush(sskey, input, out output);

            //restore input buffer
            *inputPtr = save;

            listItemsDoneCount += output.countDone;
            listOpsCount += output.opsDone;

            //return if command is only partially done
            if (output.countDone == Int32.MinValue && listOpsCount < inputCount)
                return false;

            ptr += output.bytesDone;

            //if lpushx or rpushx and not found forward left tokens
            if ((lop == ListOperation.LPUSHX || lop == ListOperation.RPUSHX) && status == GarnetStatus.NOTFOUND)
            {
                var tokens = ReadLeftToken(count - 2, ref ptr);
                if (tokens < count - 2)
                    return false;
            }

            //write result to output
            while (!RespWriteUtils.WriteInteger(listItemsDoneCount, ref dcurr, dend))
                SendAndReset();

            //reset session counters
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
        /// <param name="lop"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool ListPop<TGarnetApi>(int count, byte* ptr, ListOperation lop, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // Get the key for List
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
            int popCount = 1;

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            if (count == 3)
            {
                // Read count
                if (!RespReadUtils.ReadIntWithLengthHeader(out popCount, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.ListOp = lop;
            inputPtr->done = 0;
            inputPtr->count = popCount;

            GarnetStatus statusOp = GarnetStatus.NOTFOUND;

            if (lop == ListOperation.LPOP)
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
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
        private unsafe bool ListLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // Get the key for List
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

            // save old values
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - ptr) + sizeof(ObjectInputHeader);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.ListOp = ListOperation.LLEN;
            inputPtr->count = count;
            inputPtr->done = 0;

            var status = storageApi.ListLength(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            //restore input buffer
            *inputPtr = save;

            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Process output
                while (!RespWriteUtils.WriteInteger(output.countDone, ref dcurr, dend))
                    SendAndReset();
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
        private unsafe bool ListTrim<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                            where TGarnetApi : IGarnetApi
        {
            ptr += 11;

            if (count != 4)
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
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
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
                inputPtr->header.ListOp = ListOperation.LTRIM;
                inputPtr->count = start;
                inputPtr->done = stop;

                var statusOp = storageApi.ListTrim(key, new ArgSlice((byte*)inputPtr, inputLength));

                //restore input buffer
                *inputPtr = save;

                //GarnetStatus.OK or NOTFOUND have same result
                // no need to process output, just send OK
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
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
        private unsafe bool ListRange<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            if (count != 4)
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
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
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
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
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
        private unsafe bool ListIndex<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            if (count != 3)
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
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
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
                inputPtr->header.ListOp = ListOperation.LINDEX;
                inputPtr->count = index;
                inputPtr->done = 0;

                var statusOp = storageApi.ListIndex(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input
                *inputPtr = save;

                var error = CmdStrings.RESP_ERRNOTFOUND;

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        if (objOutputHeader.opsDone != -1)
                            error = default;
                        break;
                }

                if (error != default)
                {
                    while (!RespWriteUtils.WriteResponse(error, ref dcurr, dend))
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
        private unsafe bool ListInsert<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 13;

            if (count != 5)
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
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
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
                inputPtr->header.ListOp = ListOperation.LINSERT;
                inputPtr->done = 0;
                inputPtr->count = 0;

                var statusOp = storageApi.ListInsert(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                //restore input buffer
                *inputPtr = save;

                switch (statusOp)
                {
                    case GarnetStatus.OK:
                        //TODO: validation for different object type, pending to review
                        if (output.countDone == 0 && output.countDone == 0 && output.bytesDone == 0)
                        {
                            ReadOnlySpan<byte> errorMessage = "-ERR wrong key type used in LINSERT command.\r\n"u8;
                            while (!RespWriteUtils.WriteResponse(errorMessage, ref dcurr, dend))
                                SendAndReset();
                        }
                        //check for partial execution
                        if (output.countDone == int.MinValue)
                            return false;
                        //process output
                        ptr += output.bytesDone;
                        while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        var tokens = ReadLeftToken(count - 2, ref ptr);
                        if (tokens < count - 2)
                            return false;
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
        private unsafe bool ListRemove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            ptr += 10;
            // if params are missing return error
            if (count != 4)
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
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
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
                inputPtr->header.ListOp = ListOperation.LREM;
                inputPtr->count = nCount;
                inputPtr->done = 0;

                var statusOp = storageApi.ListRemove(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);
                //restore input buffer
                *inputPtr = save;

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
                        var tokens = ReadLeftToken(count - 3, ref ptr);
                        if (tokens < count - 3)
                            return false;
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
        private unsafe bool ListMove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 11;
            bool result = false;

            if (count != 5)
            {
                return AbortWithWrongNumberOfArguments("LMOVE", count);
            }
            else
            {
                ArgSlice sourceKey = default, destinationKey = default, param1 = default, param2 = default;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref param1.ptr, ref param1.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref param2.ptr, ref param2.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                var sourceDirection = param1.Bytes.SequenceEqual(Encoding.ASCII.GetBytes("RIGHT")) ? OperationDirection.Right : OperationDirection.Left;
                var destinationDirection = param2.Bytes.SequenceEqual(Encoding.ASCII.GetBytes("RIGHT")) ? OperationDirection.Right : OperationDirection.Left;

                result = ListMove(count, sourceKey, destinationKey, sourceDirection, destinationDirection, out var node, ref storageApi);
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
            }

            // Move input head, write result to output
            readHead = (int)(ptr - recvBufferPtr);
            return result;
        }

        /// <summary>
        /// RPOPLPUSH source destination
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool ListRightPopLeftPush<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 15;
            bool result = false;

            if (count != 3)
            {
                return AbortWithWrongNumberOfArguments("RPOPLPUSH", count);
            }
            else
            {
                ArgSlice sourceKey = default, destinationKey = default;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                result = ListMove(count, sourceKey, destinationKey, OperationDirection.Right, OperationDirection.Left, out var node, ref storageApi);

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
            }

            // update read pointers
            readHead = (int)(ptr - recvBufferPtr);
            return result;
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
        /// <returns></returns>
        private unsafe bool ListMove<TGarnetApi>(int count, ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] node, ref TGarnetApi storageApi)
                where TGarnetApi : IGarnetApi
        {
            ArgSlice[] keys = new ArgSlice[2] { sourceKey, destinationKey };
            node = null;
            if (NetworkKeyArraySlotVerify(ref keys, false))
            {
                // check for non crosslot error
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                {
                    return false;
                }
                return true;
            }

            return storageApi.ListMove(sourceKey, destinationKey, sourceDirection, destinationDirection, out node);
        }
    }
}