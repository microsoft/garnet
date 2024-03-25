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
        /// Session counter of number of Hash entries partially done
        /// </summary>
        int hashItemsDoneCount;

        /// <summary>
        /// Session counter of number of Hash operations partially done
        /// </summary>
        int hashOpsCount;

        /// <summary>
        /// HashSet/HSET key field value [field value ...]: Sets the specified field(s) to their respective value(s) in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// HashSetWhenNotExists/HSETNX key field value: Sets only if field does not yet exist. A new hash is created if it does not exists.
        /// If field exists the operation has no effect.
        /// HMSET key field value [field value ...](deprecated) Same effect as HSET
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="hop"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashSet<TGarnetApi>(int count, byte* ptr, HashOperation hop, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += hop == HashOperation.HSET ? 10 : (hop == HashOperation.HSETNX ? 12 : 11);

            // Get the key for Hash
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

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - ptr) + sizeof(ObjectInputHeader);

            var inputCount = (count - 2) / 2;

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.HashOp = hop;
            inputPtr->count = inputCount;
            inputPtr->done = hashOpsCount;

            storageApi.HashSet(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            *inputPtr = save; // reset input buffer

            hashItemsDoneCount += output.countDone;
            hashOpsCount += output.opsDone;

            // Reset buffer and return if HSET did not process the entire command tokens
            if (hashItemsDoneCount < inputCount)
                return false;

            // Move head, write result to output, reset session counters
            ptr += output.bytesDone;
            readHead = (int)(ptr - recvBufferPtr);

            if (hop == HashOperation.HMSET)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteInteger(hashOpsCount, ref dcurr, dend))
                    SendAndReset();
            }

            hashItemsDoneCount = hashOpsCount = 0;
            return true;
        }

        /// <summary>
        /// HashGet: Returns the value associated with field in the hash stored at key.
        /// HashGetAll: Returns all fields and values of the hash stored at key.
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="op">HGET, HGETALL, HMGET, HRANDFIELD</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashGet<TGarnetApi>(int count, byte* ptr, HashOperation op, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += op == HashOperation.HGET ? 10 : (op == HashOperation.HGETALL ? 13 : (op == HashOperation.HMGET ? 11 : 17));

            if ((op == HashOperation.HGETALL && count - 2 != 0) ||
                (op == HashOperation.HRANDFIELD && count - 2 < 0) ||
                (op == HashOperation.HGET && count != 3) ||
                (op == HashOperation.HMGET && count < 3))
            {
                return AbortWithWrongNumberOfArguments(op.ToString(), count);
            }
            else
            {
                // Get the key for Hash
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

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                int inputCount = op == HashOperation.HGETALL ? 0 : (op == HashOperation.HRANDFIELD ? count : count - 2);
                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Hash;
                inputPtr->header.HashOp = op;
                inputPtr->count = inputCount;
                inputPtr->done = hashItemsDoneCount;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = GarnetStatus.NOTFOUND;

                var includeCountParameter = false;
                if (op == HashOperation.HRANDFIELD)
                {
                    includeCountParameter = inputPtr->count > 2; // 4 tokens are: command key count WITHVALUES
                    status = storageApi.HashRandomField(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
                }
                else
                    status = storageApi.HashGet(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                // Reset input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        ptr += objOutputHeader.bytesDone;
                        hashItemsDoneCount += objOutputHeader.countDone;
                        hashOpsCount += objOutputHeader.opsDone;
                        // Return if command is only partially done
                        if (hashItemsDoneCount < inputCount)
                            return false;
                        break;
                    case GarnetStatus.NOTFOUND:
                        if (op == HashOperation.HMGET && count - 2 >= 1)
                        {
                            // HMGET key field [field ...]
                            // Write an empty array of count - 2 elements with null values.
                            while (!RespWriteUtils.WriteArrayWithNullElements(count - 2, ref dcurr, dend))
                                SendAndReset();
                        }
                        else if (op != HashOperation.HMGET)
                        {
                            var respBytes = (includeCountParameter || op == HashOperation.HGETALL) ? CmdStrings.RESP_EMPTYLIST : CmdStrings.RESP_ERRNOTFOUND;
                            while (!RespWriteUtils.WriteResponse(respBytes, ref dcurr, dend))
                                SendAndReset();
                        }
                        ReadLeftToken(count - 2, ref ptr);
                        break;
                }
            }

            // Reset session counters
            hashItemsDoneCount = hashOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            if (count != 2)
            {
                hashItemsDoneCount = hashOpsCount = 0;
                return AbortWithWrongNumberOfArguments("HLEN", count);
            }
            else
            {
                // Get the key 
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

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = sizeof(ObjectInputHeader);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Hash;
                inputPtr->header.HashOp = HashOperation.HLEN;
                inputPtr->count = 1;
                inputPtr->done = 0;

                var status = storageApi.HashLength(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

                // Restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process output
                        while (!RespWriteUtils.WriteInteger(output.countDone, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashDelete<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            if (count < 2)
            {
                hashItemsDoneCount = hashOpsCount = 0;
                return AbortWithWrongNumberOfArguments("HDEL", count);
            }
            else
            {
                // Get the key for Hash
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    return true;
                }

                var inputCount = count - 2;

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Hash;
                inputPtr->header.HashOp = HashOperation.HDEL;
                inputPtr->count = inputCount;
                inputPtr->done = hashItemsDoneCount;

                var status = storageApi.HashDelete(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                // Restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        hashItemsDoneCount += output.countDone;
                        hashOpsCount += output.opsDone;
                        ptr += output.bytesDone;
                        // Reset buffer and return if HDEL is only partially done
                        if (hashItemsDoneCount < inputCount)
                            return false;
                        while (!RespWriteUtils.WriteInteger(hashOpsCount, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        hashItemsDoneCount = hashOpsCount = 0;
                        ReadLeftToken(count - 2, ref ptr);
                        break;
                }
            }

            // Restart session counters
            hashItemsDoneCount = hashOpsCount = 0;
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashExists<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            ptr += 13;

            if (count != 3)
            {
                hashItemsDoneCount = hashOpsCount = 0;
                return AbortWithWrongNumberOfArguments("HEXISTS", count);
            }
            else
            {
                // Get the key for Hash
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

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Hash;
                inputPtr->header.HashOp = HashOperation.HEXISTS;
                inputPtr->count = 1;
                inputPtr->done = 0;

                var status = storageApi.HashExists(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                // Restore input buffer
                *inputPtr = save;

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process output
                        while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                            SendAndReset();
                        ptr += output.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                        ReadLeftToken(count - 2, ref ptr);
                        break;
                }
            }

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// HashKeys: Returns all field names in the hash key.
        /// HashVals: Returns all values in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        private unsafe bool HashKeys<TGarnetApi>(int count, byte* ptr, HashOperation op, ref TGarnetApi storageApi)
          where TGarnetApi : IGarnetApi
        {
            ptr += 11;

            // Get the key for Hash
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

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.HashOp = op;
            inputPtr->count = count - 2;
            inputPtr->done = hashOpsCount;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (op == HashOperation.HKEYS)
                status = storageApi.HashKeys(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
            else
                status = storageApi.HashVals(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    // CountDone: how many keys total
                    hashItemsDoneCount = objOutputHeader.countDone;
                    hashOpsCount += objOutputHeader.opsDone;
                    if (hashItemsDoneCount > hashOpsCount)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    ReadLeftToken(count - 2, ref ptr);
                    break;
            }

            // Reset session counters
            hashItemsDoneCount = hashOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// HashIncrement: Increments the number stored at field in the hash stored at key by increment.
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment. 
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        private unsafe bool HashIncrement<TGarnetApi>(int count, byte* ptr, HashOperation op, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += op == HashOperation.HINCRBY ? 13 : 19;

            // Check if parameters number is right
            if (count != 4)
            {
                return AbortWithWrongNumberOfArguments(op == HashOperation.HINCRBY ? "HINCRBY" : "HINCRBYFLOAT", count);
            }
            else
            {
                // Get the key for Hash
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

                // Save old values for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Hash;
                inputPtr->header.HashOp = op;
                inputPtr->count = count;
                inputPtr->done = 0;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.HashIncrement(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                // Restore input
                *inputPtr = save;

                // Process output
                var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                if (objOutputHeader.opsDone == Int32.MinValue)
                {
                    // Command was partially done
                    return false;
                }
                ptr += objOutputHeader.bytesDone;

            }
            // Reset counters
            hashItemsDoneCount = hashOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}