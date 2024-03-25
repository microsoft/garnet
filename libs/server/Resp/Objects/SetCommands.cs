// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    ///  Set - RESP specific operations
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Session counter of number of Set entries partially done
        /// </summary>
        int setItemsDoneCount;

        /// <summary>
        /// Session counter of number of Set operations partially done
        /// </summary>
        int setOpsCount;

        /// <summary>
        ///  Add the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored. 
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetAdd<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // Get the key for the Set
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
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
            inputPtr->header.type = GarnetObjectType.Set;
            inputPtr->header.SetOp = SetOperation.SADD;
            inputPtr->count = inputCount;
            inputPtr->done = setOpsCount;

            storageApi.SetAdd(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Restore input buffer
            *inputPtr = save;

            setItemsDoneCount += output.countDone;
            setOpsCount += output.opsDone;

            // Reset buffer and return if SADD is only partially done
            if (setOpsCount < inputCount)
                return false;

            // Move head, write result to output, reset session counters
            ptr += output.bytesDone;
            readHead = (int)(ptr - recvBufferPtr);

            while (!RespWriteUtils.WriteInteger(setItemsDoneCount, ref dcurr, dend))
                SendAndReset();

            setItemsDoneCount = setOpsCount = 0;
            return true;
        }

        /// <summary>
        /// Remove the specified members from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetRemove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // Get the key 
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }
            var inputCount = count - 2; // only identifiers

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Set;
            inputPtr->header.SetOp = SetOperation.SREM;
            inputPtr->count = inputCount;
            inputPtr->done = setItemsDoneCount;

            var status = storageApi.SetRemove(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            // Restore input buffer
            *inputPtr = save;

            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
                ReadLeftToken(count - 2, ref ptr);
            }
            else
            {
                setItemsDoneCount += output.countDone;
                setOpsCount += output.opsDone;

                // Reset buffer and return if command is only partially done
                if (setOpsCount < inputCount)
                    return false;

                // Move head, write result to output, reset session counters
                ptr += output.bytesDone;

                while (!RespWriteUtils.WriteInteger(setItemsDoneCount, ref dcurr, dend))
                    SendAndReset();

                setOpsCount = setItemsDoneCount = 0;
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 11;

            if (count != 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SCARD", count);

            }
            else
            {

                // Get the key for the Set
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
                inputPtr->header.type = GarnetObjectType.Set;
                inputPtr->header.SetOp = SetOperation.SCARD;
                inputPtr->count = 1;
                inputPtr->done = 0;

                var status = storageApi.SetLength(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                // Restore input buffer
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
            }
            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetMembers<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 14;

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

            // Save old values
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Set;
            inputPtr->header.SetOp = SetOperation.SMEMBERS;
            inputPtr->count = count;
            inputPtr->done = setItemsDoneCount;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetMembers(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    setItemsDoneCount += objOutputHeader.countDone;
                    if (setItemsDoneCount > objOutputHeader.opsDone)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    ReadLeftToken(count - 2, ref ptr);
                    break;
            }

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetPop<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // Get the key
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
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Set;
            inputPtr->header.SetOp = SetOperation.SPOP;
            inputPtr->count = Int32.MinValue;

            int countParameter = 0;
            if (count == 3)
            {
                // Get the value for the count parameter
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countParameterByteArray, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Prepare response
                var canParse = Int32.TryParse(Encoding.ASCII.GetString(countParameterByteArray), out countParameter);
                if (!canParse || countParameter < 0)
                {
                    ReadOnlySpan<byte> errorMessage = "-ERR value is not an integer or out of range\r\n"u8;
                    while (!RespWriteUtils.WriteResponse(errorMessage, ref dcurr, dend))
                        SendAndReset();

                    // Restore input buffer
                    *inputPtr = save;

                    // Move input head
                    readHead = (int)(ptr - recvBufferPtr);
                    return true;
                }
                else if (countParameter == 0)
                {
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();

                    // Restore input buffer
                    *inputPtr = save;

                    // Move input head
                    readHead = (int)(ptr - recvBufferPtr);
                    return true;
                }
                inputPtr->count = countParameter;
            }

            inputPtr->done = 0;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    setItemsDoneCount += objOutputHeader.countDone;
                    if (count == 3 && setItemsDoneCount < countParameter)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

    }
}