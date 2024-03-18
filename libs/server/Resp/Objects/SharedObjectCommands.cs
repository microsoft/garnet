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
        /// Iterates over the associated items of a key,
        /// using a pattern to match and count to limit how many items to return.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count">Number of tokens in the buffer, including the name of the command</param>
        /// <param name="ptr">Pointer to the inpu buffer</param>
        /// <param name="objectType">SortedSet, Hash or Set type</param>
        /// <param name="storageApi">The storageAPI object</param>
        /// <param name="cmdlength">Indicates the length to advance the read pointer based on the command name</param>
        /// <returns></returns>
        private unsafe bool ObjectScan<TGarnetApi>(int count, byte* ptr, GarnetObjectType objectType, ref TGarnetApi storageApi, int cmdlength = 11)
             where TGarnetApi : IGarnetApi
        {
            ptr += cmdlength;

            // Check number of required parameters
            if (count < 3)
            {
                // Forward tokens in the input
                ReadLeftToken(count - 1, ref ptr);
            }
            else
            {
                // Read key for the scan
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Get cursor value
                if (!RespReadUtils.ReadStringWithLengthHeader(out var cursor, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!Int32.TryParse(cursor, out int cursorValue) || cursorValue < 0)
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRORCURSORVALUE, ref dcurr, dend))
                        SendAndReset();
                    ReadLeftToken(count - 2, ref ptr);
                    return true;
                }

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    return true;
                }

                // Prepare input
                // Header + size of int for the limitCountInOutput
                var inputPtr = (ObjectInputHeader*)(ptr - ObjectInputHeader.Size - sizeof(int));
                var ptrToInt = (int*)(ptr - sizeof(int));

                // Save old values on buffer for possible revert
                var save = *inputPtr;
                var savePtrToInt = *ptrToInt;

                // Build the input
                byte* pcurr = (byte*)inputPtr;

                // ObjectInputHeader
                (*(ObjectInputHeader*)(pcurr)).header.type = objectType;

                switch (objectType)
                {
                    case GarnetObjectType.Hash:
                        (*(ObjectInputHeader*)(pcurr)).header.HashOp = HashOperation.HSCAN;
                        break;
                    case GarnetObjectType.Set:
                        (*(ObjectInputHeader*)(pcurr)).header.SetOp = SetOperation.SSCAN;
                        break;
                    case GarnetObjectType.SortedSet:
                        (*(ObjectInputHeader*)(pcurr)).header.SortedSetOp = SortedSetOperation.ZSCAN;
                        break;
                    case GarnetObjectType.All:
                        (*(ObjectInputHeader*)(pcurr)).header.cmd = RespCommand.COSCAN;
                        break;
                }

                // Tokens already processed: 3, command, key and cursor
                (*(ObjectInputHeader*)(pcurr)).count = count - 3;

                // Cursor value
                (*(ObjectInputHeader*)(pcurr)).done = cursorValue;
                pcurr += ObjectInputHeader.Size;

                // Object Input Limit
                *(int*)pcurr = storeWrapper.serverOptions.ObjectScanCountLimit;
                pcurr += sizeof(int);

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                var status = storageApi.ObjectScan(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                //restore input buffer
                *inputPtr = save;
                *ptrToInt = savePtrToInt;

                switch (status)
                {
                    case GarnetStatus.OK:
                        // Process output
                        var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                        // Validation for partial input reading or error
                        if (objOutputHeader.countDone == Int32.MinValue)
                            return false;
                        ptr += objOutputHeader.bytesDone;
                        break;
                    case GarnetStatus.NOTFOUND:
                        while (!RespWriteUtils.WriteScanOutputHeader(0, ref dcurr, dend))
                            SendAndReset();
                        while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        // Fast forward left of the input
                        ReadLeftToken(count - 3, ref ptr);
                        break;
                }

            }

            // Update read pointer
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}