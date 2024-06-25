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
        /// <returns></returns>
        private unsafe bool ObjectScan<TGarnetApi>(int count, byte* ptr, GarnetObjectType objectType, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            // Check number of required parameters
            if (count < 2)
            {
                var cmdName = objectType switch
                {
                    GarnetObjectType.Hash => nameof(HashOperation.HSCAN),
                    GarnetObjectType.Set => nameof(SetOperation.SSCAN),
                    GarnetObjectType.SortedSet => nameof(SortedSetOperation.ZSCAN),
                    GarnetObjectType.All => nameof(RespCommand.COSCAN),
                    _ => nameof(RespCommand.NONE)
                };

                return AbortWithWrongNumberOfArguments(cmdName, count);
            }

            // Read key for the scan
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Get cursor value
            if (!RespReadUtils.TrySliceWithLengthHeader(out var cursorBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!NumUtils.TryParse(cursorBytes, out int cursorValue) || cursorValue < 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CURSORVALUE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (NetworkSingleKeySlotVerify(key, false))
            {
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
            (*(ObjectInputHeader*)(pcurr)).header.flags = 0;

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
            (*(ObjectInputHeader*)(pcurr)).arg1 = count - 2;

            // Cursor value
            (*(ObjectInputHeader*)(pcurr)).arg2 = cursorValue;
            pcurr += ObjectInputHeader.Size;

            // Object Input Limit
            *(int*)pcurr = storeWrapper.serverOptions.ObjectScanCountLimit;
            pcurr += sizeof(int);

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = storageApi.ObjectScan(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input buffer
            *inputPtr = save;
            *ptrToInt = savePtrToInt;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    // Validation for partial input reading or error
                    if (objOutputHeader.result1 == int.MinValue)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteScanOutputHeader(0, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // Update read pointer
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }
    }
}