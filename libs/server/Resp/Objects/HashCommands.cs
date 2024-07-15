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
        /// HashSet/HSET key field value [field value ...]: Sets the specified field(s) to their respective value(s) in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// HashSetWhenNotExists/HSETNX key field value: Sets only if field does not yet exist. A new hash is created if it does not exists.
        /// If field exists the operation has no effect.
        /// HMSET key field value [field value ...](deprecated) Same effect as HSET
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashSet<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (((command == RespCommand.HSET || command == RespCommand.HMSET)
                  && (count == 1 || count % 2 != 1)) ||
                (command == RespCommand.HSETNX && count != 3))
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - ptr) + sizeof(ObjectInputHeader);

            var inputCount = (count - 1) / 2;

            var hop =
                command switch
                {
                    RespCommand.HSET => HashOperation.HSET,
                    RespCommand.HMSET => HashOperation.HMSET,
                    RespCommand.HSETNX => HashOperation.HSETNX,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = hop;
            inputPtr->arg1 = inputCount;

            var status = storageApi.HashSet(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            *inputPtr = save; // reset input buffer

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (command == RespCommand.HMSET)
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
            }
            return true;
        }


        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGet<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 2)
                return AbortWithWrongNumberOfArguments(command.ToString(), count);

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
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HGET;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.HashGet(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

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
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGetAll<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 1)
                return AbortWithWrongNumberOfArguments(command.ToString(), count);

            // Get the hash key
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
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HGETALL;
            inputPtr->arg1 = respProtocolVersion;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.HashGetAll(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            return true;
        }

        /// <summary>
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGetMultiple<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
                return AbortWithWrongNumberOfArguments(command.ToString(), count);

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
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HMGET;
            inputPtr->arg1 = count - 1;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.HashGetMultiple(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    // Write an empty array of count - 1 elements with null values.
                    while (!RespWriteUtils.WriteArrayWithNullElements(count - 1, ref dcurr, dend))
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
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashRandomField<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 3)
                return AbortWithWrongNumberOfArguments(command.ToString(), count);

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var paramCount = 1;
            var withValues = false;
            var includedCount = false;

            if (count >= 2)
            {
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

                // Read WITHVALUES
                if (count == 3)
                {
                    var withValuesSlice = parseState.GetArgSliceByRef(2);

                    if (!withValuesSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHVALUES))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    var sbWithValues = withValuesSlice.SpanByte;
                    ptr = sbWithValues.ToPointer() + sbWithValues.Length + 2;
                    withValues = true;
                }
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Create a random seed
            var seed = RandomGen.Next();

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HRANDFIELD;
            inputPtr->arg1 = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (withValues ? 1 : 0);
            inputPtr->arg2 = seed;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = GarnetStatus.NOTFOUND;

            // This prevents going to the backend if HRANDFIELD is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare GarnetObjectStore output
                outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                status = storageApi.HashRandomField(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
            }

            // Reset input buffer
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
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashLength<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments("HLEN", count);
            }

            // Get the key 
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
            var inputLength = sizeof(ObjectInputHeader);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HLEN;
            inputPtr->arg1 = 1;

            var status = storageApi.HashLength(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Restore input buffer
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
        /// Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private unsafe bool HashStrLength<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("HSTRLEN", count);
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

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HSTRLEN;
            inputPtr->arg1 = 1;

            var status = storageApi.HashStrLength(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            // Restore input buffer
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
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashDelete<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments("HDEL", count);
            }

            // Get the key for Hash
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var ptr = sbKey.ToPointer() + sbKey.Length + 2;

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var inputCount = count - 1;

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HDEL;
            inputPtr->arg1 = inputCount;

            var status = storageApi.HashDelete(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
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
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashExists<TGarnetApi>(int count, ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            if (count != 2)
            {
                return AbortWithWrongNumberOfArguments("HEXISTS", count);
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

            // Save old values on buffer for possible revert
            var save = *inputPtr;

            // Prepare length of header in input buffer
            var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = HashOperation.HEXISTS;
            inputPtr->arg1 = 1;

            var status = storageApi.HashExists(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), out var output);

            // Restore input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
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
        /// HashKeys: Returns all field names in the hash key.
        /// HashVals: Returns all values in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashKeys<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
          where TGarnetApi : IGarnetApi
        {
            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments(command.ToString(), count);
            }

            // Get the key for Hash
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

            HashOperation op =
                command switch
                {
                    RespCommand.HKEYS => HashOperation.HKEYS,
                    RespCommand.HVALS => HashOperation.HVALS,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = op;
            inputPtr->arg1 = count - 1;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            GarnetStatus status = GarnetStatus.NOTFOUND;

            if (command == RespCommand.HKEYS)
                status = storageApi.HashKeys(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
            else
                status = storageApi.HashVals(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

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
            return true;
        }

        /// <summary>
        /// HashIncrement: Increments the number stored at field in the hash stored at key by increment.
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment. 
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashIncrement<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Check if parameters number is right
            if (count != 3)
            {
                // Send error to output
                return AbortWithWrongNumberOfArguments(command == RespCommand.HINCRBY ? "HINCRBY" : "HINCRBYFLOAT", count);
            }

            // Get the key for Hash
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

            HashOperation op =
                command switch
                {
                    RespCommand.HINCRBY => HashOperation.HINCRBY,
                    RespCommand.HINCRBYFLOAT => HashOperation.HINCRBYFLOAT,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.Hash;
            inputPtr->header.flags = 0;
            inputPtr->header.HashOp = op;
            inputPtr->arg1 = count + 1;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.HashIncrement(keyBytes, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
            }
            return true;
        }
    }
}