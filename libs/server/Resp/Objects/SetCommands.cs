// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            if (count < 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SADD", count);
            }
            else
            {
                // Get the key for the Set
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(key, false))
                {
                    var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                    if (!DrainCommands(bufSpan, count)) return false;
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
                inputPtr->header.type = GarnetObjectType.Set;
                inputPtr->header.flags = 0;
                inputPtr->header.SetOp = SetOperation.SADD;
                inputPtr->count = inputCount;
                inputPtr->done = setOpsCount;

                var status = storageApi.SetAdd(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

                // Restore input buffer
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
                        setItemsDoneCount += output.countDone;
                        setOpsCount += output.opsDone;

                        // Reset buffer and return if SADD is only partially done
                        if (setOpsCount < inputCount)
                            return false;

                        // Move head, write result to output, reset session counters
                        ptr += output.bytesDone;

                        while (!RespWriteUtils.WriteInteger(setItemsDoneCount, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            setItemsDoneCount = setOpsCount = 0;
            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private bool SetIntersect<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments("SINTER", count);
            }

            // Read all the keys
            ArgSlice[] keys = new ArgSlice[count];

            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetIntersect(keys, out var result);

            switch (status)
            {
                case GarnetStatus.OK:
                    // write the size of result
                    int resultCount = 0;
                    if (result != null)
                    {
                        resultCount = result.Count;
                        while (!RespWriteUtils.WriteArrayLength(resultCount, ref dcurr, dend))
                            SendAndReset();

                        foreach (var item in result)
                        {
                            while (!RespWriteUtils.WriteBulkString(item, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteArrayLength(resultCount, ref dcurr, dend))
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
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetIntersectStore<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("SINTERSTORE", count);
            }

            // Get the key
            if (!RespReadUtils.TrySliceWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            var keys = new ArgSlice[count - 1];
            for (var i = 0; i < count - 1; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetIntersectStore(key.ToArray(), keys, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(output, ref dcurr, dend))
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
        /// Returns the members of the set resulting from the union of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private bool SetUnion<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments("SUNION", count);
            }

            // Read all the keys
            ArgSlice[] keys = new ArgSlice[count];

            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetUnion(keys, out var result);

            switch (status)
            {
                case GarnetStatus.OK:
                    // write the size of result
                    var resultCount = result.Count;
                    while (!RespWriteUtils.WriteArrayLength(resultCount, ref dcurr, dend))
                        SendAndReset();

                    foreach (var item in result)
                    {
                        while (!RespWriteUtils.WriteBulkString(item, ref dcurr, dend))
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
        /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetUnionStore<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("SUNIONSTORE", count);
            }

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

            var keys = new ArgSlice[count - 1];
            for (var i = 0; i < count - 1; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetUnionStore(key, keys, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(output, ref dcurr, dend))
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
            if (count < 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SREM", count);
            }
            else
            {
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
                var inputCount = count - 1; // only identifiers

                // Prepare input
                var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

                // Save old values on buffer for possible revert
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Set;
                inputPtr->header.flags = 0;
                inputPtr->header.SetOp = SetOperation.SREM;
                inputPtr->count = inputCount;
                inputPtr->done = setItemsDoneCount;

                var status = storageApi.SetRemove(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

                // Restore input buffer
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
            if (count != 1)
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
                inputPtr->header.flags = 0;
                inputPtr->header.SetOp = SetOperation.SCARD;
                inputPtr->count = 1;
                inputPtr->done = 0;

                var status = storageApi.SetLength(key, new ArgSlice((byte*)inputPtr, inputLength), out var output);

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
            if (count != 1)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SMEMBERS", count);
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

                // Save old values
                var save = *inputPtr;

                // Prepare length of header in input buffer
                var inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

                // Prepare header in input buffer
                inputPtr->header.type = GarnetObjectType.Set;
                inputPtr->header.flags = 0;
                inputPtr->header.SetOp = SetOperation.SMEMBERS;
                inputPtr->count = count;
                inputPtr->done = setItemsDoneCount;

                // Prepare GarnetObjectStore output
                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

                var status = storageApi.SetMembers(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

                // Restore input buffer
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
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private unsafe bool SetIsMember<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SISMEMBER", count);
            }

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
            inputPtr->header.flags = 0;
            inputPtr->header.SetOp = SetOperation.SISMEMBER;
            inputPtr->count = count - 2;
            inputPtr->done = 0;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetIsMember(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Restore input buffer
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
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    setItemsDoneCount += objOutputHeader.countDone;
                    if (setItemsDoneCount > objOutputHeader.opsDone)
                        return false;
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
            if (count < 1 || count > 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SPOP", count);
            }

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
            inputPtr->header.flags = 0;
            inputPtr->header.SetOp = SetOperation.SPOP;
            inputPtr->count = int.MinValue;

            int countParameter = 0;
            if (count == 2)
            {
                // Get the value for the count parameter
                if (!RespReadUtils.TrySliceWithLengthHeader(out var countParameterBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Prepare response
                if (!NumUtils.TryParse(countParameterBytes, out countParameter) || countParameter < 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
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
                    if (count == 2 && setItemsDoneCount < countParameter)
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

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Moves a member from a source set to a destination set.
        /// If the move was performed, this command returns 1.
        /// If the member was not found in the source set, or if no operation was performed, this command returns 0.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetMove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 3)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SMOVE", count);
            }

            ArgSlice sourceKey = default;
            ArgSlice destinationKey = default;
            ArgSlice sourceMember = default;

            // Get the source key 
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Get the destination key
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Get the member to move
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceMember.ptr, ref sourceMember.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var keys = new ArgSlice[2] { sourceKey, destinationKey };

            if (NetworkKeyArraySlotVerify(ref keys, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetMove(sourceKey, destinationKey, sourceMember, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(output, ref dcurr, dend))
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

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// When called with just the key argument, return a random element from the set value stored at key.
        /// If the provided count argument is positive, return an array of distinct elements. 
        /// The array's length is either count or the set's cardinality (SCARD), whichever is lower.
        /// If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times. 
        /// In this case, the number of returned elements is the absolute value of the specified count.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetRandomMember<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1 || count > 2)
            {
                setItemsDoneCount = setOpsCount = 0;
                return AbortWithWrongNumberOfArguments("SRANDMEMBER", count);
            }

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
            inputPtr->header.type = GarnetObjectType.Set;
            inputPtr->header.flags = 0;
            inputPtr->header.SetOp = SetOperation.SRANDMEMBER;
            inputPtr->count = Int32.MinValue;

            int countParameter = 0;
            if (count == 2)
            {
                // Get the value for the count parameter
                if (!RespReadUtils.TrySliceWithLengthHeader(out var countParameterBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Prepare response
                if (!NumUtils.TryParse(countParameterBytes, out countParameter))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
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

            var status = storageApi.SetRandomMember(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    setItemsDoneCount += objOutputHeader.countDone;
                    if (count == 2 && setItemsDoneCount < countParameter)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    if (count == 2)
                    {
                        while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        break;
                    }

                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            // Reset session counters
            setItemsDoneCount = setOpsCount = 0;

            // Move input head
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetDiff<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments("SDIFF", count);
            }

            var keys = new ArgSlice[count];
            for (var i = 0; i < count; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetDiff(keys, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (output == null || output.Count == 0)
                    {
                        while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteArrayLength(output.Count, ref dcurr, dend))
                            SendAndReset();
                        foreach (var item in output)
                        {
                            while (!RespWriteUtils.WriteBulkString(item, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
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

        private bool SetDiffStore<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("SDIFFSTORE", count);
            }

            // Get the key
            if (!RespReadUtils.TrySliceWithLengthHeader(out var key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            var keys = new ArgSlice[count - 1];
            for (var i = 0; i < count - 1; i++)
            {
                keys[i] = default;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keys[i].ptr, ref keys[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            if (NetworkKeyArraySlotVerify(ref keys, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count)) return false;
                return true;
            }

            var status = storageApi.SetDiffStore(key.ToArray(), keys, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(output, ref dcurr, dend))
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
    }
}