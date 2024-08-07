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
        ///  Add the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored. 
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetAdd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("SADD");
            }

            // Get the key for the Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SADD,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = storageApi.SetAdd(keyBytes, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    // Write result to output
                    while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private bool SetIntersect<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("SINTER");
            }

            // Read all keys
            var keys = new ArgSlice[parseState.Count];
            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

            var status = storageApi.SetIntersect(keys, out var result);

            switch (status)
            {
                case GarnetStatus.OK:
                    // write the size of result
                    var resultCount = 0;
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

            return true;
        }

        /// <summary>
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetIntersectStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("SINTERSTORE");
            }

            // Get the key
            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            var keys = new ArgSlice[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            var status = storageApi.SetIntersectStore(keyBytes, keys, out var output);

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

            return true;
        }


        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private bool SetUnion<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("SUNION");
            }

            // Read all the keys
            var keys = new ArgSlice[parseState.Count];

            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

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

            return true;
        }

        /// <summary>
        /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetUnionStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("SUNIONSTORE");
            }

            // Get the key
            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            var keys = new ArgSlice[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            var status = storageApi.SetUnionStore(keyBytes, keys, out var output);

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

            return true;
        }

        /// <summary>
        /// Remove the specified members from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetRemove<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("SREM");
            }

            // Get the key 
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SREM,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = storageApi.SetRemove(keyBytes, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Write result to output
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
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("SCARD");
            }

            // Get the key for the Set
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SCARD,
                },
            };

            var status = storageApi.SetLength(keyBytes, ref input, out var output);

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
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetMembers<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("SMEMBERS");
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SMEMBERS,
                },
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetMembers(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
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

        private unsafe bool SetIsMember<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("SISMEMBER");
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SISMEMBER,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetIsMember(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetPop<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments("SPOP");
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var countParameter = int.MinValue;
            if (parseState.Count == 2)
            {
                // Prepare response
                if (!parseState.TryGetInt(1, out countParameter)|| countParameter < 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                if (countParameter == 0)
                {
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SPOP,
                },
                arg1 = countParameter,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetPop(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
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
        /// Moves a member from a source set to a destination set.
        /// If the move was performed, this command returns 1.
        /// If the member was not found in the source set, or if no operation was performed, this command returns 0.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetMove<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments("SMOVE");
            }

            // Get the source key 
            var sourceKey = parseState.GetArgSliceByRef(0);

            // Get the destination key
            var destinationKey = parseState.GetArgSliceByRef(1);

            // Get the member to move
            var sourceMember = parseState.GetArgSliceByRef(2);

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 2))
                return true;

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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetRandomMember<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments("SRANDMEMBER");
            }

            // Get the key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var countParameter = int.MinValue;
            if (parseState.Count == 2)
            {
                // Prepare response
                if (!parseState.TryGetInt(1, out countParameter))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                if (countParameter == 0)
                {
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
            }

            // Create a random seed
            var seed = RandomGen.Next();

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Set,
                    SetOp = SetOperation.SRANDMEMBER,
                },
                arg1 = countParameter,
                arg2 = seed,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SetRandomMember(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    if (parseState.Count == 2)
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

            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetDiff<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("SDIFF");
            }

            var keys = new ArgSlice[parseState.Count];
            for (var i = 0; i < parseState.Count; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

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

            return true;
        }

        private bool SetDiffStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("SDIFFSTORE");
            }

            // Get the key
            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            var keys = new ArgSlice[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            var status = storageApi.SetDiffStore(keyBytes, keys, out var output);

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

            return true;
        }
    }
}