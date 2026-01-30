// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        ///  Add the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
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
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState, startIdx: 1) { SetOp = SetOperation.SADD };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetAdd(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
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
            var keys = new PinnedSpanByte[parseState.Count];
            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetIntersect(keys, out var result);
            switch (status)
            {
                case GarnetStatus.OK:
                    // write the size of result
                    var resultCount = 0;
                    if (result != null)
                    {
                        WriteSetLength(result.Count);

                        foreach (var item in result)
                        {
                            while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteArrayLength(resultCount, ref dcurr, dend))
                            SendAndReset();
                    }

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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
            var key = parseState.GetArgSliceByRef(0);

            var keys = new PinnedSpanByte[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetIntersectStore(key, keys, out var output);
            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(output, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the cardinality of the intersection between multiple sets.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private bool SetIntersectLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2) // Need at least numkeys + 1 key
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SINTERCARD));
            }

            // Number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if ((parseState.Count - 1) < nKeys)
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrShouldBeGreaterThanZero, "numkeys")));
            }

            var keys = parseState.Parameters.Slice(1, nKeys);

            // Optional LIMIT argument
            int? limit = null;
            if (parseState.Count > nKeys + 1)
            {
                var limitArg = parseState.GetArgSliceByRef(nKeys + 1);
                if (!limitArg.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LIMIT) || parseState.Count != nKeys + 3)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                if (!parseState.TryGetInt(nKeys + 2, out var limitVal))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (limitVal < 0)
                {
                    return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrCantBeNegative, "LIMIT")));
                }

                limit = limitVal;
            }

            var status = storageApi.SetIntersectLength(keys, limit, out var result);
            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(result, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
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
            var keys = new PinnedSpanByte[parseState.Count];

            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetUnion(keys, out var result);
            switch (status)
            {
                case GarnetStatus.OK:
                    // write the size of result
                    WriteSetLength(result.Count);

                    foreach (var item in result)
                    {
                        while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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
            var key = parseState.GetArgSliceByRef(0);

            var keys = new PinnedSpanByte[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetUnionStore(key, keys, out var output);
            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(output, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState, startIdx: 1) { SetOp = SetOperation.SREM };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetRemove(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
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
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState) { SetOp = SetOperation.SCARD };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetLength(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
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
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState) { SetOp = SetOperation.SMEMBERS };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetMembers(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    WriteEmptySet();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        private unsafe bool SetIsMember<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.SISMEMBER || cmd == RespCommand.SMISMEMBER);

            var isSingle = cmd == RespCommand.SISMEMBER;
            if (isSingle)
            {
                if (parseState.Count != 2)
                {
                    return AbortWithWrongNumberOfArguments("SISMEMBER");
                }
            }
            else
            {
                if (parseState.Count < 2)
                {
                    return AbortWithWrongNumberOfArguments("SMISMEMBER");
                }
            }

            // Get the key
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState, startIdx: 1) { SetOp = isSingle ? SetOperation.SISMEMBER : SetOperation.SMISMEMBER };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetIsMember(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    if (isSingle)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        var count = parseState.Count - 1; // Remove key
                        while (!RespWriteUtils.TryWriteArrayLength(count, ref dcurr, dend))
                            SendAndReset();

                        for (var i = 0; i < count; i++)
                        {
                            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                                SendAndReset();
                        }
                    }

                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes and returns one or more random members from the set at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetPop<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments("SPOP");
            }

            var key = parseState.GetArgSliceByRef(0);

            var countParameter = int.MinValue;
            if (parseState.Count == 2)
            {
                // Prepare response
                if (!parseState.TryGetInt(1, out countParameter) || countParameter < 0)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (countParameter == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
            }

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState, arg1: countParameter) { SetOp = SetOperation.SPOP };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetPop(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    WriteNull();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            var status = storageApi.SetMove(sourceKey, destinationKey, sourceMember, out var output);
            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(output, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SetRandomMember<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments("SRANDMEMBER");
            }

            var key = parseState.GetArgSliceByRef(0);

            var countParameter = int.MinValue;
            if (parseState.Count == 2)
            {
                // Prepare response
                if (!parseState.TryGetInt(1, out countParameter))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (countParameter == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
            }

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.Set, ref metaCommandInfo, ref parseState, arg1: countParameter, arg2: seed) { SetOp = SetOperation.SRANDMEMBER };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SetRandomMember(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    if (parseState.Count == 2)
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        break;
                    }

                    WriteNull();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool SetDiff<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("SDIFF");
            }

            var keys = new PinnedSpanByte[parseState.Count];
            for (var i = 0; i < parseState.Count; i++)
            {
                keys[i] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetDiff(keys, out var output);
            switch (status)
            {
                case GarnetStatus.OK:
                    if (output == null || output.Count == 0)
                    {
                        WriteEmptySet();
                    }
                    else
                    {
                        WriteSetLength(output.Count);

                        foreach (var item in output)
                        {
                            while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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
            var key = parseState.GetArgSliceByRef(0);

            var keys = new PinnedSpanByte[parseState.Count - 1];
            for (var i = 1; i < parseState.Count; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            var status = storageApi.SetDiffStore(key, keys, out var count);
            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(count, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }
    }
}