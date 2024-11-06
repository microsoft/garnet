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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashSet<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (((command == RespCommand.HSET || command == RespCommand.HMSET)
                  && (parseState.Count == 1 || parseState.Count % 2 != 1)) ||
                (command == RespCommand.HSETNX && parseState.Count != 3))
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
                return true;

            var hop = command switch
            {
                RespCommand.HSET => HashOperation.HSET,
                RespCommand.HMSET => HashOperation.HMSET,
                RespCommand.HSETNX => HashOperation.HSETNX,
                _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
            };

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = hop,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = garnetApi.HashSet(keyBytes, ref input, out var output);

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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private bool HashGet<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HGET,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = garnetApi.HashGet(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private bool HashGetAll<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments(command.ToString());

            // Get the hash key
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HGETALL,
                },
                arg1 = respProtocolVersion,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = garnetApi.HashGetAll(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private bool HashGetMultiple<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count < 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

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
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HMGET,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = garnetApi.HashGetMultiple(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    // Write an empty array of count - 1 elements with null values.
                    while (!RespWriteUtils.WriteArrayWithNullElements(parseState.Count - 1, ref dcurr, dend))
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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private bool HashRandomField<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count < 1 || parseState.Count > 3)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            var paramCount = 1;
            var withValues = false;
            var includedCount = false;

            if (parseState.Count >= 2)
            {
                if (!parseState.TryGetInt(1, out paramCount))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                includedCount = true;

                // Read WITHVALUES
                if (parseState.Count == 3)
                {
                    var withValuesSlice = parseState.GetArgSliceByRef(2);

                    if (!withValuesSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHVALUES))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    withValues = true;
                }
            }

            var countWithMetadata = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (withValues ? 1 : 0);

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HRANDFIELD,
                },
                arg1 = countWithMetadata,
                arg2 = seed,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = GarnetStatus.NOTFOUND;

            // This prevents going to the backend if HRANDFIELD is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare GarnetObjectStore output
                outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                status = garnetApi.HashRandomField(keyBytes, ref input, ref outputFooter);
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("HLEN");
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
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HLEN,
                },
            };

            var status = garnetApi.HashLength(keyBytes, ref input, out var output);

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
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashStrLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("HSTRLEN");

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HSTRLEN,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = garnetApi.HashStrLength(keyBytes, ref input, out var output);

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
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashDelete<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count < 1)
                return AbortWithWrongNumberOfArguments("HDEL");

            // Get the key for Hash
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
                return true;

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HDEL,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = garnetApi.HashDelete(keyBytes, ref input, out var output);

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
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashExists<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("HEXISTS");

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HEXISTS,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = garnetApi.HashExists(keyBytes, ref input, out var output);

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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashKeys<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments(command.ToString());

            // Get the key for Hash
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, true))
                return true;

            var op = command switch
            {
                RespCommand.HKEYS => HashOperation.HKEYS,
                RespCommand.HVALS => HashOperation.HVALS,
                _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
            };

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = op,
                },
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = command == RespCommand.HKEYS
                ? garnetApi.HashKeys(keyBytes, ref input, ref outputFooter)
                : garnetApi.HashVals(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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
        /// <param name="command"></param>
        /// <param name="garnetApi"></param>
        /// <returns></returns>
        private unsafe bool HashIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand command, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            // Check if parameters number is right
            if (parseState.Count != 3)
                return AbortWithWrongNumberOfArguments(command == RespCommand.HINCRBY ? "HINCRBY" : "HINCRBYFLOAT");

            // Get the key for Hash
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
                return true;

            var op = command switch
            {
                RespCommand.HINCRBY => HashOperation.HINCRBY,
                RespCommand.HINCRBYFLOAT => HashOperation.HINCRBYFLOAT,
                _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
            };

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = op,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = garnetApi.HashIncrement(keyBytes, ref input, ref outputFooter);

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