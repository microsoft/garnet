// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;

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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashSet<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (((command == RespCommand.HSET || command == RespCommand.HMSET)
                  && (parseState.Count == 1 || parseState.Count % 2 != 1)) ||
                (command == RespCommand.HSETNX && parseState.Count != 3))
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            var hop =
                command switch
                {
                    RespCommand.HSET => HashOperation.HSET,
                    RespCommand.HMSET => HashOperation.HMSET,
                    RespCommand.HSETNX => HashOperation.HSETNX,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = hop };

            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            var status = storageApi.HashSet(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (command == RespCommand.HMSET)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGet<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = HashOperation.HGET };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashGet(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
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
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGetAll<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments(command.ToString());

            // Get the hash key
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HGETALL };
            var input = new ObjectInput(header, respProtocolVersion);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashGetAll(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
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
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashGetMultiple<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = HashOperation.HMGET };

            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashGetMultiple(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    // Write an empty array of count - 1 elements with null values.
                    while (!RespWriteUtils.TryWriteArrayLength(parseState.Count - 1, ref dcurr, dend))
                        SendAndReset();

                    for (var i = 0; i < parseState.Count - 1; i++)
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
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HashRandomField<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 3)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var key = parseState.GetArgSliceByRef(0);

            var paramCount = 1;
            var withValues = false;
            var includedCount = false;

            if (parseState.Count >= 2)
            {
                if (!parseState.TryGetInt(1, out paramCount))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                includedCount = true;

                // Read WITHVALUES
                if (parseState.Count == 3)
                {
                    var withValuesSlice = parseState.GetArgSliceByRef(2);

                    if (!withValuesSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHVALUES))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    withValues = true;
                }
            }

            var countWithMetadata = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (withValues ? 1 : 0);

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HRANDFIELD };
            var input = new ObjectInput(header, countWithMetadata, seed);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = GarnetStatus.NOTFOUND;

            // This prevents going to the backend if HRANDFIELD is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare output
                output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));
                status = storageApi.HashRandomField(key, ref input, ref output);
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    if (includedCount)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        WriteNull();
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
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("HLEN");
            }

            // Get the key 
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HLEN };
            var input = new ObjectInput(header);

            var status = storageApi.HashLength(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
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
        /// Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        private unsafe bool HashStrLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("HSTRLEN");
            }

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = HashOperation.HSTRLEN };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            var status = storageApi.HashStrLength(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
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
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashDelete<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("HDEL");
            }

            // Get the key for Hash
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = HashOperation.HDEL };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            var status = storageApi.HashDelete(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
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
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashExists<TGarnetApi>(ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("HEXISTS");
            }

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = HashOperation.HEXISTS };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            var status = storageApi.HashExists(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
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
        /// HashKeys: Returns all field names in the hash key.
        /// HashVals: Returns all values in the hash key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashKeys<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
          where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key for Hash
            var key = parseState.GetArgSliceByRef(0);

            var op =
                command switch
                {
                    RespCommand.HKEYS => HashOperation.HKEYS,
                    RespCommand.HVALS => HashOperation.HVALS,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = op };
            var input = new ObjectInput(header);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = command == RespCommand.HKEYS
                ? storageApi.HashKeys(key, ref input, ref output)
                : storageApi.HashVals(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
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
        /// HashIncrement: Increments the number stored at field in the hash stored at key by increment.
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashIncrement<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Check if parameters number is right
            if (parseState.Count != 3)
            {
                // Send error to output
                return AbortWithWrongNumberOfArguments(command == RespCommand.HINCRBY ? "HINCRBY" : "HINCRBYFLOAT");
            }

            // Get the key for Hash
            var key = parseState.GetArgSliceByRef(0);

            var op =
                command switch
                {
                    RespCommand.HINCRBY => HashOperation.HINCRBY,
                    RespCommand.HINCRBYFLOAT => HashOperation.HINCRBYFLOAT,
                    _ => throw new Exception($"Unexpected {nameof(HashOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash, metaCommand) { HashOp = op };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, ref metaCommandParseState);

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashIncrement(key, ref input, ref output);

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
        /// Sets an expiration time for a field in the hash stored at key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool HashExpire<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 4)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            if (!parseState.TryGetLong(1, out var expiration))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (expiration < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_EXPIRE_TIME);
            }

            var currIdx = 2;
            if (parseState.TryGetExpireOption(currIdx, out var expireOption))
            {
                currIdx++; // If expire option is present, move to next argument else continue with the current argument
            }

            var fieldOption = parseState.GetArgSliceByRef(currIdx++);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FIELDS))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMandatoryMissing, "FIELDS")));
            }

            if (!parseState.TryGetInt(currIdx++, out var numFields))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numFields")));
            }

            if (parseState.Count != currIdx + numFields)
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMustMatchNoOfArgs, "numFields")));
            }

            // Convert to expiration time in ticks
            var expirationTimeInTicks = command switch
            {
                RespCommand.HEXPIRE => DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks,
                RespCommand.HPEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks,
                RespCommand.HEXPIREAT => ConvertUtils.UnixTimestampInSecondsToTicks(expiration),
                _ => ConvertUtils.UnixTimestampInMillisecondsToTicks(expiration)
            };

            // Encode expiration time and expiration option and pass them into the input object
            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HEXPIRE };
            var input = new ObjectInput(header, ref parseState, startIdx: currIdx, ref metaCommandParseState, arg1: expirationWithOption.WordHead, arg2: expirationWithOption.WordTail);

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashExpire(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numFields, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numFields; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the time to live (TTL) for the specified fields in the hash stored at the given key.
        /// </summary>
        /// <typeparam name="TGarnetApi">The type of the storage API.</typeparam>
        /// <param name="command">The RESP command indicating the type of TTL operation.</param>
        /// <param name="storageApi">The storage API instance to interact with the underlying storage.</param>
        /// <returns>True if the operation was successful; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown when the object store is disabled.</exception>
        private unsafe bool HashTimeToLive<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            var fieldOption = parseState.GetArgSliceByRef(1);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FIELDS))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMandatoryMissing, "FIELDS")));
            }

            if (!parseState.TryGetInt(2, out var numFields))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numFields")));
            }

            if (parseState.Count != 3 + numFields)
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMustMatchNoOfArgs, "numFields")));
            }

            var isMilliseconds = false;
            var isTimestamp = false;
            switch (command)
            {
                case RespCommand.HPTTL:
                    isMilliseconds = true;
                    isTimestamp = false;
                    break;
                case RespCommand.HEXPIRETIME:
                    isMilliseconds = false;
                    isTimestamp = true;
                    break;
                case RespCommand.HPEXPIRETIME:
                    isMilliseconds = true;
                    isTimestamp = true;
                    break;
                default: // RespCommand.HTTL
                    break;
            }

            var fieldsParseState = parseState.Slice(3, numFields);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HTTL };
            var input = new ObjectInput(header, ref fieldsParseState);

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashTimeToLive(key, isMilliseconds, isTimestamp, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numFields, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numFields; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        private unsafe bool HashPersist<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.HPERSIST));
            }

            var key = parseState.GetArgSliceByRef(0);

            var fieldOption = parseState.GetArgSliceByRef(1);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FIELDS))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMandatoryMissing, "FIELDS")));
            }

            if (!parseState.TryGetInt(2, out var numFields))
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numFields")));
            }

            if (parseState.Count != 3 + numFields)
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrMustMatchNoOfArgs, "numFields")));
            }

            var fieldsParseState = parseState.Slice(3, numFields);

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HPERSIST };
            var input = new ObjectInput(header, ref fieldsParseState);

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.HashPersist(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numFields, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numFields; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }
    }
}