﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{

    /// <summary>
    ///  Hash - RESP specific operations
    /// </summary>
    public unsafe partial class HashObject : IGarnetObject
    {
        private void HashGet(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            var key = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            if (TryGetValue(key, out var hashValue))
            {
                output.WriteBulkString(hashValue);
            }
            else
            {
                output.WriteNull();
            }

            output.IncResult1();
        }

        private void HashMultipleGet(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            output.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                if (TryGetValue(key, out var hashValue))
                {
                    output.WriteBulkString(hashValue);
                }
                else
                {
                    output.WriteNull();
                }

                output.IncResult1();
            }
        }

        private void HashGetAll(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            if (!input.IsResp3)
            {
                output.WriteArrayLength(Count() * 2);
            }
            else
            {
                output.WriteMapLength(Count());
            }

            var isExpirable = HasExpirableItems();

            foreach (var item in hash)
            {
                if (isExpirable && IsExpired(item.Key))
                {
                    continue;
                }

                output.WriteBulkString(item.Key);
                output.WriteBulkString(item.Value);
            }
        }

        private void HashDelete(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                if (Remove(key, out var hashValue))
                {
                    _output->result1++;
                }
            }
        }

        private void HashLength(byte* output)
        {
            ((ObjectOutputHeader*)output)->result1 = Count();
        }

        private void HashStrLength(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var key = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();
            _output->result1 = TryGetValue(key, out var hashValue) ? hashValue.Length : 0;
        }

        private void HashExists(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var field = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();
            _output->result1 = ContainsKey(field) ? 1 : 0;
        }

        private void HashRandomField(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            // HRANDFIELD key [count [WITHVALUES]]
            var countParameter = input.arg1 >> 2;
            var withValues = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            var countDone = 0;

            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            if (includedCount)
            {
                var count = Count();

                if (count == 0) // This can happen because of expiration but RMW operation haven't applied yet
                {
                    output.WriteEmptyArray();
                    output.SetResult1(0);
                    return;
                }

                if (countParameter > 0 && countParameter > count)
                    countParameter = count;

                var absCount = Math.Abs(countParameter);
                var indexes = RandomUtils.PickKRandomIndexes(count, absCount, seed, countParameter > 0);

                // Write the size of the array reply
                output.WriteArrayLength(withValues ? absCount * 2 : absCount);

                foreach (var index in indexes)
                {
                    var pair = ElementAt(index);
                    output.WriteBulkString(pair.Key);

                    if (withValues)
                    {
                        output.WriteBulkString(pair.Value);
                    }

                    countDone++;
                }
            }
            else // No count parameter is present, we just return a random field
            {
                // Write a bulk string value of a random field from the hash value stored at key.
                var count = Count();
                if (count == 0) // This can happen because of expiration but RMW operation haven't applied yet
                {
                    output.WriteNull();
                    output.SetResult1(0);
                    return;
                }

                var index = RandomUtils.PickRandomIndex(count, seed);
                var pair = ElementAt(index);
                output.WriteBulkString(pair.Key);
                countDone = 1;
            }

            output.SetResult1(countDone);
        }

        private void HashSet(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var hop = input.header.HashOp;
            for (var i = 0; i < input.parseState.Count; i += 2)
            {
                var key = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();
                var value = input.parseState.GetArgSliceByRef(i + 1).SpanByte.ToByteArray();

                if (!TryGetValue(key, out var hashValue))
                {
                    Add(key, value);
                    _output->result1++;
                }
                else if ((hop == HashOperation.HSET || hop == HashOperation.HMSET) && hashValue != default)
                {
                    Set(key, value);
                }
            }
        }

        private void HashCollect(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            DeleteExpiredItems();

            _output->result1 = 1;
        }

        private void HashGetKeysOrValues(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            var count = Count();
            var op = input.header.HashOp;

            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            output.WriteArrayLength(count);

            var isExpirable = HasExpirableItems();

            foreach (var item in hash)
            {
                if (isExpirable && IsExpired(item.Key))
                {
                    continue;
                }

                if (HashOperation.HKEYS == op)
                {
                    output.WriteBulkString(item.Key);
                }
                else
                {
                    output.WriteBulkString(item.Value);
                }

                output.IncResult1();
            }
        }

        private void HashIncrement(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            var op = input.header.HashOp;

            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            // This value is used to indicate partial command execution
            output.SetResult1(int.MinValue);

            var key = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();
            var incrSlice = input.parseState.GetArgSliceByRef(1);

            var valueExists = TryGetValue(key, out var value);
            if (op == HashOperation.HINCRBY)
            {
                if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out long incr))
                {
                    output.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                    return;
                }

                byte[] resultBytes;

                if (valueExists)
                {
                    if (!NumUtils.TryParse(value, out long result))
                    {
                        output.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
                        return;
                    }

                    result += incr;

                    var resultSpan = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatInt64Length];
                    var success = Utf8Formatter.TryFormat(result, resultSpan, out int bytesWritten,
                        format: default);
                    Debug.Assert(success);

                    resultSpan = resultSpan.Slice(0, bytesWritten);

                    resultBytes = resultSpan.ToArray();
                    SetWithoutPersist(key, resultBytes);
                }
                else
                {
                    resultBytes = incrSlice.SpanByte.ToByteArray();
                    Add(key, resultBytes);
                }

                output.WriteIntegerFromBytes(resultBytes);
            }
            else
            {
                if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out double incr))
                {
                    output.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
                    return;
                }

                byte[] resultBytes;

                if (valueExists)
                {
                    if (!NumUtils.TryParse(value, out double result))
                    {
                        output.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_FLOAT);
                        return;
                    }

                    result += incr;

                    resultBytes = Encoding.ASCII.GetBytes(result.ToString(CultureInfo.InvariantCulture));
                    SetWithoutPersist(key, resultBytes);
                }
                else
                {
                    resultBytes = incrSlice.SpanByte.ToByteArray();
                    Add(key, resultBytes);
                }

                output.WriteBulkString(resultBytes);
            }

            output.SetResult1(1);
        }

        private void HashExpire(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            DeleteExpiredItems();

            var expireOption = (ExpireOption)input.arg1;
            var expiration = input.parseState.GetLong(0);
            var numFields = input.parseState.Count - 1;
            output.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters.Slice(1))
            {
                var result = SetExpiration(item.ToArray(), expiration, expireOption);
                output.WriteInt32(result);
                output.IncResult1();
            }
        }

        private void HashTimeToLive(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            DeleteExpiredItems();

            var isMilliseconds = input.arg1 == 1;
            var isTimestamp = input.arg2 == 1;
            var numFields = input.parseState.Count;
            output.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
                var result = GetExpiration(item.ToArray());

                if (result >= 0)
                {
                    if (isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInMillisecondsFromTicks(result);
                    }
                    else if (isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInSecondsFromTicks(result);
                    }
                    else if (!isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.MillisecondsFromDiffUtcNowTicks(result);
                    }
                    else if (!isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.SecondsFromDiffUtcNowTicks(result);
                    }
                }

                output.WriteInt64(result);
                output.IncResult1();
            }
        }

        private void HashPersist(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input, ref outputFooter);

            DeleteExpiredItems();

            var numFields = input.parseState.Count;
            output.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
                var result = Persist(item.ToArray());
                output.WriteInt32(result);
                output.IncResult1();
            }
        }
    }
}