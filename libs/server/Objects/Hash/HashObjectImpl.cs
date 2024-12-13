// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
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
        private void HashGet(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var key = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

                if (TryGetValue(key, out var hashValue))
                {
                    while (!RespWriteUtils.WriteBulkString(hashValue, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                _output.result1++;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashMultipleGet(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.WriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    var key = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (TryGetValue(key, out var hashValue))
                    {
                        while (!RespWriteUtils.WriteBulkString(hashValue, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    _output.result1++;
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashGetAll(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var respProtocolVersion = input.arg1;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (respProtocolVersion < 3)
                {
                    while (!RespWriteUtils.WriteArrayLength(Count() * 2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteMapLength(Count(), ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                if (HasExpirableItems())
                {
                    foreach (var item in hash)
                    {
                        if (IsExpired(item.Key))
                        {
                            continue;
                        }

                        while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                else
                {
                    foreach (var item in hash)
                    {
                        while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
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

        private void HashRandomField(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // HRANDFIELD key [count [WITHVALUES]]
            var countParameter = input.arg1 >> 2;
            var withValues = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            var countDone = 0;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (includedCount)
                {
                    var count = Count();

                    if (count == 0) // This can happen because of expiration but RMW operation haven't applied yet
                    {
                        while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        _output.result1 = 0;
                        return;
                    }

                    if (countParameter > 0 && countParameter > count)
                        countParameter = count;

                    var absCount = Math.Abs(countParameter);
                    var indexes = RandomUtils.PickKRandomIndexes(count, absCount, seed, countParameter > 0);

                    // Write the size of the array reply
                    while (!RespWriteUtils.WriteArrayLength(withValues ? absCount * 2 : absCount, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var index in indexes)
                    {
                        var pair = ElementAt(index);
                        while (!RespWriteUtils.WriteBulkString(pair.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        if (withValues)
                        {
                            while (!RespWriteUtils.WriteBulkString(pair.Value, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        _output.result1 = 0;
                        return;
                    }

                    var index = RandomUtils.PickRandomIndex(count, seed);
                    var pair = ElementAt(index);
                    while (!RespWriteUtils.WriteBulkString(pair.Key, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    countDone = 1;
                }

                _output.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
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

        private void HashGetKeysOrValues(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = Count();
            var op = input.header.HashOp;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (HasExpirableItems())
                {
                    foreach (var item in hash)
                    {
                        if (IsExpired(item.Key))
                        {
                            continue;
                        }

                        if (HashOperation.HKEYS == op)
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        _output.result1++;
                    }
                }
                else
                {
                    foreach (var item in hash)
                    {
                        if (HashOperation.HKEYS == op)
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        _output.result1++;
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var op = input.header.HashOp;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            // This value is used to indicate partial command execution
            _output.result1 = int.MinValue;

            try
            {
                var key = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();
                var incrSlice = input.parseState.GetArgSliceByRef(1);

                var valueExists = TryGetValue(key, out var value);
                if (op == HashOperation.HINCRBY)
                {
                    if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out int incr))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    byte[] resultBytes;

                    if (valueExists)
                    {
                        if (!NumUtils.TryParse(value, out int result))
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_INTEGER, ref curr,
                                       end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                    ref end);
                            return;
                        }

                        result += incr;

                        var resultSpan = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatInt64Length];
                        var success = Utf8Formatter.TryFormat((long)result, resultSpan, out int bytesWritten,
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

                    while (!RespWriteUtils.WriteIntegerFromBytes(resultBytes, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                            ref end);
                }
                else
                {
                    if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out double incr))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                ref end);
                        return;
                    }

                    byte[] resultBytes;

                    if (valueExists)
                    {
                        if (!NumUtils.TryParse(value, out double result))
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_FLOAT, ref curr,
                                       end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                    ref end);
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

                    while (!RespWriteUtils.WriteBulkString(resultBytes, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                            ref end);
                }

                _output.result1 = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashExpire(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                DeleteExpiredItems();

                var expireOption = (ExpireOption)input.arg1;
                var expiration = input.parseState.GetLong(0);
                var numFields = input.parseState.Count - 1;
                while (!RespWriteUtils.WriteArrayLength(numFields, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var item in input.parseState.Parameters.Slice(1))
                {
                    var result = SetExpiration(item.ToArray(), expiration, expireOption);
                    while (!RespWriteUtils.WriteInteger(result, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.result1++;
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashTimeToLive(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                DeleteExpiredItems();

                var isMilliseconds = input.arg1 == 1;
                var isTimestamp = input.arg2 == 1;
                var numFields = input.parseState.Count;
                while (!RespWriteUtils.WriteArrayLength(numFields, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

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

                    while (!RespWriteUtils.WriteInteger(result, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.result1++;
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashPersist(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                DeleteExpiredItems();

                var numFields = input.parseState.Count;
                while (!RespWriteUtils.WriteArrayLength(numFields, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var item in input.parseState.Parameters)
                {
                    var result = Persist(item.ToArray());
                    while (!RespWriteUtils.WriteInteger(result, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.result1++;
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}