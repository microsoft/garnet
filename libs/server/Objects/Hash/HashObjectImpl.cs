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

            var currTokenIdx = input.parseStateStartIdx;

            ObjectOutputHeader _output = default;
            try
            {
                var key = input.parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

                if (hash.TryGetValue(key, out var hashValue))
                {
                    while (!RespWriteUtils.WriteBulkString(hashValue.Value, ref curr, end))
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

            var count = input.parseState.Count;
            var currTokenIdx = input.parseStateStartIdx;

            ObjectOutputHeader _output = default;
            try
            {
                var expectedTokenCount = count - input.parseStateStartIdx;
                while (!RespWriteUtils.WriteArrayLength(expectedTokenCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (currTokenIdx < count)
                {
                    var key = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();

                    if (hash.TryGetValue(key, out var hashValue))
                    {
                        while (!RespWriteUtils.WriteBulkString(hashValue.Value, ref curr, end))
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
                    while (!RespWriteUtils.WriteArrayLength(hash.Count * 2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteMapLength(hash.Count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                foreach (var item in hash)
                {
                    while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    while (!RespWriteUtils.WriteBulkString(item.Value.Value, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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

            for (var currTokenIdx = input.parseStateStartIdx; currTokenIdx < input.parseState.Count; currTokenIdx++)
            {
                var key = input.parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

                if (hash.Remove(key, out var hashValue))
                {
                    _output->result1++;
                    this.UpdateSize(key, hashValue.Value, false);
                }
            }
        }

        private void HashLength(byte* output)
        {
            ((ObjectOutputHeader*)output)->result1 = hash.Count;
        }

        private void HashStrLength(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var key = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).SpanByte.ToByteArray();
            _output->result1 = hash.TryGetValue(key, out var hashValue) ? hashValue.Value.Length : 0;
        }

        private void HashExists(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var field = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).SpanByte.ToByteArray();
            _output->result1 = hash.ContainsKey(field) ? 1 : 0;
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
                    if (countParameter > 0 && countParameter > hash.Count)
                        countParameter = hash.Count;

                    var absCount = Math.Abs(countParameter);
                    var indexes = RandomUtils.PickKRandomIndexes(hash.Count, absCount, seed, countParameter > 0);

                    // Write the size of the array reply
                    while (!RespWriteUtils.WriteArrayLength(withValues ? absCount * 2 : absCount, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var index in indexes)
                    {
                        var pair = hash.ElementAt(index);
                        while (!RespWriteUtils.WriteBulkString(pair.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        if (withValues)
                        {
                            while (!RespWriteUtils.WriteBulkString(pair.Value.Value, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }

                        countDone++;
                    }
                }
                else // No count parameter is present, we just return a random field
                {
                    // Write a bulk string value of a random field from the hash value stored at key.
                    var index = RandomUtils.PickRandomIndex(hash.Count, seed);
                    var pair = hash.ElementAt(index);
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
            for (var currIdx = input.parseStateStartIdx; currIdx < input.parseState.Count; currIdx += 2)
            {
                var key = input.parseState.GetArgSliceByRef(currIdx).SpanByte.ToByteArray();
                var value = input.parseState.GetArgSliceByRef(currIdx + 1).SpanByte.ToByteArray();

                if (!hash.TryGetValue(key, out var hashValue))
                {
                    hash.Add(key, new HashValue(value));
                    this.UpdateSize(key, value);
                    _output->result1++;
                }
                else if ((hop == HashOperation.HSET || hop == HashOperation.HMSET) && hashValue != default &&
                         !hashValue.Value.AsSpan().SequenceEqual(value))
                {
                    hash[key] = new HashValue(value);
                    // Skip overhead as existing item is getting replaced.
                    this.Size += Utility.RoundUp(value.Length, IntPtr.Size) -
                                 Utility.RoundUp(hashValue.Value.Length, IntPtr.Size);
                }
            }
        }

        private void HashGetKeysOrValues(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = hash.Count;
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

                foreach (var item in hash)
                {
                    if (HashOperation.HKEYS == op)
                    {
                        while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteBulkString(item.Value.Value, ref curr, end))
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

        private void HashIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var op = input.header.HashOp;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            var currTokenIdx = input.parseStateStartIdx;

            // This value is used to indicate partial command execution
            _output.result1 = int.MinValue;

            try
            {
                var key = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                var incrSlice = input.parseState.GetArgSliceByRef(currTokenIdx);

                var valueExists = hash.TryGetValue(key, out var value);
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
                        if (!NumUtils.TryParse(value.Value, out int result))
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
                        hash[key] = new HashValue(resultBytes);
                        Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) -
                                Utility.RoundUp(value.Value.Length, IntPtr.Size);
                    }
                    else
                    {
                        resultBytes = incrSlice.SpanByte.ToByteArray();
                        hash.Add(key, new HashValue(resultBytes));
                        UpdateSize(key, resultBytes);
                    }

                    while (!RespWriteUtils.WriteIntegerFromBytes(resultBytes, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                            ref end);
                }
                else
                {
                    if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out float incr))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                ref end);
                        return;
                    }

                    byte[] resultBytes;

                    if (valueExists)
                    {
                        if (!NumUtils.TryParse(value.Value, out float result))
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_FLOAT, ref curr,
                                       end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                                    ref end);
                            return;
                        }

                        result += incr;

                        resultBytes = Encoding.ASCII.GetBytes(result.ToString(CultureInfo.InvariantCulture));
                        hash[key] = new HashValue(resultBytes);
                        Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) -
                                Utility.RoundUp(value.Value.Length, IntPtr.Size);
                    }
                    else
                    {
                        resultBytes = incrSlice.SpanByte.ToByteArray();
                        hash.Add(key, new HashValue(resultBytes));
                        UpdateSize(key, resultBytes);
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
            var hop = input.header.HashOp;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            _output.result1 = int.MinValue;
            try
            {
                var parseState = input.parseState;
                var currIdx = input.parseStateStartIdx;
                var expireOption = ExpireOption.None;

                if (!parseState.TryGetLong(currIdx, out var expirationValue))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }
                if (parseState.TryGetEnum(++currIdx, true, out expireOption) && expireOption.IsValid(ref parseState.GetArgSliceByRef(currIdx)))
                {
                    currIdx++;
                }
                else
                {
                    expireOption = ExpireOption.None;
                }
                var fieldsKeyword = parseState.GetString(currIdx);
                if (fieldsKeyword != "FIELDS")
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MISSING_ARGUMENT_FIELDS, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }
                currIdx++;
                if (!parseState.TryGetInt(currIdx, out var fieldCount))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                var expiryTime = hop switch {
                    HashOperation.HEXPIRE => DateTimeOffset.UtcNow.AddSeconds(expirationValue),
                    HashOperation.HEXPIREAT => DateTimeOffset.FromUnixTimeSeconds(expirationValue),
                    HashOperation.HPEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expirationValue),
                    HashOperation.HPEXPIREAT => DateTimeOffset.FromUnixTimeMilliseconds(expirationValue),
                    _ => DateTimeOffset.UtcNow.AddSeconds(expirationValue)
                };

                currIdx++;
                if (fieldCount != parseState.Count - currIdx)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MISMATCH_NUMFIELDS, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }
                while (!RespWriteUtils.WriteArrayLength(fieldCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++, currIdx++)
                {
                    var key = parseState.GetArgSliceByRef(currIdx).SpanByte.ToByteArray();

                    var result = 1; // Assume success
                    if (!hash.TryGetValue(key, out var hashValue) /* || hashValue.IsExpired() */)
                    {
                        result = -2;
                    }
                    else
                    {
                        switch (expireOption)
                        {
                            case ExpireOption.NX:   // Only set if not already set
                                if (hashValue.Expiration > 0)
                                    result = 0;
                                break;
                            case ExpireOption.XX:   // Only set if already set
                                if (hashValue.Expiration <= 0)
                                    result = 0;
                                break;
                            case ExpireOption.GT:   // Only set if greater
                                // Unset TTL is interpreted as infinite
                                if (hashValue.Expiration <= 0 || hashValue.Expiration >= expiryTime.Ticks)
                                    result = 0;
                                break;
                            case ExpireOption.LT:   // Only set if smaller
                                // Unset TTL is interpreted as infinite
                                if (hashValue.Expiration > 0 && hashValue.Expiration <= expiryTime.Ticks)
                                    result = 0;
                                break;
                        }
                        if (result != 0)    // Option did not reject the operation
                        {
                            if (DateTimeOffset.UtcNow >= expiryTime)
                            {
                                // If provided expiration time is before or equal to now, delete key
                                if (hash.Remove(key))
                                {
                                    this.UpdateSize(key, hashValue.Value, false);
                                }
                                result = 2;
                            }
                            else
                            {
                                hashValue.Expiration = expiryTime.Ticks;    // Update the expiration time
                                hash[key] = hashValue;
                            }

                        }
                    }
                    while (!RespWriteUtils.WriteInteger(result, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                            ref end);
                    _output.result1 = (_output.result1 < 0) ? 1 : _output.result1 + 1;
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