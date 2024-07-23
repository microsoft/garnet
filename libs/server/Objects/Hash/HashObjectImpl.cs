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
        private void HashSet(ref ObjectInput input, byte* output)
        {
            SetOrSetNX(ref input, output);
        }

        private void HashSetWhenNotExists(ref ObjectInput input, byte* output)
        {
            SetOrSetNX(ref input, output);
        }

        private void HashGet(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var key, ref input_currptr, input_endptr))
                    return;

                if (hash.TryGetValue(key.ToArray(), out var hashValue))
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
            var count = input.arg1; // for multiples fields

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

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

                while (count > 0)
                {
                    if (!RespReadUtils.TrySliceWithLengthHeader(out var key, ref input_currptr, input_endptr))
                        break;

                    if (hash.TryGetValue(key.ToArray(), out var hashValue))
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
                    count--;
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
                    while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
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

            var count = input.arg1;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            for (var c = 0; c < count; c++)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var key, ref input_currptr, input_endptr))
                    return;

                if (hash.Remove(key.ToArray(), out var hashValue))
                {
                    _output->result1++;
                    this.UpdateSize(key, hashValue, false);
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

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            *_output = default;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref input_currptr, input_endptr))
                return;
            _output->result1 = hash.TryGetValue(key, out var hashValue) ? hashValue.Length : 0;
        }

        private void HashExists(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var field, ref input_currptr, input_endptr))
                return;

            _output->result1 = hash.ContainsKey(field) ? 1 : 0;
        }

        private void HashKeys(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            GetHashKeysOrValues(ref input, ref output);
        }

        private void HashVals(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            GetHashKeysOrValues(ref input, ref output);
        }

        private void HashIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            IncrementIntegerOrFloat(ref input, ref output);
        }

        private void HashIncrementByFloat(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            IncrementIntegerOrFloat(ref input, ref output);
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
                            while (!RespWriteUtils.WriteBulkString(pair.Value, ref curr, end))
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

        #region CommonMethods

        private void SetOrSetNX(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var count = input.arg1;
            var hop = input.header.HashOp;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            for (var c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref input_currptr, input_endptr))
                    return;

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var value, ref input_currptr, input_endptr))
                    return;

                if (!hash.TryGetValue(key, out var hashValue))
                {
                    hash.Add(key, value);
                    this.UpdateSize(key, value);
                    _output->result1++;
                }
                else if ((hop == HashOperation.HSET || hop == HashOperation.HMSET) && hashValue != default &&
                         !hashValue.AsSpan().SequenceEqual(value))
                {
                    hash[key] = value;
                    // Skip overhead as existing item is getting replaced.
                    this.Size += Utility.RoundUp(value.Length, IntPtr.Size) -
                                 Utility.RoundUp(hashValue.Length, IntPtr.Size);
                }
            }
        }

        private void IncrementIntegerOrFloat(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.arg1;
            var op = input.header.HashOp;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

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
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref input_currptr, input_endptr) ||
                    !RespReadUtils.TrySliceWithLengthHeader(out var incr, ref input_currptr, input_endptr))
                    return;

                if (hash.TryGetValue(key, out var value))
                {
                    if (NumUtils.TryParse(value, out float result) &&
                        NumUtils.TryParse(incr, out float resultIncr))
                    {
                        result += resultIncr;

                        if (op == HashOperation.HINCRBY)
                        {
                            Span<byte> resultBytes = stackalloc byte[NumUtils.MaximumFormatInt64Length];
                            bool success = Utf8Formatter.TryFormat((long)result, resultBytes, out int bytesWritten, format: default);
                            Debug.Assert(success);

                            resultBytes = resultBytes.Slice(0, bytesWritten);

                            hash[key] = resultBytes.ToArray();
                            Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) - Utility.RoundUp(value.Length, IntPtr.Size);

                            while (!RespWriteUtils.WriteIntegerFromBytes(resultBytes, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            var resultBytes = Encoding.ASCII.GetBytes(result.ToString(CultureInfo.InvariantCulture));
                            hash[key] = resultBytes;
                            Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) - Utility.RoundUp(value.Length, IntPtr.Size);

                            while (!RespWriteUtils.WriteBulkString(resultBytes, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError("ERR field value is not a number"u8, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                else
                {
                    if (!NumUtils.TryParse(incr, out float resultIncr))
                    {
                        while (!RespWriteUtils.WriteError("ERR field value is not a number"u8, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        hash.Add(key, incr.ToArray());
                        UpdateSize(key, incr);
                        if (op == HashOperation.HINCRBY)
                        {
                            while (!RespWriteUtils.WriteInteger((long)resultIncr, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            while (!RespWriteUtils.WriteAsciiBulkString(resultIncr.ToString(CultureInfo.InvariantCulture), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                    }
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

        private void GetHashKeysOrValues(ref ObjectInput input, ref SpanByteAndMemory output)
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
                        while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
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

        #endregion

    }
}