// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
using System.Security.Cryptography;
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
        private void HashSet(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            SetOrSetNX(_input->header.HashOp, input, length, output);
        }

        private void HashSetWhenNotExists(byte* input, int length, byte* output)
        {
            SetOrSetNX(HashOperation.HSETNX, input, length, output);
        }

        private void HashGet(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int count = _input->count; // for multiples fields
            int prevDone = _input->done; // how many were previously done

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            int countDone = 0;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (count > 1) // Multiple keys
                {
                    while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                }
                else if (count == 0 && _input->header.HashOp == HashOperation.HGETALL) // Get all keys
                {
                    while (!RespWriteUtils.WriteArrayLength(hash.Count * 2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var item in hash)
                    {
                        while (!RespWriteUtils.WriteBulkString(item.Key, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        while (!RespWriteUtils.WriteBulkString(item.Value, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                while (count > 0)
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref input_currptr, input + length))
                        break;

                    if (countDone < prevDone) // Skip processing previously done entries
                    {
                        countDone++;
                        count--;
                        continue;
                    }

                    if (hash.TryGetValue(key, out var _value))
                    {

                        while (!RespWriteUtils.WriteBulkString(_value, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }

                    countDone++;
                    count--;
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
                _output.opsDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void HashDelete(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;
            int count = _input->count; // count of fields to delete
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            var end = input + length;

            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                if (hash.Remove(key, out var _value))
                {
                    _output->opsDone++;
                    this.UpdateSize(key, _value, false);
                }

                _output->bytesDone = (int)(ptr - startptr);
            }
        }

        private void HashLength(byte* output)
        {
            ((ObjectOutputHeader*)output)->countDone = hash.Count;
        }

        private void HashExists(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var field, ref ptr, end))
                return;

            _output->countDone = 1;
            _output->opsDone = hash.ContainsKey(field) ? 1 : 0;
            _output->bytesDone = (int)(ptr - startptr);
        }

        private void HashKeys(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            GetHashKeysOrValues(_input->header.HashOp, input, ref output);
            return;
        }

        private void HashVals(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            GetHashKeysOrValues(_input->header.HashOp, input, ref output);
            return;
        }

        private void HashIncrement(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            IncrementIntegerOrFloat(_input->header.HashOp, input, length, ref output);
            return;
        }

        private void HashIncrementByFloat(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            IncrementIntegerOrFloat(_input->header.HashOp, input, length, ref output);
            return;
        }

        private void HashRandomField(byte* input, int length, ref SpanByteAndMemory output)
        {
            // HRANDFIELD key [count [WITHVALUES]]
            var _input = (ObjectInputHeader*)input;
            int count = _input->count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;
            int countDone = 0;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var withValues = false;
                int[] indexes = default;

                // [count [WITHVALUES]]
                if (count > 2)
                {
                    // Get the value for the count parameter
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countParameterByteArray, ref input_currptr, input + length))
                        return;
                    if (count == 4)
                    {
                        // Advance to read the withvalues flag
                        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var withValuesByteArray, ref input_currptr, input + length))
                            return;

                        if (String.Compare(Encoding.ASCII.GetString(withValuesByteArray), "WITHVALUES", StringComparison.InvariantCultureIgnoreCase) == 0)
                        {
                            withValues = true;
                        }
                    }

                    // All tokens have been read
                    countDone = count;

                    // Prepare response
                    if (!Int32.TryParse(Encoding.ASCII.GetString(countParameterByteArray), out var countParameter))
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERROR_VALUE_IS_NOT_INTEGER, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        if (countParameter == 0)
                        {
                            while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else if (countParameter > 0)
                        {
                            // No repeated fields are returned.
                            // If count is bigger than the number of fields in the hash, the command will only return the whole hash without additional fields.
                            // The order of fields in the reply is not truly random, so it is up to the client to shuffle them if needed.
                            countParameter = countParameter > hash.Count ? hash.Count : countParameter;
                            indexes = Enumerable.Range(0, hash.Count).OrderBy(x => Guid.NewGuid()).Take(countParameter).ToArray();
                        }
                        else if (countParameter < 0)
                        {
                            // Repeating fields are possible.
                            // Exactly count fields, or an empty array if the hash is empty(non - existing key), are always returned.
                            // The order of fields in the reply is truly random.
                            // The number of returned fields is the absolute value of the specified count.
                            countParameter = Math.Abs(countParameter);
                            indexes = new int[countParameter];
                            for (int i = 0; i < countParameter; i++)
                                indexes[i] = RandomNumberGenerator.GetInt32(0, hash.Count);
                        }

                        // Write the size of the array reply
                        while (!RespWriteUtils.WriteArrayLength(withValues ? countParameter * 2 : countParameter, ref curr, end))
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
                        }
                    }
                }
                else if (count == 2) // No count parameter or withvalues is present, we just return a random field
                {
                    // Write a bulk string value of a random field from the hash value stored at key.
                    int index = RandomNumberGenerator.GetInt32(0, hash.Count);
                    var pair = hash.ElementAt(index);
                    while (!RespWriteUtils.WriteBulkString(pair.Key, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    countDone = count;
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
                _output.opsDone = countDone;
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

        private void SetOrSetNX(HashOperation op, byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref ptr, end))
                    return;

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var value, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                byte[] _value = default;
                if (!hash.TryGetValue(key, out _value))
                {
                    hash.Add(key, value);
                    this.UpdateSize(key, value);
                    _output->opsDone++;
                }
                else if (_input->header.HashOp == HashOperation.HSET && _value != default && !_value.SequenceEqual(value))
                {
                    hash[key] = value;
                    this.Size += Utility.RoundUp(value.Length, IntPtr.Size) - Utility.RoundUp(_value.Length, IntPtr.Size); // Skip overhead as existing item is getting replaced.
                }
                _output->countDone++;
                _output->bytesDone = (int)(ptr - startptr);
            }
        }

        private void IncrementIntegerOrFloat(HashOperation op, byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            int count = _input->count;
            int prevDone = _input->done; // how many were previously done

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            int countDone = 0;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            // This value is used to indicate partial command execution
            _output.opsDone = Int32.MinValue;

            try
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var key, ref input_currptr, input + length))
                    return;

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var incr, ref input_currptr, input + length))
                    return;

                if (hash.TryGetValue(key, out var _value))
                {
                    if (Single.TryParse(Encoding.ASCII.GetString(_value), out var result))
                    {
                        if (Single.TryParse(Encoding.ASCII.GetString(incr), out var resultIncr))
                        {
                            result += resultIncr;

                            if (op == HashOperation.HINCRBY)
                            {
                                int numDigits = NumUtils.NumDigitsInLong((long)result);
                                byte sign = (byte)(result < 0 ? 1 : 0);
                                var resultBytes = new byte[sign + numDigits];

                                fixed (byte* resultBytesPtr = &resultBytes[0])
                                {
                                    byte* resultRef = resultBytesPtr;
                                    NumUtils.LongToBytes((long)result, numDigits, ref resultRef);

                                    hash[key] = resultBytes;
                                    this.Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) - Utility.RoundUp(_value.Length, IntPtr.Size);

                                    while (!RespWriteUtils.WriteIntegerFromBytes(resultBytesPtr, sign + numDigits, ref curr, end))
                                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                }
                            }
                            else
                            {
                                var resultBytes = Encoding.ASCII.GetBytes(result.ToString());
                                hash[key] = resultBytes;
                                this.Size += Utility.RoundUp(resultBytes.Length, IntPtr.Size) - Utility.RoundUp(_value.Length, IntPtr.Size);

                                while (!RespWriteUtils.WriteBulkString(resultBytes, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                    }
                    else
                    {
                        ReadOnlySpan<byte> errorMessage = "-ERR field value is not a number\r\n"u8;
                        while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                else
                {
                    if (!Single.TryParse(Encoding.ASCII.GetString(incr), out var resultIncr))
                    {
                        ReadOnlySpan<byte> errorMessage = "-ERR field value is not a number\r\n"u8;
                        while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        hash.Add(key, incr);
                        this.UpdateSize(key, incr);
                        if (op == HashOperation.HINCRBY)
                        {
                            while (!RespWriteUtils.WriteInteger((long)resultIncr, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {

                            while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(resultIncr.ToString()), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                    }
                }

                countDone++;

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
                _output.opsDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void GetHashKeysOrValues(HashOperation op, byte* input, ref SpanByteAndMemory output)
        {
            var count = hash.Count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;
            var countDone = 0;

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
                    countDone++;
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = hash.Count;
                _output.opsDone = countDone;
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