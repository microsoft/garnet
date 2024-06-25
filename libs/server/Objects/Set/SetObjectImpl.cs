// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
using System.Security.Cryptography;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    ///  Set - RESP specific operations
    /// </summary>
    public unsafe partial class SetObject : IGarnetObject
    {
        private void SetAdd(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            *_output = default;
            int count = _input->arg1;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;


            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref ptr, end))
                    return;

                if (set.Add(member))
                {
                    _output->result1++;
                    this.UpdateSize(member);
                }
            }
        }

        private void SetMembers(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.WriteArrayLength(set.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var item in set)
                {
                    while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
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

        private void SetIsMember(byte* input, int length, ref SpanByteAndMemory output)
        {
            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref input_currptr, input + length))
                    return;

                bool isMember = set.Contains(member);

                while (!RespWriteUtils.WriteInteger(isMember ? 1 : 0, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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

        private void SetRemove(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->arg1;
            *_output = default;
            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            while (count > 0)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var field, ref ptr, end))
                    break;

                if (set.Remove(field.ToArray()))
                {
                    _output->result1++;
                    this.UpdateSize(field, false);
                }
                count--;
            }
        }

        private void SetLength(byte* input, int length, byte* output)
        {
            // SCARD key
            var _output = (ObjectOutputHeader*)output;
            _output->result1 = set.Count;
        }

        private void SetPop(byte* input, int length, ref SpanByteAndMemory output)
        {
            // SPOP key[count]
            var _input = (ObjectInputHeader*)input;
            int count = _input->arg1;

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
                // key [count]
                if (count >= 1)
                {
                    // POP this number of random fields
                    var countParameter = count > set.Count ? set.Count : count;

                    // Write the size of the array reply
                    while (!RespWriteUtils.WriteArrayLength(countParameter, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    for (int i = 0; i < countParameter; i++)
                    {
                        // Generate a new index based on the elements left in the set
                        var index = RandomNumberGenerator.GetInt32(0, set.Count);
                        var item = set.ElementAt(index);
                        set.Remove(item);
                        this.UpdateSize(item, false);
                        while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone++;
                    }

                    countDone += count - countDone;
                }
                else if (count == int.MinValue) // no count parameter is present, we just pop and return a random item of the set
                {
                    // Write a bulk string value of a random field from the hash value stored at key.
                    if (set.Count > 0)
                    {
                        int index = RandomNumberGenerator.GetInt32(0, set.Count);
                        var item = set.ElementAt(index);
                        set.Remove(item);
                        this.UpdateSize(item, false);
                        while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        // If set empty return nil
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    countDone++;
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

        private void SetRandomMember(byte* input, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
            var count = _input->arg1;
            var seed = _input->arg2;

            var countDone = 0;
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                Span<int> indexes = default;

                if (count > 0)
                {
                    // Return an array of distinct elements
                    var countParameter = count > set.Count ? set.Count : count;

                    // The order of fields in the reply is not truly random
                    indexes = RandomUtils.PickKRandomIndexes(set.Count, countParameter, seed);

                    // Write the size of the array reply
                    while (!RespWriteUtils.WriteArrayLength(countParameter, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var index in indexes)
                    {
                        var element = set.ElementAt(index);
                        while (!RespWriteUtils.WriteBulkString(element, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone++;
                    }
                    countDone += count - countParameter;
                }
                else if (count == int.MinValue) // no count parameter is present
                {
                    // Return a single random element from the set
                    if (set.Count > 0)
                    {
                        var index = RandomUtils.PickRandomIndex(set.Count, seed);
                        var item = set.ElementAt(index);
                        while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        // If set is empty, return nil
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    countDone++;
                }
                else // count < 0
                {
                    // Return an array with potentially duplicate elements
                    var countParameter = Math.Abs(count);

                    indexes = RandomUtils.PickKRandomIndexes(set.Count, countParameter, seed, false);

                    if (set.Count > 0)
                    {
                        // Write the size of the array reply
                        while (!RespWriteUtils.WriteArrayLength(countParameter, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        foreach (var index in indexes)
                        {
                            var element = set.ElementAt(index);
                            while (!RespWriteUtils.WriteBulkString(element, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone++;
                        }
                    }
                    else
                    {
                        // If set is empty, return nil
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
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
    }
}