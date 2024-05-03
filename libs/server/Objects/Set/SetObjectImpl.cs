﻿// Copyright (c) Microsoft Corporation.
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
            int count = _input->count;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;


            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref ptr, end))
                    return;

                if (set.Add(member))
                {
                    _output->countDone++;
                    this.UpdateSize(member);
                }
                _output->opsDone++;
            }
            _output->bytesDone = (int)(ptr - startptr);
        }

        private void SetMembers(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;
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
                while (!RespWriteUtils.WriteArrayLength(set.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var item in set)
                {
                    if (countDone < prevDone) // skip processing previously done entries
                    {
                        countDone++;
                        continue;
                    }

                    while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    countDone++;
                }

                // Write bytes parsed from input and count done, into output footer                
                _output.opsDone = countDone;
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
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

                _output.opsDone = 1;
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = 1;
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

            int count = _input->count;
            *_output = default;
            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            int prevDone = _input->done;
            int countDone = 0;
            while (count > 0)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var field, ref ptr, end))
                    break;

                if (countDone < prevDone) // skip processing previously done entries
                {
                    countDone++;
                    count--;
                    continue;
                }

                if (set.Remove(field))
                {
                    countDone++;
                    this.UpdateSize(field, false);
                }

                count--;
            }

            // Write bytes parsed from input and count done, into output footer
            _output->bytesDone = (int)(ptr - startptr);
            _output->countDone = countDone;
            _output->opsDone = _input->count;
        }

        private void SetLength(byte* input, int length, byte* output)
        {
            // SCARD key
            var _output = (ObjectOutputHeader*)output;
            _output->countDone = set.Count;
            _output->opsDone = 1;
            _output->bytesDone = 0;
        }

        private void SetPop(byte* input, int length, ref SpanByteAndMemory output)
        {
            // SPOP key[count]
            var _input = (ObjectInputHeader*)input;
            int count = _input->count;
            int prevDone = _input->done;

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

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SetRandomMember(byte* input, int length, ref SpanByteAndMemory output)
        {
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
                int[] indexes = default;

                if (count > 0)
                {
                    // Return an array of distinct elements
                    var countParameter = count > set.Count ? set.Count : count;

                    // The order of fields in the reply is not truly random
                    indexes = Enumerable.Range(0, set.Count).OrderBy(x => Guid.NewGuid()).Take(countParameter).ToArray();

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
                        int index = RandomNumberGenerator.GetInt32(0, set.Count);
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
                    int countParameter = Math.Abs(count);

                    indexes = new int[countParameter];
                    for (int i = 0; i < countParameter; i++)
                    {
                        indexes[i] = RandomNumberGenerator.GetInt32(0, set.Count);
                    }

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
                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
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