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
        private void SetAdd(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Add(member))
#else
                if (Set.Add(member.ToArray()))
#endif
                {
                    _output->result1++;
                    this.UpdateSize(member);
                }
            }
        }

        private void SetMembers(ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.TryWriteArrayLength(Set.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var item in Set)
                {
                    while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.result1++;
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SetIsMember(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var member = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
#if NET9_0_OR_GREATER
                var isMember = setLookup.Contains(member);
#else
                var isMember = Set.Contains(member.ToArray());
#endif
                while (!RespWriteUtils.TryWriteInt32(isMember ? 1 : 0, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                _output.result1 = 1;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SetMultiIsMember(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                while (!RespWriteUtils.TryWriteArrayLength(input.parseState.Count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < input.parseState.Count; i++)
                {
                    var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
#if NET9_0_OR_GREATER
                    var isMember = setLookup.Contains(member);
#else
                    var isMember = Set.Contains(member.ToArray());
#endif
                    while (!RespWriteUtils.TryWriteInt32(isMember ? 1 : 0, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                _output.result1 = input.parseState.Count;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SetRemove(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var field = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Remove(field))
#else
                if (Set.Remove(field.ToArray()))
#endif
                {
                    _output->result1++;
                    this.UpdateSize(field, false);
                }
            }
        }

        private void SetLength(byte* output)
        {
            // SCARD key
            var _output = (ObjectOutputHeader*)output;
            _output->result1 = Set.Count;
        }

        private void SetPop(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // SPOP key [count]
            var count = input.arg1;
            var countDone = 0;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // key [count]
                if (count >= 1)
                {
                    // POP this number of random fields
                    var countParameter = count > Set.Count ? Set.Count : count;

                    // Write the size of the array reply
                    while (!RespWriteUtils.TryWriteArrayLength(countParameter, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    for (int i = 0; i < countParameter; i++)
                    {
                        // Generate a new index based on the elements left in the set
                        var index = RandomNumberGenerator.GetInt32(0, Set.Count);
                        var item = Set.ElementAt(index);
                        Set.Remove(item);
                        this.UpdateSize(item, false);
                        while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone++;
                    }

                    countDone += count - countDone;
                }
                else if (count == int.MinValue) // no count parameter is present, we just pop and return a random item of the set
                {
                    // Write a bulk string value of a random field from the hash value stored at key.
                    if (Set.Count > 0)
                    {
                        var index = RandomNumberGenerator.GetInt32(0, Set.Count);
                        var item = Set.ElementAt(index);
                        Set.Remove(item);
                        this.UpdateSize(item, false);
                        while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        // If set empty return nil
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    countDone++;
                }
                _output.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SetRandomMember(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.arg1;
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
                Span<int> indexes = default;

                if (count > 0)
                {
                    // Return an array of distinct elements
                    var countParameter = count > Set.Count ? Set.Count : count;

                    // The order of fields in the reply is not truly random
                    indexes = RandomUtils.PickKRandomIndexes(Set.Count, countParameter, seed);

                    // Write the size of the array reply
                    while (!RespWriteUtils.TryWriteArrayLength(countParameter, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var index in indexes)
                    {
                        var element = Set.ElementAt(index);
                        while (!RespWriteUtils.TryWriteBulkString(element, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone++;
                    }
                    countDone += count - countParameter;
                }
                else if (count == int.MinValue) // no count parameter is present
                {
                    // Return a single random element from the set
                    if (Set.Count > 0)
                    {
                        var index = RandomUtils.PickRandomIndex(Set.Count, seed);
                        var item = Set.ElementAt(index);
                        while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        // If set is empty, return nil
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    countDone++;
                }
                else // count < 0
                {
                    // Return an array with potentially duplicate elements
                    var countParameter = Math.Abs(count);

                    indexes = RandomUtils.PickKRandomIndexes(Set.Count, countParameter, seed, false);

                    if (Set.Count > 0)
                    {
                        // Write the size of the array reply
                        while (!RespWriteUtils.TryWriteArrayLength(countParameter, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        foreach (var index in indexes)
                        {
                            var element = Set.ElementAt(index);
                            while (!RespWriteUtils.TryWriteBulkString(element, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone++;
                        }
                    }
                    else
                    {
                        // If set is empty, return nil
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                _output.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}