// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// List - RESP specific operations
    /// </summary>
    public unsafe partial class ListObject : IGarnetObject
    {

        private void ListRemove(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            //indicates partial execution
            _output->countDone = Int32.MinValue;

            // get the source string to remove
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var item, ref ptr, end))
                return;

            var count = _input->count;
            var rem_count = 0;
            _output->countDone = 0;

            //remove all equals to item
            if (count == 0)
            {
                var elements = list.Count;
                list.Where(i => i.SequenceEqual(item)).ToList().ForEach(i => { list.Remove(i); this.UpdateSize(i, false); });
                rem_count = elements - list.Count;
            }
            else
            {
                while (rem_count < Math.Abs(count) && list.Count() > 0)
                {
                    var node = count > 0 ? list.FirstOrDefault(i => i.SequenceEqual(item)) : list.LastOrDefault(i => i.SequenceEqual(item));
                    if (node != null)
                    {
                        list.Remove(node);
                        this.UpdateSize(node, false);
                        rem_count++;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            _output->bytesDone = (int)(ptr - startptr);
            _output->opsDone = rem_count;
        }

        private void ListInsert(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;
            LinkedListNode<byte[]> current = null;

            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            //indicates partial execution
            _output->countDone = int.MinValue;

            if (list.Count > 0)
            {
                // figure out where to insert BEFORE or AFTER
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var position, ref ptr, end))
                    return;

                // get the source string
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var pivot, ref ptr, end))
                    return;

                // get the string to INSERT into the list
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var insertitem, ref ptr, end))
                    return;

                bool fBefore = CmdStrings.BEFORE.SequenceEqual(position);

                // find the first ocurrence of the pivot element
                current = list.Nodes().DefaultIfEmpty(null).FirstOrDefault(i => i.Value.SequenceEqual(pivot));
                var newNode = current != default ? (fBefore ? list.AddBefore(current, insertitem) : list.AddAfter(current, insertitem)) : default;
                if (current != null)
                    this.UpdateSize(insertitem);
                _output->opsDone = current != default ? list.Count : -1;
                _output->countDone = _output->opsDone;
            }
            // Write bytes parsed from input and count done, into output footer
            _output->bytesDone = (int)(ptr - startptr);
        }

        private void ListIndex(byte* input, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;
            byte[] item = default;

            ObjectOutputHeader _output = default;
            _output.opsDone = -1;
            try
            {
                var index = _input->count < 0 ? list.Count + _input->count : _input->count;
                item = list.ElementAtOrDefault(index);
                if (item != default)
                {
                    while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.opsDone = 1;
                }
            }
            finally
            {
                _output.countDone = _output.opsDone;
                _output.bytesDone = 0;

                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void ListRange(byte* input, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (0 == list.Count)
                {
                    // write empty list
                    while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    var start = _input->count < 0 ? list.Count + _input->count : _input->count;
                    if (start < 0) start = 0;

                    var stop = _input->done < 0 ? list.Count + _input->done : _input->done;
                    if (stop < 0) stop = 0;
                    if (stop >= list.Count) stop = list.Count - 1;

                    if (start > stop || 0 == list.Count)
                    {
                        while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        _output.opsDone = 0;
                    }
                    else
                    {
                        var count = stop - start + 1;
                        while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        var i = -1;
                        foreach (var bytes in list)
                        {
                            i++;
                            if (i < start)
                                continue;
                            if (i > stop)
                                break;
                            while (!RespWriteUtils.WriteBulkString(bytes, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        _output.opsDone = count;
                    }
                }
                //updating output
                _output.bytesDone = 0; // no reads done 
                _output.countDone = _output.opsDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void ListTrim(byte* input, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            if (list.Count > 0)
            {
                var start = _input->count < 0 ? list.Count + _input->count : _input->count;
                if (start < -(list.Count - 1) || start >= list.Count) start = list.Count - 1;

                var end = _input->done < 0 ? list.Count + _input->done : _input->done;
                if (end < -(list.Count - 1) || end >= list.Count) end = list.Count - 1;

                Debug.Assert(end - start <= list.Count);
                if (start > end)
                {
                    _output->opsDone = list.Count;
                    list.Clear();
                }
                else
                {
                    // Only  the first end+1 elements will remain
                    if (start == 0)
                    {
                        var numDeletes = list.Count - (end + 1);
                        for (int i = 0; i < numDeletes; i++)
                        {
                            var _value = list.Last.Value;
                            list.RemoveLast();
                            this.UpdateSize(_value, false);
                        }
                        _output->opsDone = numDeletes;
                    }
                    else
                    {
                        int i = 0;
                        IList<byte[]> readOnly = new List<byte[]>(list).AsReadOnly();
                        foreach (byte[] node in readOnly)
                        {
                            if (!(i >= start && i <= end))
                            {
                                list.Remove(node);
                                this.UpdateSize(node, false);
                            }
                            i++;
                        }
                        _output->opsDone = i;
                    }
                }
                _output->bytesDone = 0;
                _output->countDone = _output->opsDone;
            }
        }

        private void ListLength(byte* input, byte* output)
        {
            ((ObjectOutputHeader*)output)->countDone = list.Count;
        }

        private void ListPush(byte* input, int length, byte* output, bool fAddAtHead)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            //this value is used in the validations for partial execution
            _output->countDone = Int32.MinValue;

            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var value, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                //Add the value to the top of the list
                if (fAddAtHead)
                    list.AddFirst(value);
                else
                    list.AddLast(value);

                this.UpdateSize(value);
                _output->countDone = list.Count;
                _output->opsDone++;
                _output->bytesDone = (int)(ptr - startptr);
            }
        }

        private void ListPop(byte* input, ref SpanByteAndMemory output, bool fDelAtHead)
        {
            var _input = (ObjectInputHeader*)input;
            int count = _input->count; // for multiple elements

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            if (list.Count < count)
                count = list.Count;

            int countDone = 0;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (list.Count == 0)
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    count = 0;
                }
                else if (count > 1)
                {
                    while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                while (count > 0 && list.Any())
                {
                    LinkedListNode<byte[]> node = null;
                    if (fDelAtHead)
                    {
                        node = list.First;
                        list.RemoveFirst();
                    }
                    else
                    {
                        node = list.Last;
                        list.RemoveLast();
                    }

                    UpdateSize(node.Value, false);
                    while (!RespWriteUtils.WriteBulkString(node.Value, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    count--;
                    countDone++;
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

        private void ListSet(byte* input, int length, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var _out_ptr = output.SpanByte.ToPointer();
            var _out_curr = _out_ptr;
            var _out_end = _out_curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                if (list.Count == 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref _out_curr, _out_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref _out_ptr, ref ptrHandle, ref _out_curr, ref _out_end);
                    return;
                }

                byte* _in_ptr = input + sizeof(ObjectInputHeader);
                byte* _in_curr = _in_ptr;
                byte* _in_end = input + length;

                byte* indexParam = default;
                var indexParamSize = 0;

                // index
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref indexParam, ref indexParamSize, ref _in_curr, _in_end))
                    return;

                if (NumUtils.TryBytesToInt(indexParam, indexParamSize, out var index) == false)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref _out_curr, _out_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref _out_ptr, ref ptrHandle, ref _out_curr, ref _out_end);
                    return;
                }

                index = index < 0 ? list.Count + index : index;

                if (index > list.Count - 1)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE, ref _out_curr, _out_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref _out_ptr, ref ptrHandle, ref _out_curr, ref _out_end);
                    return;
                }

                // element
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var element, ref _in_curr, _in_end))
                    return;

                if (index == 0)
                {
                    UpdateSize(list.First.Value, false);
                    list.First.Value = element;
                    UpdateSize(list.First.Value, true);
                }
                else if (index == list.Count - 1)
                {
                    UpdateSize(list.Last.Value, false);
                    list.Last.Value = element;
                    UpdateSize(list.Last.Value, true);
                }
                else
                {
                    var start = 0;
                    foreach (var node in list.Nodes())
                    {
                        if (start == index)
                        {
                            UpdateSize(node.Value, false);
                            node.Value = element;
                            UpdateSize(element, false);
                            break;
                        }
                        start++;
                    }
                }

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref _out_curr, _out_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref _out_ptr, ref ptrHandle, ref _out_curr, ref _out_end);

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(_in_curr - _in_ptr);
                _output.countDone = 1;
                _output.opsDone = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref _out_curr, _out_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref _out_ptr, ref ptrHandle, ref _out_curr, ref _out_end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(_out_curr - _out_ptr);
            }
        }
    }
}