// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
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
            if (!RespReadUtils.TrySliceWithLengthHeader(out var itemSpan, ref ptr, end))
                return;

            var count = _input->count;
            var removedCount = 0;
            _output->countDone = 0;

            //remove all equals to item
            if (count == 0)
            {
                var currentNode = list.First;
                do
                {
                    var nextNode = currentNode.Next;
                    if (currentNode.Value.AsSpan().SequenceEqual(itemSpan))
                    {
                        list.Remove(currentNode);
                        this.UpdateSize(currentNode.Value, false);

                        removedCount++;
                    }
                    currentNode = nextNode;
                }
                while (currentNode != null);
            }
            else
            {
                bool fromHeadToTail = count > 0;
                var currentNode = fromHeadToTail ? list.First : list.Last;

                count = Math.Abs(count);
                while (removedCount < count && currentNode != null)
                {
                    var nextNode = fromHeadToTail ? currentNode.Next : currentNode.Previous;

                    if (currentNode.Value.AsSpan().SequenceEqual(itemSpan))
                    {
                        list.Remove(currentNode);
                        this.UpdateSize(currentNode.Value, false);
                        removedCount++;
                    }

                    currentNode = nextNode;
                }
            }
            _output->bytesDone = (int)(ptr - startptr);
            _output->opsDone = removedCount;
        }

        private void ListInsert(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            //indicates partial execution
            _output->countDone = int.MinValue;

            if (list.Count > 0)
            {
                // figure out where to insert BEFORE or AFTER
                if (!RespReadUtils.TrySliceWithLengthHeader(out var position, ref ptr, end))
                    return;

                // get the source string
                if (!RespReadUtils.TrySliceWithLengthHeader(out var pivot, ref ptr, end))
                    return;

                // get the string to INSERT into the list
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var item, ref ptr, end))
                    return;

                var insertBefore = position.SequenceEqual(CmdStrings.BEFORE);

                _output->opsDone = -1;

                // find the first ocurrence of the pivot element
                var currentNode = list.First;
                do
                {
                    if (currentNode.Value.AsSpan().SequenceEqual(pivot))
                    {
                        if (insertBefore)
                            list.AddBefore(currentNode, item);
                        else
                            list.AddAfter(currentNode, item);

                        this.UpdateSize(item);
                        _output->opsDone = list.Count;
                        break;
                    }
                }
                while ((currentNode = currentNode.Next) != null);

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
            var inputHeader = (ObjectInputHeader*)input;
            var outputHeader = (ObjectOutputHeader*)output;

            if (list.Count > 0)
            {
                var start = inputHeader->count < 0 ? list.Count + inputHeader->count : inputHeader->count;
                var end = inputHeader->done < 0 ? list.Count + inputHeader->done : inputHeader->done;

                if (start > end || start >= list.Count || end < 0)
                {
                    outputHeader->opsDone = list.Count;
                    list.Clear();
                }
                else
                {
                    start = start < 0 ? 0 : start;
                    end = end >= list.Count ? list.Count : end + 1;

                    // Only  the first end elements will remain
                    if (start == 0)
                    {
                        var numDeletes = list.Count - end;
                        for (var i = 0; i < numDeletes; i++)
                        {
                            var value = list.Last!.Value;
                            list.RemoveLast();
                            this.UpdateSize(value, false);
                        }
                        outputHeader->opsDone = numDeletes;
                    }
                    else
                    {
                        var i = 0;
                        IList<byte[]> readOnly = new List<byte[]>(list).AsReadOnly();
                        foreach (var node in readOnly)
                        {
                            if (!(i >= start && i < end))
                            {
                                list.Remove(node);
                                this.UpdateSize(node, false);
                            }
                            i++;
                        }
                        outputHeader->opsDone = i;
                    }
                }

                outputHeader->bytesDone = 0;
                outputHeader->countDone = outputHeader->opsDone;
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
            var output_startptr = output.SpanByte.ToPointer();
            var output_currptr = output_startptr;
            var output_end = output_currptr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                if (list.Count == 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                byte* input_startptr = input + sizeof(ObjectInputHeader);
                byte* input_currptr = input_startptr;
                byte* input_end = input + length;

                // index
                if (!RespReadUtils.TrySliceWithLengthHeader(out var indexParamBytes, ref input_currptr, input_end))
                    return;

                if (!NumUtils.TryParse(indexParamBytes, out int index))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                index = index < 0 ? list.Count + index : index;

                if (index > list.Count - 1 || index < 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_INDEX_OUT_RANGE, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                // element
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var element, ref input_currptr, input_end))
                    return;

                var targetNode = index == 0 ? list.First
                    : (index == list.Count - 1 ? list.Last
                        : list.Nodes().ElementAtOrDefault(index));

                UpdateSize(targetNode.Value, false);
                targetNode.Value = element;
                UpdateSize(targetNode.Value);

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = 1;
                _output.opsDone = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(output_currptr - output_startptr);
            }
        }
    }
}