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

        private void ListRemove(ref ObjectInput input, byte* output)
        {
            var count = input.arg1;
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            //indicates partial execution
            _output->result1 = int.MinValue;

            // get the source string to remove
            var itemSpan = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;

            var removedCount = 0;
            _output->result1 = 0;

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
                var fromHeadToTail = count > 0;
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
            _output->result1 = removedCount;
        }

        private void ListInsert(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            //indicates partial execution
            _output->result1 = int.MinValue;

            if (list.Count > 0)
            {
                var currTokenIdx = input.parseStateStartIdx;

                // figure out where to insert BEFORE or AFTER
                var position = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                // get the source string
                var pivot = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                // get the string to INSERT into the list
                var item = input.parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

                var insertBefore = position.SequenceEqual(CmdStrings.BEFORE);

                _output->result1 = -1;

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
                        _output->result1 = list.Count;
                        break;
                    }
                }
                while ((currentNode = currentNode.Next) != null);
            }
        }

        private void ListIndex(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var index = input.arg1;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            _output.result1 = -1;

            try
            {
                index = index < 0 ? list.Count + index : index;
                var item = list.ElementAtOrDefault(index);
                if (item != default)
                {
                    while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.result1 = 1;
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

        private void ListRange(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var start = input.arg1;
            var stop = input.arg2;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

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
                    start = start < 0 ? list.Count + start : start;
                    if (start < 0) start = 0;

                    stop = stop < 0 ? list.Count + stop : stop;
                    if (stop < 0) stop = 0;
                    if (stop >= list.Count) stop = list.Count - 1;

                    if (start > stop || 0 == list.Count)
                    {
                        while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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
                        _output.result1 = count;
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

        private void ListTrim(ref ObjectInput input, byte* output)
        {
            var start = input.arg1;
            var end = input.arg2;

            var outputHeader = (ObjectOutputHeader*)output;

            if (list.Count > 0)
            {
                start = start < 0 ? list.Count + start : start;
                end = end < 0 ? list.Count + end : end;

                if (start > end || start >= list.Count || end < 0)
                {
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
                        outputHeader->result1 = numDeletes;
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
                        outputHeader->result1 = i;
                    }
                }
            }
        }

        private void ListLength(byte* output)
        {
            ((ObjectOutputHeader*)output)->result1 = list.Count;
        }

        private void ListPush(ref ObjectInput input, byte* output, bool fAddAtHead)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            _output->result1 = 0;
            for (var currTokenIdx = input.parseStateStartIdx; currTokenIdx < input.parseState.Count; currTokenIdx++)
            {
                var value = input.parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

                // Add the value to the top of the list
                if (fAddAtHead)
                    list.AddFirst(value);
                else
                    list.AddLast(value);

                this.UpdateSize(value);
            }
            _output->result1 = list.Count;
        }

        private void ListPop(ref ObjectInput input, ref SpanByteAndMemory output, bool fDelAtHead)
        {
            var count = input.arg1;

            if (list.Count < count)
                count = list.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

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

        private void ListSet(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var output_startptr = output.SpanByte.ToPointer();
            var output_currptr = output_startptr;
            var output_end = output_currptr + output.Length;

            ObjectOutputHeader _output = default;
            var currTokenIdx = input.parseStateStartIdx;
            try
            {
                if (list.Count == 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                // index
                if (!input.parseState.TryGetInt(currTokenIdx++, out var index))
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
                var element = input.parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

                var targetNode = index == 0 ? list.First
                    : (index == list.Count - 1 ? list.Last
                        : list.Nodes().ElementAtOrDefault(index));

                UpdateSize(targetNode.Value, false);
                targetNode.Value = element;
                UpdateSize(targetNode.Value);

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                _output.result1 = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(output_currptr - output_startptr);
            }
        }

        private void ListPosition(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var element = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;
            input.parseStateStartIdx++;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var output_startptr = output.SpanByte.ToPointer();
            var output_currptr = output_startptr;
            var output_end = output_currptr + output.Length;
            var count = 0;
            ObjectOutputHeader outputHeader = default;

            try
            {
                if (!ReadListPositionInput(ref input, out var rank, out count, out var maxlen, out var error))
                {
                    while (!RespWriteUtils.WriteError(error, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                if (count < 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                if (maxlen < 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                if (rank == 0)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                    return;
                }

                count = count == 0 ? list.Count : count;
                var totalArrayHeaderLen = 0;
                var lastFoundItemIndex = -1;

                if (count != 1)
                {
                    while (!RespWriteUtils.WriteArrayLength(count, ref output_currptr, output_end, out var _, out totalArrayHeaderLen))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                }

                var noOfFoundItem = 0;
                if (rank > 0)
                {
                    var currentNode = list.First;
                    var currentIndex = 0;
                    var maxlenIndex = maxlen == 0 ? list.Count : maxlen;
                    do
                    {
                        var nextNode = currentNode.Next;
                        if (currentNode.Value.AsSpan().SequenceEqual(element))
                        {
                            if (rank == 1)
                            {
                                lastFoundItemIndex = currentIndex;
                                while (!RespWriteUtils.WriteInteger(currentIndex, ref output_currptr, output_end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                                noOfFoundItem++;
                                if (noOfFoundItem == count)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                rank--;
                            }
                        }
                        currentNode = nextNode;
                        currentIndex++;
                    }
                    while (currentNode != null && currentIndex < maxlenIndex);
                }
                else // (rank < 0)
                {
                    var currentNode = list.Last;
                    var currentIndex = list.Count - 1;
                    var maxlenIndex = maxlen == 0 ? 0 : list.Count - maxlen;
                    do
                    {
                        var nextNode = currentNode.Previous;
                        if (currentNode.Value.AsSpan().SequenceEqual(element))
                        {
                            if (rank == -1)
                            {
                                lastFoundItemIndex = currentIndex;
                                while (!RespWriteUtils.WriteInteger(currentIndex, ref output_currptr, output_end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                                noOfFoundItem++;
                                if (noOfFoundItem == count)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                rank++;
                            }
                        }
                        currentNode = nextNode;
                        currentIndex--;
                    }
                    while (currentNode != null && currentIndex >= maxlenIndex);
                }

                if (noOfFoundItem == 0)
                {
                    output_currptr = output_startptr;
                    while (!RespWriteUtils.WriteNull(ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                }
                else if (count > 1 && noOfFoundItem == 1)
                {
                    output_currptr = output_startptr;
                    while (!RespWriteUtils.WriteInteger(lastFoundItemIndex, ref output_currptr, output_end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                }
                else if(noOfFoundItem > 1)
                {
                    var newTotalArrayHeaderLen = 0;
                    var startOutputStartptr = output_startptr;
                    RespWriteUtils.WriteArrayLength(noOfFoundItem, ref startOutputStartptr, output_end, out var _, out newTotalArrayHeaderLen);  // ReallocateOutput is not needed here as there should be always be available space in the output buffer as we have already written the max array length
                    Debug.Assert(totalArrayHeaderLen >= newTotalArrayHeaderLen, "newTotalArrayHeaderLen can't be bigger than totalArrayHeaderLen as we have already written max array lenght in the buffer");

                    if (totalArrayHeaderLen != newTotalArrayHeaderLen)
                    {
                        Buffer.MemoryCopy(output_startptr + totalArrayHeaderLen, output_startptr + newTotalArrayHeaderLen, output.Length, output.Length);
                        output_currptr = output_currptr - (totalArrayHeaderLen - newTotalArrayHeaderLen);
                    }
                }

                outputHeader.result1 = noOfFoundItem;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

                if (isMemory)
                    ptrHandle.Dispose();
                output.Length = (int)(output_currptr - output_startptr);
            }
        }

        private static unsafe bool ReadListPositionInput(ref ObjectInput input, out int rank, out int count, out int maxlen, out ReadOnlySpan<byte> error)
        {
            var currTokenIdx = input.parseStateStartIdx;

            rank = 1; // By default, LPOS takes first match element
            count = 1; // By default, LPOS return 1 element
            maxlen = 0; // By default, iterate to all the item

            error = default;

            while (currTokenIdx < input.parseState.Count)
            {
                var sbParam = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (sbParam.SequenceEqual(CmdStrings.RANK) || sbParam.SequenceEqual(CmdStrings.rank))
                {
                    if (!input.parseState.TryGetInt(currTokenIdx++, out rank))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }
                }
                else if (sbParam.SequenceEqual(CmdStrings.COUNT) || sbParam.SequenceEqual(CmdStrings.count))
                {
                    if (!input.parseState.TryGetInt(currTokenIdx++, out count))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }
                }
                else if (sbParam.SequenceEqual(CmdStrings.MAXLEN) || sbParam.SequenceEqual(CmdStrings.maxlen))
                {
                    if (!input.parseState.TryGetInt(currTokenIdx++, out maxlen))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }
                }
                else
                {
                    error = CmdStrings.RESP_SYNTAX_ERROR;
                    return false;
                }
            }

            return true;
        }
    }
}