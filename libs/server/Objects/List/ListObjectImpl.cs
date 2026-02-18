// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// List - RESP specific operations
    /// </summary>
    public partial class ListObject : IGarnetObject
    {
        private void ListRemove(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var count = input.arg1;

            // get the source string to remove
            var itemSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

            var removedCount = 0;

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
                        UpdateSize(currentNode.Value, false);
                        removedCount++;
                    }

                    currentNode = nextNode;
                }
            }

            if (!input.header.CheckSkipRespOutputFlag())
            {
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteInt32(removedCount);
            }

            if (removedCount == 0)
                output.OutputFlags |= ObjectOutputFlags.ValueUnchanged;

            output.Result1 = removedCount;
        }

        private void ListInsert(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // Result is -1 if the pivot was not found.
            var result = -1;

            if (list.Count > 0)
            {
                // figure out where to insert BEFORE or AFTER
                var position = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                // get the source string
                var pivot = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                // get the string to INSERT into the list
                var item = input.parseState.GetArgSliceByRef(2).ToArray();

                var insertBefore = position.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BEFORE);

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

                        UpdateSize(item);
                        result = list.Count;
                        break;
                    }
                }
                while ((currentNode = currentNode.Next) != null);
            }

            if (!input.header.CheckSkipRespOutputFlag())
            {
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteInt32(result);
            }

            if (result == -1)
                output.OutputFlags |= ObjectOutputFlags.ValueUnchanged;

            output.Result1 = result;
        }

        private void ListIndex(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var index = input.arg1;

            output.Result1 = -1;

            index = index < 0 ? list.Count + index : index;
            var item = list.ElementAtOrDefault(index);
            if (item != null)
            {
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteBulkString(item);
                output.Result1 = 1;
            }
        }

        private void ListRange(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var start = input.arg1;
            var stop = input.arg2;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (0 == list.Count)
            {
                // write empty list
                writer.WriteEmptyArray();
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
                    writer.WriteEmptyArray();
                }
                else
                {
                    var count = stop - start + 1;
                    writer.WriteArrayLength(count);

                    var i = -1;
                    foreach (var bytes in list)
                    {
                        i++;
                        if (i < start)
                            continue;
                        if (i > stop)
                            break;
                        writer.WriteBulkString(bytes);
                    }

                    output.Result1 = count;
                }
            }
        }

        private void ListTrim(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var start = input.arg1;
            var end = input.arg2;

            var trimmed = 0;
            if (list.Count > 0)
            {
                start = start < 0 ? list.Count + start : start;
                end = end < 0 ? list.Count + end : end;

                if (start > end || start >= list.Count || end < 0)
                {
                    trimmed = list.Count;
                    list.Clear();
                }
                else
                {
                    start = start < 0 ? 0 : start;
                    end = end >= list.Count ? list.Count : end + 1;

                    // Only  the first end elements will remain
                    if (start == 0)
                    {
                        trimmed = list.Count - end;
                        for (var i = 0; i < trimmed; i++)
                        {
                            var value = list.Last!.Value;
                            list.RemoveLast();
                            this.UpdateSize(value, false);
                        }
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
                        trimmed = i;
                    }
                }
            }

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteDirect(CmdStrings.RESP_OK);

            if (trimmed == 0)
                output.OutputFlags |= ObjectOutputFlags.ValueUnchanged;

            output.Result1 = trimmed;
        }

        private void ListLength(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var length = list.Count;

            if (!input.header.CheckSkipRespOutputFlag())
            {
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteInt32(length);
            }

            output.Result1 = length;
        }

        private void ListPush(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var addFirst = input.header.ListOp is ListOperation.LPUSH or ListOperation.LPUSHX;

            output.Result1 = 0;
            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ToArray();

                // Add the value to the top of the list
                if (addFirst)
                    list.AddFirst(value);
                else
                    list.AddLast(value);

                UpdateSize(value);
            }

            if (!input.header.CheckSkipRespOutputFlag())
            {
                using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                writer.WriteInt32(list.Count);
            }

            output.Result1 = list.Count;
        }

        private void ListPop(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var removeFirst = input.header.ListOp == ListOperation.LPOP;

            var count = input.arg1;

            if (list.Count < count)
                count = list.Count;
            
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (list.Count == 0)
            {
                writer.WriteNull();
                count = 0;
            }
            else if (count > 1)
            {
                writer.WriteArrayLength(count);
            }

            while (count > 0 && list.Any())
            {
                LinkedListNode<byte[]> node = null;
                if (removeFirst)
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
                writer.WriteBulkString(node.Value);

                count--;
                output.Result1++;
            }
        }

        private void ListSet(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // index
            var index = input.parseState.GetInt(0);

            index = index < 0 ? list.Count + index : index;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (index > list.Count - 1 || index < 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_INDEX_OUT_RANGE);
                output.OutputFlags |= ObjectOutputFlags.ValueUnchanged;
                return;
            }

            // element
            var element = input.parseState.GetArgSliceByRef(1).ToArray();

            var targetNode = index == 0 ? list.First
                : (index == list.Count - 1 ? list.Last
                    : list.Nodes().ElementAtOrDefault(index));

            UpdateSize(targetNode.Value, false);
            targetNode.Value = element;
            UpdateSize(targetNode.Value);

            writer.WriteDirect(CmdStrings.RESP_OK);
            output.Result1 = 1;
        }

        private void ListPosition(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var element = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (!input.parseState.TryGetListPositionOptions(1, out var rank, out var count, out var isDefaultCount,
                    out var maxLen, out var error))
            {
                writer.WriteError(error);
                return;
            }

            count = count == 0 ? list.Count : count;
            var totalArrayHeaderLen = 0;
            var lastFoundItemIndex = -1;

            if (!isDefaultCount)
            {
                writer.WriteArrayLength(count, out _, out totalArrayHeaderLen);
            }

            var noOfFoundItem = 0;
            if (rank > 0)
            {
                var currentNode = list.First;
                var currentIndex = 0;
                var maxlenIndex = maxLen == 0 ? list.Count : maxLen;
                do
                {
                    var nextNode = currentNode.Next;
                    if (currentNode.Value.AsSpan().SequenceEqual(element))
                    {
                        if (rank == 1)
                        {
                            lastFoundItemIndex = currentIndex;
                            writer.WriteInt32(currentIndex);

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
                var maxlenIndex = maxLen == 0 ? 0 : list.Count - maxLen;
                do
                {
                    var nextNode = currentNode.Previous;
                    if (currentNode.Value.AsSpan().SequenceEqual(element))
                    {
                        if (rank == -1)
                        {
                            lastFoundItemIndex = currentIndex;
                            writer.WriteInt32(currentIndex);

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

            if (isDefaultCount && noOfFoundItem == 0)
            {
                writer.ResetPosition();
                writer.WriteNull();
            }
            else if (!isDefaultCount && noOfFoundItem == 0)
            {
                writer.ResetPosition();
                writer.WriteEmptyArray();
            }
            else if (!isDefaultCount && noOfFoundItem != count)
            {
                writer.DecreaseArrayLength(noOfFoundItem, totalArrayHeaderLen);
            }

            output.Result1 = noOfFoundItem;
        }
    }
}