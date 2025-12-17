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
        private void ListRemove(ref ObjectInput input, ref ObjectOutput output)
        {
            var count = input.arg1;

            //indicates partial execution
            output.Header.result1 = int.MinValue;

            // get the source string to remove
            var itemSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

            var removedCount = 0;
            output.Header.result1 = 0;

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
            output.Header.result1 = removedCount;
        }

        private void ListInsert(ref ObjectInput input, ref ObjectOutput output)
        {
            //indicates partial execution
            output.Header.result1 = int.MinValue;

            if (list.Count > 0)
            {
                // figure out where to insert BEFORE or AFTER
                var position = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                // get the source string
                var pivot = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                // get the string to INSERT into the list
                var item = input.parseState.GetArgSliceByRef(2).ToArray();

                var insertBefore = position.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BEFORE);

                output.Header.result1 = -1;

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
                        output.Header.result1 = list.Count;
                        break;
                    }
                }
                while ((currentNode = currentNode.Next) != null);
            }
        }

        private void ListIndex(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var index = input.arg1;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            output.Header.result1 = -1;

            index = index < 0 ? list.Count + index : index;
            var item = list.ElementAtOrDefault(index);
            if (item != default)
            {
                writer.WriteBulkString(item);
                output.Header.result1 = 1;
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

                    output.Header.result1 = count;
                }
            }
        }

        private void ListTrim(ref ObjectInput input, ref ObjectOutput output)
        {
            var start = input.arg1;
            var end = input.arg2;

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
                        output.Header.result1 = numDeletes;
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
                        output.Header.result1 = i;
                    }
                }
            }
        }

        private void ListLength(ref ObjectOutput output)
        {
            output.Header.result1 = list.Count;
        }

        private void ListPush(ref ObjectInput input, ref ObjectOutput output, bool fAddAtHead)
        {
            output.Header.result1 = 0;
            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ToArray();

                // Add the value to the top of the list
                if (fAddAtHead)
                    list.AddFirst(value);
                else
                    list.AddLast(value);

                UpdateSize(value);
            }
            output.Header.result1 = list.Count;
        }

        private void ListPop(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion, bool fDelAtHead)
        {
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
                writer.WriteBulkString(node.Value);

                count--;
                output.Header.result1++;
            }
        }

        private void ListSet(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (list.Count == 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY);
                return;
            }

            // index
            if (!input.parseState.TryGetInt(0, out var index))
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                return;
            }

            index = index < 0 ? list.Count + index : index;

            if (index > list.Count - 1 || index < 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_INDEX_OUT_RANGE);
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
            output.Header.result1 = 1;
        }

        private void ListPosition(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var element = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

            var count = 0;
            var isDefaultCount = true;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (!ReadListPositionInput(ref input, out var rank, out count, out isDefaultCount, out var maxlen, out var error))
            {
                writer.WriteError(error);
                return;
            }

            if (count < 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                return;
            }

            if (maxlen < 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                return;
            }

            if (rank == 0)
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
                var maxlenIndex = maxlen == 0 ? list.Count : maxlen;
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
                var maxlenIndex = maxlen == 0 ? 0 : list.Count - maxlen;
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

            output.Header.result1 = noOfFoundItem;
        }

        private static bool ReadListPositionInput(ref ObjectInput input, out int rank, out int count, out bool isDefaultCount, out int maxlen, out ReadOnlySpan<byte> error)
        {
            rank = 1; // By default, LPOS takes first match element
            count = 1; // By default, LPOS return 1 element
            isDefaultCount = true;
            maxlen = 0; // By default, iterate to all the item

            error = default;

            var currTokenIdx = 1;

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

                    isDefaultCount = false;
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