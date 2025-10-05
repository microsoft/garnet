// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    /// <summary>
    /// Operations on SortedSet
    /// </summary>
    public enum ListOperation : byte
    {
        LPOP,
        LPUSH,
        LPUSHX,
        RPOP,
        RPUSH,
        RPUSHX,
        LLEN,
        LTRIM,
        LRANGE,
        LINDEX,
        LINSERT,
        LREM,
        RPOPLPUSH,
        LMOVE,
        LSET,
        BRPOP,
        BLPOP,
        LPOS,
    }

    /// <summary>
    /// Direction for the List operations
    /// </summary>
    public enum OperationDirection : byte
    {
        /// <summary>
        /// Left or head
        /// </summary>
        Left,

        /// <summary>
        /// Right or tail
        /// </summary>
        Right,
        Unknown,
    }

    /// <summary>
    /// List
    /// </summary>
    public partial class ListObject : GarnetObjectBase
    {
        readonly LinkedList<byte[]> list;

        /// <summary>
        /// Constructor
        /// </summary>
        public ListObject()
            : base(new(MemoryUtils.ListOverhead, sizeof(int), serializedIsExact: true))
        {
            list = new LinkedList<byte[]>();
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public ListObject(BinaryReader reader)
            : base(reader, new(MemoryUtils.ListOverhead, sizeof(int), serializedIsExact: true))
        {
            list = new LinkedList<byte[]>();

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var item = reader.ReadBytes(reader.ReadInt32());
                list.AddLast(item);

                this.UpdateSize(item);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public ListObject(LinkedList<byte[]> list, ObjectSizes sizes)
            : base(sizes)
        {
            this.list = list;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.List;

        /// <summary>
        /// Public getter for the list
        /// </summary>
        public LinkedList<byte[]> LnkList => list;

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            int count = list.Count;
            writer.Write(count);
            foreach (var item in list)
            {
                writer.Write(item.Length);
                writer.Write(item);
                count--;
            }
            Debug.Assert(count == 0);
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new ListObject(list, sizes);

        /// <inheritdoc />
        public override bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output,
                                     byte respProtocolVersion, out long memorySizeChange)
        {
            memorySizeChange = 0;

            if (input.header.type != GarnetObjectType.List)
            {
                // Indicates an incorrect type of key
                output.OutputFlags |= OutputFlags.WrongType;
                output.SpanByteAndMemory.Length = 0;
                return true;
            }

            var previousMemorySize = this.HeapMemorySize;
            switch (input.header.ListOp)
            {
                case ListOperation.LPUSH:
                case ListOperation.LPUSHX:
                    ListPush(ref input, ref output, true);
                    break;
                case ListOperation.LPOP:
                    ListPop(ref input, ref output, respProtocolVersion, true);
                    break;
                case ListOperation.RPUSH:
                case ListOperation.RPUSHX:
                    ListPush(ref input, ref output, false);
                    break;
                case ListOperation.RPOP:
                    ListPop(ref input, ref output, respProtocolVersion, false);
                    break;
                case ListOperation.LLEN:
                    ListLength(ref output);
                    break;
                case ListOperation.LTRIM:
                    ListTrim(ref input, ref output);
                    break;
                case ListOperation.LRANGE:
                    ListRange(ref input, ref output, respProtocolVersion);
                    break;
                case ListOperation.LINDEX:
                    ListIndex(ref input, ref output, respProtocolVersion);
                    break;
                case ListOperation.LINSERT:
                    ListInsert(ref input, ref output);
                    break;
                case ListOperation.LREM:
                    ListRemove(ref input, ref output);
                    break;
                case ListOperation.LSET:
                    ListSet(ref input, ref output, respProtocolVersion);
                    break;
                case ListOperation.LPOS:
                    ListPosition(ref input, ref output, respProtocolVersion);
                    break;

                default:
                    throw new GarnetException($"Unsupported operation {input.header.ListOp} in ListObject.Operate");
            }

            memorySizeChange = this.HeapMemorySize - previousMemorySize;

            if (list.Count == 0)
                output.OutputFlags |= OutputFlags.RemoveKey;

            return true;
        }

        internal void UpdateSize(byte[] item, bool add = true)
        {
            var memorySize = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + MemoryUtils.ListEntryOverhead;
            var diskSize = sizeof(int) + item.Length;

            if (add)
            {
                this.HeapMemorySize += memorySize;
                this.SerializedSize += diskSize;
            }
            else
            {
                this.HeapMemorySize -= memorySize;
                this.SerializedSize -= diskSize;
                Debug.Assert(this.HeapMemorySize >= MemoryUtils.ListOverhead);
                Debug.Assert(this.SerializedSize >= sizeof(int));
            }
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            throw new NotImplementedException("For scan items in a list use LRANGE command");
        }
    }


    /// <summary>
    /// Extensions methods for LinkedList
    /// </summary>
    public static class LinkedListHelper
    {

        /// <summary>
        /// Extension method that gets an enumerable to compare the value of each node
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public static IEnumerable<LinkedListNode<T>> Nodes<T>(this LinkedList<T> list)
        {
            for (var node = list.First; node != null; node = node.Next)
            {
                yield return node;
            }
        }
    }
}