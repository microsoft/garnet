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
    }

    /// <summary>
    /// Direction for the List operations
    /// </summary>
    public enum OperationDirection
    {
        /// <summary>
        /// Left or head
        /// </summary>
        Left,

        /// <summary>
        /// Right or tail
        /// </summary>
        Right,
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
        public ListObject(long expiration = 0)
            : base(expiration, MemoryUtils.ListOverhead)
        {
            list = new LinkedList<byte[]>();
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public ListObject(BinaryReader reader)
            : base(reader, MemoryUtils.ListOverhead)
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
        public ListObject(LinkedList<byte[]> list, long expiration, long size)
            : base(expiration, size)
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
        public override GarnetObjectBase Clone() => new ListObject(list, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange)
        {
            fixed (byte* _input = input.AsSpan())
            fixed (byte* _output = output.SpanByte.AsSpan())
            {
                var header = (RespInputHeader*)_input;
                if (header->type != GarnetObjectType.List)
                {
                    //Indicates an incorrect type of key
                    output.Length = 0;
                    sizeChange = 0;
                    return true;
                }

                var previouseSize = this.Size;
                switch (header->ListOp)
                {
                    case ListOperation.LPUSH:
                    case ListOperation.LPUSHX:
                        ListPush(_input, input.Length, _output, true);
                        break;
                    case ListOperation.LPOP:
                        ListPop(_input, ref output, true);
                        break;
                    case ListOperation.RPUSH:
                    case ListOperation.RPUSHX:
                        ListPush(_input, input.Length, _output, false);
                        break;
                    case ListOperation.RPOP:
                        ListPop(_input, ref output, false);
                        break;
                    case ListOperation.LLEN:
                        ListLength(_input, _output);
                        break;
                    case ListOperation.LTRIM:
                        ListTrim(_input, _output);
                        break;
                    case ListOperation.LRANGE:
                        ListRange(_input, ref output);
                        break;
                    case ListOperation.LINDEX:
                        ListIndex(_input, ref output);
                        break;
                    case ListOperation.LINSERT:
                        ListInsert(_input, input.Length, _output);
                        break;
                    case ListOperation.LREM:
                        ListRemove(_input, input.Length, _output);
                        break;

                    default:
                        throw new GarnetException($"Unsupported operation {(ListOperation)_input[0]} in ListObject.Operate");
                }

                sizeChange = this.Size - previouseSize;
            }
            return true;
        }

        internal void UpdateSize(byte[] item, bool add = true)
        {
            var size = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + MemoryUtils.ListEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.ListOverhead);
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0)
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