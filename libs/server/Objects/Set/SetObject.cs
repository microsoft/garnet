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
    /// Operations on Set
    /// </summary>
    public enum SetOperation : byte
    {
        SADD,
        SREM,
        SPOP,
        SMEMBERS,
        SCARD,
        SSCAN,
    }


    /// <summary>
    ///  Set Object Class
    /// </summary>
    public unsafe partial class SetObject : GarnetObjectBase
    {
        readonly HashSet<byte[]> set;

        /// <summary>
        ///  Constructor
        /// </summary>
        public SetObject(long expiration = 0)
            : base(expiration, MemoryUtils.HashSetOverhead)
        {
            set = new HashSet<byte[]>(new ByteArrayComparer());
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public SetObject(BinaryReader reader)
            : base(reader, MemoryUtils.HashSetOverhead)
        {
            set = new HashSet<byte[]>(new ByteArrayComparer());

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var item = reader.ReadBytes(reader.ReadInt32());
                set.Add(item);

                this.UpdateSize(item);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public SetObject(HashSet<byte[]> set, long expiration, long size)
            : base(expiration, size)
        {
            this.set = set;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Set;

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            int count = set.Count;
            writer.Write(count);
            foreach (var item in set)
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
        public override GarnetObjectBase Clone() => new SetObject(set, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange)
        {
            fixed (byte* _input = input.AsSpan())
            fixed (byte* _output = output.SpanByte.AsSpan())
            {
                var header = (RespInputHeader*)_input;
                Debug.Assert(header->type == GarnetObjectType.Set);
                long prevSize = this.Size;
                switch (header->SetOp)
                {
                    case SetOperation.SADD:
                        SetAdd(_input, input.Length, _output);
                        break;
                    case SetOperation.SMEMBERS:
                        SetMembers(_input, input.Length, ref output);
                        break;
                    case SetOperation.SREM:
                        SetRemove(_input, input.Length, _output);
                        break;
                    case SetOperation.SCARD:
                        SetLength(_input, input.Length, _output);
                        break;
                    case SetOperation.SPOP:
                        SetPop(_input, input.Length, ref output);
                        break;
                    case SetOperation.SSCAN:
                        if (ObjectUtils.ReadScanInput(_input, input.Length, ref output, out var cursorInput, out var pattern, out var patternLength, out int limitCount, out int bytesDone))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern, patternLength: patternLength);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output, bytesDone);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {(SetOperation)_input[0]} in SetObject.Operate");
                }
                sizeChange = this.Size - prevSize;
            }
            return true;
        }

        private void UpdateSize(byte[] item, bool add = true)
        {
            var size = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + MemoryUtils.HashSetEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.HashSetOverhead);
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0)
        {
            cursor = start;
            items = new List<byte[]>();

            if (set.Count < start)
            {
                cursor = 0;
                return;
            }

            int index = 0;
            foreach (var item in set)
            {
                if (index < start)
                {
                    index++;
                    continue;
                }

                if (patternLength == 0)
                {
                    items.Add(item);
                }
                else
                {
                    fixed (byte* keyPtr = item)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Length))
                        {
                            items.Add(item);
                        }
                    }
                }

                cursor++;

                if (items.Count == count)
                    break;
            }

            // Indicates end of collection has been reached.
            if (cursor == set.Count)
                cursor = 0;
        }
    }
}