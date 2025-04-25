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
        SMOVE,
        SRANDMEMBER,
        SISMEMBER,
        SMISMEMBER,
        SUNION,
        SUNIONSTORE,
        SDIFF,
        SDIFFSTORE,
        SINTER,
        SINTERSTORE
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
        public SetObject()
            : base(new(MemoryUtils.HashSetOverhead, sizeof(int)))
        {
            set = new HashSet<byte[]>(ByteArrayComparer.Instance);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public SetObject(BinaryReader reader)
            : base(reader, new(MemoryUtils.HashSetOverhead, sizeof(int)))
        {
            set = new HashSet<byte[]>(ByteArrayComparer.Instance);

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
        public SetObject(HashSet<byte[]> set, ObjectSizes sizes)
            : base(sizes)
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
        public override GarnetObjectBase Clone() => new SetObject(set, sizes);

        /// <inheritdoc />
        public override unsafe bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, out long memorySizeChange)
        {
            memorySizeChange = 0;

            fixed (byte* outputSpan = output.SpanByteAndMemory.SpanByte.Span)
            {
                if (input.header.type != GarnetObjectType.Set)
                {
                    // Indicates an incorrect type of key
                    output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                    output.SpanByteAndMemory.Length = 0;
                    return true;
                }

                var prevMemorySize = this.MemorySize;
                switch (input.header.SetOp)
                {
                    case SetOperation.SADD:
                        SetAdd(ref input, outputSpan);
                        break;
                    case SetOperation.SMEMBERS:
                        SetMembers(ref output.SpanByteAndMemory);
                        break;
                    case SetOperation.SISMEMBER:
                        SetIsMember(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SetOperation.SMISMEMBER:
                        SetMultiIsMember(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SetOperation.SREM:
                        SetRemove(ref input, outputSpan);
                        break;
                    case SetOperation.SCARD:
                        SetLength(outputSpan);
                        break;
                    case SetOperation.SPOP:
                        SetPop(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SetOperation.SRANDMEMBER:
                        SetRandomMember(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SetOperation.SSCAN:
                        if (ObjectUtils.ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                                out var patternLength, out var limitCount, out _, out var error))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                                patternLength: patternLength);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output.SpanByteAndMemory);
                        }
                        else
                        {
                            ObjectUtils.WriteScanError(error, ref output.SpanByteAndMemory);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {input.header.SetOp} in SetObject.Operate");
                }

                memorySizeChange = this.MemorySize - prevMemorySize;
            }

            if (set.Count == 0)
                output.OutputFlags |= ObjectStoreOutputFlags.RemoveKey;

            return true;
        }

        internal void UpdateSize(ReadOnlySpan<byte> item, bool add = true)
        {
            var memorySize = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + MemoryUtils.HashSetEntryOverhead;
            var kvSize = sizeof(int) + item.Length;

            if (add)
            {
                this.MemorySize += memorySize;
                this.DiskSize += kvSize;
            }
            else
            {
                this.MemorySize -= memorySize;
                this.DiskSize -= kvSize;
                Debug.Assert(this.MemorySize >= MemoryUtils.HashSetOverhead);
                Debug.Assert(this.DiskSize >= sizeof(int));
            }
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
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

        public HashSet<byte[]> Set => set;
    }
}