﻿// Copyright (c) Microsoft Corporation.
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
    /// Operations on Hash
    /// </summary>
    public enum HashOperation : byte
    {
        HGET,
        HMGET,
        HSET,
        HMSET,
        HSETNX,
        HLEN,
        HDEL,
        HEXISTS,
        HGETALL,
        HKEYS,
        HVALS,
        HINCRBY,
        HINCRBYFLOAT,
        HRANDFIELD,
        HSCAN,
        HSTRLEN
    }


    /// <summary>
    ///  Hash Object Class
    /// </summary>
    public unsafe partial class HashObject : GarnetObjectBase
    {
        readonly Dictionary<byte[], byte[]> hash;

        /// <summary>
        ///  Constructor
        /// </summary>
        public HashObject(long expiration = 0)
            : base(expiration, MemoryUtils.DictionaryOverhead)
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, MemoryUtils.DictionaryOverhead)
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var item = reader.ReadBytes(reader.ReadInt32());
                var value = reader.ReadBytes(reader.ReadInt32());
                hash.Add(item, value);

                this.UpdateSize(item, value);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public HashObject(Dictionary<byte[], byte[]> hash, long expiration, long size)
            : base(expiration, size)
        {
            this.hash = hash;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Hash;

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            int count = hash.Count;
            writer.Write(count);
            foreach (var kvp in hash)
            {
                writer.Write(kvp.Key.Length);
                writer.Write(kvp.Key);
                writer.Write(kvp.Value.Length);
                writer.Write(kvp.Value);
                count--;
            }
            Debug.Assert(count == 0);
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new HashObject(hash, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref ObjectInput input, ref SpanByteAndMemory output, out long sizeChange, out bool removeKey)
        {
            removeKey = false;

            fixed (byte* _output = output.SpanByte.AsSpan())
            {
                if (input.header.type != GarnetObjectType.Hash)
                {
                    //Indicates when there is an incorrect type 
                    output.Length = 0;
                    sizeChange = 0;
                    return true;
                }

                var previousSize = this.Size;
                switch (input.header.HashOp)
                {
                    case HashOperation.HSET:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HMSET:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HGET:
                        HashGet(ref input, ref output);
                        break;
                    case HashOperation.HMGET:
                        HashMultipleGet(ref input, ref output);
                        break;
                    case HashOperation.HGETALL:
                        HashGetAll(ref input, ref output);
                        break;
                    case HashOperation.HDEL:
                        HashDelete(ref input, _output);
                        break;
                    case HashOperation.HLEN:
                        HashLength(_output);
                        break;
                    case HashOperation.HSTRLEN:
                        HashStrLength(ref input, _output);
                        break;
                    case HashOperation.HEXISTS:
                        HashExists(ref input, _output);
                        break;
                    case HashOperation.HKEYS:
                        HashGetKeysOrValues(ref input, ref output);
                        break;
                    case HashOperation.HVALS:
                        HashGetKeysOrValues(ref input, ref output);
                        break;
                    case HashOperation.HINCRBY:
                        HashIncrement(ref input, ref output);
                        break;
                    case HashOperation.HINCRBYFLOAT:
                        HashIncrement(ref input, ref output);
                        break;
                    case HashOperation.HSETNX:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HRANDFIELD:
                        HashRandomField(ref input, ref output);
                        break;
                    case HashOperation.HSCAN:
                        if (ObjectUtils.ReadScanInput(ref input, ref output, out var cursorInput, out var pattern,
                                out var patternLength, out var limitCount, out bool isNoValue, out var error))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                                patternLength: patternLength, isNoValue);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output);
                        }
                        else
                        {
                            ObjectUtils.WriteScanError(error, ref output);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {input.header.HashOp} in HashObject.Operate");
                }

                sizeChange = this.Size - previousSize;
            }

            removeKey = hash.Count == 0;
            return true;
        }

        private void UpdateSize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool add = true)
        {
            var size = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.DictionaryOverhead);
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = new List<byte[]>();

            if (hash.Count < start)
            {
                cursor = 0;
                return;
            }

            // Hashset has key and value, so count is multiplied by 2
            count = isNoValue ? count : count * 2;
            int index = 0;
            foreach (var item in hash)
            {
                if (index < start)
                {
                    index++;
                    continue;
                }

                if (patternLength == 0)
                {
                    items.Add(item.Key);
                    if (!isNoValue)
                    {
                        items.Add(item.Value);
                    }
                }
                else
                {
                    fixed (byte* keyPtr = item.Key)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Key.Length))
                        {
                            items.Add(item.Key);
                            if (!isNoValue)
                            {
                                items.Add(item.Value);
                            }
                        }
                    }
                }

                cursor++;

                if (items.Count == count)
                    break;

            }

            // Indicates end of collection has been reached.
            if (cursor == hash.Count)
                cursor = 0;
        }
    }
}