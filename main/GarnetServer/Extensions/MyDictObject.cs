// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    class MyDictFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type)
            => new MyDict(type);

        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new MyDict(type, reader);
    }

    class MyDict : CustomObjectBase
    {
        readonly Dictionary<byte[], byte[]> dict;

        public MyDict(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
            dict = new(new ByteArrayComparer());
        }

        public MyDict(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
            dict = new(new ByteArrayComparer());

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var key = reader.ReadBytes(reader.ReadInt32());
                var value = reader.ReadBytes(reader.ReadInt32());
                dict.Add(key, value);

                UpdateSize(key, value);
            }
        }

        public MyDict(MyDict obj)
            : base(obj)
        {
            dict = obj.dict;
        }

        public override CustomObjectBase CloneObject() => new MyDict(this);

        public override void SerializeObject(BinaryWriter writer)
        {
            writer.Write(dict.Count);
            foreach (var kvp in dict)
            {
                writer.Write(kvp.Key.Length);
                writer.Write(kvp.Key);
                writer.Write(kvp.Value.Length);
                writer.Write(kvp.Value);
            }
        }

        public override void Operate(byte subCommand, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output)
        {
            switch (subCommand)
            {
                case 0: // MYDICTSET
                    {
                        int offset = 0;
                        var key = GetNextArg(input, ref offset).ToArray();
                        var value = GetNextArg(input, ref offset).ToArray();

                        dict[key] = value;
                        UpdateSize(key, value);
                        break; // +OK is sent as response, by default
                    }
                case 1: // MYDICTGET
                    {
                        var key = GetFirstArg(input);
                        if (dict.TryGetValue(key.ToArray(), out var result))
                            WriteBulkString(ref output, result);
                        else
                            WriteNullBulkString(ref output);
                        break;
                    }
                default:
                    WriteError(ref output, "Unexpected command");
                    break;
            }
        }

        public override void Dispose() { }


        /// <summary>
        /// Returns the items from this object using a cursor to indicate the start of the scan,
        /// a pattern to filter out the items to return, and a count to indicate the number of items to return.
        /// </summary>
        /// <param name="start"></param>
        /// <param name="items"></param>
        /// <param name="cursor"></param>
        /// <param name="count"></param>
        /// <param name="pattern"></param>
        /// <param name="patternLength"></param>
        /// <returns></returns>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0)
        {
            cursor = start;
            items = new();
            int index = 0;

            if (dict.Count < start)
            {
                cursor = 0;
                return;
            }

            foreach (var item in dict)
            {
                if (index < start)
                {
                    index++;
                    continue;
                }

                bool addToList = false;
                if (patternLength == 0)
                {
                    items.Add(item.Key);
                    addToList = true;
                }
                else
                {
                    fixed (byte* keyPtr = item.Key)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Key.Length))
                        {
                            items.Add(item.Key);
                            addToList = true;
                        }
                    }
                }

                if (addToList)
                    items.Add(item.Value);

                cursor++;

                // Each item is a pair in the Dictionary but two items in the result List
                if (items.Count == (count * 2))
                    break;
            }

            // Indicates end of collection has been reached.
            if (cursor == dict.Count)
                cursor = 0;
        }

        private void UpdateSize(byte[] key, byte[] value, bool add = true)
        {
            var size = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.DictionaryOverhead);
        }
    }
}