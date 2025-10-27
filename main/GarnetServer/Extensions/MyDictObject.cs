// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
            : base(type, MemoryUtils.DictionaryOverhead)
        {
            dict = new(ByteArrayComparer.Instance);
        }

        public MyDict(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
            dict = new(ByteArrayComparer.Instance);

            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
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

        public override void Dispose() { }

        /// <summary>
        /// Returns the items from this object using a cursor to indicate the start of the scan,
        /// a pattern to filter out the items to return, and a count to indicate the number of items to return.
        /// </summary>
        /// <returns></returns>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = [];
            var index = 0;

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

                var addToList = false;
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

        public bool Set(byte[] key, byte[] value)
        {
            if (dict.TryGetValue(key, out var oldValue))
                UpdateSize(key, oldValue, false);

            dict[key] = value;
            UpdateSize(key, value);
            return true;
        }

        private void UpdateSize(byte[] key, byte[] value, bool add = true)
        {
            var memorySize = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;

            if (add)
                HeapMemorySize += memorySize;
            else
            {
                HeapMemorySize -= memorySize;
                Debug.Assert(HeapMemorySize >= MemoryUtils.DictionaryOverhead);
            }
        }

        public bool TryGetValue(byte[] key, [MaybeNullWhen(false)] out byte[] value)
            => dict.TryGetValue(key, out value);
    }
}