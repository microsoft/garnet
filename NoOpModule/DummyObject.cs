// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace NoOpModule
{
    public class DummyObjectFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type)
            => new DummyObject(type);

        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new DummyObject(type, reader);
    }

    public class DummyObject : CustomObjectBase
    {
        public DummyObject(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
        }

        public DummyObject(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
        }

        public DummyObject(DummyObject obj)
            : base(obj)
        {
        }

        public override CustomObjectBase CloneObject() => new DummyObject(this);

        public override void SerializeObject(BinaryWriter writer)
        {
        }

        public override void Dispose() { }

        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false)
        {
            items = new();
            cursor = 0;
        }
    }
}