// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace NoOpModule
{
    /// <summary>
    /// Represents a factory for creating instances of <see cref="DummyObject"/>.
    /// </summary>
    public class DummyObjectFactory : CustomObjectFactory
    {
        /// <inheritdoc />
        public override CustomObjectBase Create(byte type)
            => new DummyObject(type);

        /// <inheritdoc />
        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new DummyObject(type, reader);
    }

    /// <summary>
    /// Represents a dummy Garnet object
    /// </summary>
    public class DummyObject : CustomObjectBase
    {
        /// <inheritdoc />
        public DummyObject(byte type)
            : base(type, MemoryUtils.DictionaryOverhead)
        {
        }

        /// <inheritdoc />
        public DummyObject(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
        }

        /// <inheritdoc />
        public DummyObject(DummyObject obj)
            : base(obj)
        {
        }

        /// <inheritdoc />
        public override CustomObjectBase CloneObject() => new DummyObject(this);

        /// <inheritdoc />
        public override void SerializeObject(BinaryWriter writer)
        {
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10,
            byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            items = [];
            cursor = 0;
        }
    }
}