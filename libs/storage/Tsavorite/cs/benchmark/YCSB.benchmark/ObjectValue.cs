// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public class ObjectValue : IHeapObject
    {
        public long value;

        public long Size { get => sizeof(int); set => throw new System.NotImplementedException("TestValueObject.Size.set"); }

        public void Dispose() { }

        public override string ToString() => value.ToString();

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new ObjectValue { value = reader.ReadInt32() };

            public override void Serialize(IHeapObject obj) => writer.Write(((ObjectValue)obj).value);
        }
    }
}