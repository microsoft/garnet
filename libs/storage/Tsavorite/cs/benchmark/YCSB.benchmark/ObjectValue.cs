// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System;
using System.IO;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public class ObjectValue : HeapObjectBase
    {
        public long value;

        public override string ToString() => value.ToString();

        public override void Dispose() { }

        public override HeapObjectBase Clone() => throw new NotImplementedException();
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public ObjectValue()
        {
            HeapMemorySize = sizeof(long);
        }

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new ObjectValue { value = reader.ReadInt32() };

            public override void Serialize(IHeapObject obj) => writer.Write(((ObjectValue)obj).value);
        }
    }
}