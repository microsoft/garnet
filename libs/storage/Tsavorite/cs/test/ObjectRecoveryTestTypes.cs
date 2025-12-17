// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.recovery.objects
{
    public class NumClicksObj : HeapObjectBase
    {
        public long numClicks;

        public override string ToString() => numClicks.ToString();

        public override void Dispose() { }

        public override HeapObjectBase Clone() => throw new NotImplementedException();
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public NumClicksObj()
        {
            HeapMemorySize = sizeof(long);
        }

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new NumClicksObj { numClicks = reader.ReadInt64() };

            public override void Serialize(IHeapObject obj) => writer.Write(((NumClicksObj)obj).numClicks);
        }
    }

    public class Input
    {
        public AdId adId;
        public NumClicksObj numClicks;
    }

    public class Output
    {
        public NumClicksObj value;
    }

    public class Functions : SessionFunctionsBase<Input, Output, Empty>
    {
        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
        {
            output.value = (NumClicksObj)srcLogRecord.ValueObject;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(input.numClicks);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Add(ref ((NumClicksObj)logRecord.ValueObject).numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new NumClicksObj { numClicks = ((NumClicksObj)srcLogRecord.ValueObject).numClicks + input.numClicks.numClicks });

        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref Input input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }
}