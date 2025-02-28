// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    public struct TestObjectKey
    {
        public int key;

        public override string ToString() => key.ToString();

        public struct Comparer : IKeyComparer
        {
            public readonly long GetHashCode64(SpanByte key) => Utility.GetHashCode(key.AsRef<TestObjectKey>().key);

            public readonly bool Equals(SpanByte k1, SpanByte k2) => k1.AsRef<TestObjectKey>().key == k2.AsRef<TestObjectKey>().key;
        }
    }

    public class TestObjectValue : IHeapObject
    {
        public int value;

        public long Size { get => sizeof(int); set => throw new System.NotImplementedException("TestValueObject.Size.set"); }

        public void Dispose() { }

        public override string ToString() => value.ToString();

        public class Serializer : BinaryObjectSerializer<TestObjectValue>
        {
            public override void Deserialize(out TestObjectValue obj) => obj = new TestObjectValue { value = reader.ReadInt32() };

            public override void Serialize(ref TestObjectValue obj) => writer.Write(obj.value);
        }
    }

    public struct TestObjectInput
    {
        public int value;

        public bool wantInlineValue;

        public override readonly string ToString() => value.ToString();
    }

    public struct TestObjectOutput
    {
        public TestObjectValue value;

        public bool srcWasInlineValue;
        public bool destIsInlineValue;

        public override string ToString() => value.ToString();
    }

    public class TestObjectFunctions : SessionFunctionsBase<TestObjectValue, TestObjectInput, TestObjectOutput, Empty>
    {
        public override bool InitialUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, ref sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueObject.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TestObjectValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = srcLogRecord.ValueObject.value + input.value }, ref sizeInfo);

        public override bool ConcurrentReader(ref LogRecord<TestObjectValue> logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = logRecord.ValueObject;
            return true;
        }

        public override bool ConcurrentWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue, ref sizeInfo))
                return false;
            output.value = logRecord.ValueObject;
            return true;
        }

        public override void ReadCompletionCallback(ref DiskLogRecord<TestObjectValue> srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(output.value.value, srcLogRecord.Key.AsRef<TestObjectKey>().key);
        }

        public override void RMWCompletionCallback(ref DiskLogRecord<TestObjectValue> srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueObject;
            return true;
        }

        public override bool SingleWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyTotalSize = srcLogRecord.Key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TestObjectValue value, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
    }

    public class TestObjectFunctionsDelete : SessionFunctionsBase<TestObjectValue, TestObjectInput, TestObjectOutput, int>
    {
        public override bool InitialUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, ref sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueObject.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TestObjectValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = srcLogRecord.ValueObject.value + input.value }, ref sizeInfo);

        public override bool ConcurrentReader(ref LogRecord<TestObjectValue> srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueObject;
            return true;
        }

        public override bool ConcurrentWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override void ReadCompletionCallback(ref DiskLogRecord<TestObjectValue> srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(srcLogRecord.Key.AsRef<TestObjectKey>().key, output.value.value);
            }
            else if (ctx == 1)
            {
                ClassicAssert.IsFalse(status.Found);
            }
        }

        public override void RMWCompletionCallback(ref DiskLogRecord<TestObjectValue> srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.IsTrue(status.Record.CopyUpdated);
            }
            else if (ctx == 1)
                ClassicAssert.IsFalse(status.Found);
        }

        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueObject;
            return true;
        }

        public override bool SingleWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyTotalSize = srcLogRecord.Key.TotalSize, ValueTotalSize = ObjectIdMap.ObjectIdSize };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = ObjectIdMap.ObjectIdSize };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TestObjectValue value, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = ObjectIdMap.ObjectIdSize };
    }

    public class TestLargeObjectValue
    {
        public byte[] value;

        public TestLargeObjectValue()
        {
        }

        public TestLargeObjectValue(int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
                value[i] = (byte)(size + i);
        }

        public class Serializer : BinaryObjectSerializer<TestLargeObjectValue>
        {
            public override void Deserialize(out TestLargeObjectValue obj)
            {
                obj = new TestLargeObjectValue();
                int size = reader.ReadInt32();
                obj.value = reader.ReadBytes(size);
            }

            public override void Serialize(ref TestLargeObjectValue obj)
            {
                writer.Write(obj.value.Length);
                writer.Write(obj.value);
            }
        }
    }

    public class TestLargeObjectOutput
    {
        public TestLargeObjectValue value;
    }

    public class TestLargeObjectFunctions : SessionFunctionsBase<TestLargeObjectValue, TestObjectInput, TestLargeObjectOutput, Empty>
    {
        public override void ReadCompletionCallback(ref DiskLogRecord<TestLargeObjectValue> srcLogRecord, ref TestObjectInput input, ref TestLargeObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                ClassicAssert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
            }
        }

        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestLargeObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueObject;
            return true;
        }

        public override bool ConcurrentReader(ref LogRecord<TestLargeObjectValue> srcLogRecord, ref TestObjectInput input, ref TestLargeObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueObject;
            return true;
        }

        public override bool ConcurrentWriter(ref LogRecord<TestLargeObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestLargeObjectValue srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = logRecord.ValueObject;
            return true;
        }

        public override bool SingleWriter(ref LogRecord<TestLargeObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestLargeObjectValue srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo, WriteReason reason)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = logRecord.ValueObject;
            return true;
        }

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyTotalSize = srcLogRecord.Key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TestLargeObjectValue value, ref TestObjectInput input)
            => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = RecordFieldInfo.ValueObjectIdSize };
    }
}
