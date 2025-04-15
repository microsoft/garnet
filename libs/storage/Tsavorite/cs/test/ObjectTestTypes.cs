// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    public enum TestValueStyle : byte { None, Inline, Overflow, Object };

    public struct TestObjectKey
    {
        public int key;

        public override string ToString() => key.ToString();

        public struct Comparer : IKeyComparer
        {
            public readonly long GetHashCode64(ReadOnlySpan<byte> key) => Utility.GetHashCode(key.AsRef<TestObjectKey>().key);

            public readonly bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.AsRef<TestObjectKey>().key == k2.AsRef<TestObjectKey>().key;
        }
    }

    public class TestObjectValue : IHeapObject
    {
        public int value;

        public long Size { get => sizeof(int); set => throw new System.NotImplementedException("TestValueObject.Size.set"); }

        public void Dispose() { }

        public override string ToString() => value.ToString();

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TestObjectValue { value = reader.ReadInt32() };

            public override void Serialize(IHeapObject obj) => writer.Write(((TestObjectValue)obj).value);
        }
    }

    public struct TestObjectInput
    {
        public int value;

        public TestValueStyle wantValueStyle;

        public override readonly string ToString() => $"value {value}, wantValStyle {wantValueStyle}";
    }

    public struct TestObjectOutput
    {
        public TestObjectValue value;

        public TestValueStyle srcValueStyle;
        public TestValueStyle destValueStyle;

        public override readonly string ToString() => $"value {value}, srcValStyle {srcValueStyle}, destValStyle {destValueStyle}";
    }

    public class TestObjectFunctions : SessionFunctionsBase<TestObjectInput, TestObjectOutput, Empty>
    {
        public override bool InitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, ref sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, ref sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue, ref sizeInfo))
                return false;
            output.value = (TestObjectValue)logRecord.ValueObject;
            return true;
        }

        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(output.value.value, srcLogRecord.Key.AsRef<TestObjectKey>().key);
        }

        public override void RMWCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = (TestObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }

    public class TestObjectFunctionsDelete : SessionFunctionsBase<TestObjectInput, TestObjectOutput, int>
    {
        public override bool InitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, ref sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, ref sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, int ctx, Status status, RecordMetadata recordMetadata)
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

        public override void RMWCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.IsTrue(status.Record.CopyUpdated);
            }
            else if (ctx == 1)
                ClassicAssert.IsFalse(status.Found);
        }

        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = (TestObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, ref sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }

    public class TestLargeObjectValue : IHeapObject
    {
        public byte[] value;

        public long Size { get => sizeof(int) + value.Length; set => throw new System.NotImplementedException("TestValueObject.Size.set"); }

        public void Dispose() { }

        public TestLargeObjectValue()
        {
        }

        public TestLargeObjectValue(int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
                value[i] = (byte)(size + i);
        }

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj)
            {
                var value = new TestLargeObjectValue();
                obj = value;
                int size = reader.ReadInt32();
                value.value = reader.ReadBytes(size);
            }

            public override void Serialize(IHeapObject obj)
            {
                var value = (TestLargeObjectValue)obj;
                writer.Write(value.value.Length);
                writer.Write(value.value);
            }
        }
    }

    public class TestLargeObjectOutput
    {
        public TestLargeObjectValue value;
    }

    public class TestLargeObjectFunctions : SessionFunctionsBase<TestObjectInput, TestLargeObjectOutput, Empty>
    {
        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestLargeObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                ClassicAssert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
            }
        }

        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestLargeObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = (TestLargeObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = (TestLargeObjectValue)logRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = (TestLargeObjectValue)logRecord.ValueObject;
            return true;
        }

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
            => new() { KeyDataSize = key.Length, ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }
}
