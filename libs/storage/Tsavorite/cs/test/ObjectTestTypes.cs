// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
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

    public class TestObjectValue : HeapObjectBase
    {
        public int value;

        public override string ToString() => value.ToString();

        public override void Dispose() { }

        public override HeapObjectBase Clone() => throw new NotImplementedException();
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public TestObjectValue()
        {
            HeapMemorySize = sizeof(int);
            SerializedSize = HeapMemorySize;
            SerializedSizeIsExact = true;
        }

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
        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, in sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, in sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue, in sizeInfo))
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

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = (TestObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, in sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }

    public class TestObjectFunctionsDelete : SessionFunctionsBase<TestObjectInput, TestObjectOutput, int>
    {
        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueObject(new TestObjectValue { value = input.value }, in sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObject(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, in sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, in sizeInfo);

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

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
        {
            output.value = (TestObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue, in sizeInfo);

        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }

    public class TestLargeObjectValue : HeapObjectBase
    {
        public byte[] value;

        public override HeapObjectBase Clone() => throw new NotImplementedException();
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public override void Dispose() { }

        public TestLargeObjectValue() { }

        public TestLargeObjectValue(int size, bool serializedSizeIsExact)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
                value[i] = (byte)(size + i);
            SetSizes(serializedSizeIsExact);
        }

        private void SetSizes(bool serializedSizeIsExact = true)
        {
            SerializedSize = sizeof(int) + value.Length;
            HeapMemorySize = SerializedSize + 24; // TODO: ByteArrayOverhead
            SerializedSizeIsExact = serializedSizeIsExact;
        }

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj)
            {
                var value = new TestLargeObjectValue();
                obj = value;
                int size = reader.ReadInt32();
                value.value = reader.ReadBytes(size);
                value.SetSizes();
            }

            public override void Serialize(IHeapObject obj)
            {
                var value = (TestLargeObjectValue)obj;
                writer.Write(value.value.Length);
                writer.Write(value.value);
            }
        }
    }

    public struct TestLargeObjectInput
    {
        public int value;

        public TestValueStyle wantValueStyle;

        public int expectedSpanLength;

        public override readonly string ToString() => $"value {value}, wantValStyle {wantValueStyle}";
    }

    public class TestLargeObjectOutput
    {
        public TestLargeObjectValue value;
    }

    public class TestLargeObjectFunctions : SessionFunctionsBase<TestLargeObjectInput, TestLargeObjectOutput, Empty>
    {
        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestLargeObjectInput input, ref TestLargeObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                ClassicAssert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
            }
        }

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestLargeObjectInput input, ref TestLargeObjectOutput output, ref ReadInfo readInfo)
        {
            switch (input.wantValueStyle)
            {
                case TestValueStyle.None:
                    Assert.Fail("wantValueStyle should not be None");
                    break;
                case TestValueStyle.Inline:
                    Assert.That(srcLogRecord.Info.ValueIsInline, Is.True);
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(input.expectedSpanLength));
                    break;
                case TestValueStyle.Overflow:
                    Assert.That(srcLogRecord.Info.ValueIsOverflow, Is.True);
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(input.expectedSpanLength));
                    break;
                case TestValueStyle.Object:
                    Assert.That(srcLogRecord.Info.ValueIsObject, Is.True);
                    break;
            }
            output.value = (TestLargeObjectValue)srcLogRecord.ValueObject;
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestLargeObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = (TestLargeObjectValue)logRecord.ValueObject;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestLargeObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.value = (TestLargeObjectValue)logRecord.ValueObject;
            return true;
        }

        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestLargeObjectInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestLargeObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestLargeObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TestLargeObjectInput input)
            => new() { KeySize = key.Length, ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length, ValueIsObject = inputLogRecord.Info.ValueIsObject,
                       HasETag = inputLogRecord.Info.HasETag, HasExpiration = inputLogRecord.Info.HasExpiration};
    }
}
