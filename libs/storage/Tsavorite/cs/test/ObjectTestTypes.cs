// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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

        public override HeapObjectBase Clone() => new TestObjectValue() { value = value };
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public TestObjectValue()
        {
            HeapMemorySize = sizeof(int);
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
            => logRecord.TrySetValueObjectAndPrepareOptionals(new TestObjectValue { value = input.value }, in sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObjectAndPrepareOptionals(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, in sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo))
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
            => logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);

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
            => logRecord.TrySetValueObjectAndPrepareOptionals(new TestObjectValue { value = input.value }, in sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
        {
            ((TestObjectValue)logRecord.ValueObject).value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueObjectAndPrepareOptionals(new TestObjectValue { value = ((TestObjectValue)srcLogRecord.ValueObject).value + input.value }, in sizeInfo);

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);

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
            => logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);

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
                Assert.That(size, Is.Not.EqualTo(0));

                value.value = reader.ReadBytes(size);
                Assert.That(value.value.Length, Is.EqualTo(size));
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
        public TestLargeObjectValue valueObject;
        public byte[] valueArray;
    }

    public class TestLargeObjectFunctions : SessionFunctionsBase<TestLargeObjectInput, TestLargeObjectOutput, Empty>
    {
        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestLargeObjectInput input, ref TestLargeObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.That(status.Found, Is.True);
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
                    output.valueArray = srcLogRecord.ValueSpan.ToArray();
                    break;
                case TestValueStyle.Overflow:
                    Assert.That(srcLogRecord.Info.ValueIsOverflow, Is.True);
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(input.expectedSpanLength));
                    output.valueArray = srcLogRecord.ValueSpan.ToArray();
                    break;
                case TestValueStyle.Object:
                    Assert.That(srcLogRecord.Info.ValueIsObject, Is.True);
                    break;
            }
            output.valueObject = srcLogRecord.Info.ValueIsObject ? (TestLargeObjectValue)srcLogRecord.ValueObject : default;
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestLargeObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            output.valueObject = logRecord.Info.ValueIsObject ? (TestLargeObjectValue)logRecord.ValueObject : default;
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestLargeObjectInput input, IHeapObject srcValue, ref TestLargeObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            if (output is not null)
                output.valueObject = logRecord.Info.ValueIsObject ? (TestLargeObjectValue)logRecord.ValueObject : default;
            return true;
        }

        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestLargeObjectInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestLargeObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestLargeObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TestLargeObjectInput input)
            => new()
            {
                KeySize = key.Length,
                ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = inputLogRecord.Info.ValueIsObject,
                HasETag = inputLogRecord.Info.HasETag,
                HasExpiration = inputLogRecord.Info.HasExpiration
            };
    }

    public class TestMultiListObjectValue : HeapObjectBase
    {
        public List<long>[] lists;
        int objectIndex;

        public override HeapObjectBase Clone() => throw new NotImplementedException();
        public override void DoSerialize(BinaryWriter writer) => throw new NotImplementedException();
        public override void WriteType(BinaryWriter writer, bool isNull) => throw new NotImplementedException();

        public override void Dispose() { }

        public TestMultiListObjectValue() { }

        public static long CreateValue(int objectIndex, int listIndex, int itemIndex) => (long)(((ulong)objectIndex << 48) + ((ulong)listIndex << 32) + (ulong)itemIndex);

        public TestMultiListObjectValue(int objectIndex, int numLists, int numItems, Random rng = null)
        {
            this.objectIndex = objectIndex;
            lists = new List<long>[numLists];
            for (int ii = 0; ii < numLists; ii++)
            {
                var numElements = rng is not null ? 1 + rng.Next(numItems) : numItems;
                lists[ii] = new List<long>(numElements);
                for (int jj = 0; jj < numElements; jj++)
                    lists[ii].Add(CreateValue(objectIndex, ii, jj));
            }
        }

        public class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj)
            {
                var value = new TestMultiListObjectValue();
                obj = value;

                value.objectIndex = reader.ReadInt32();
                int numLists = reader.ReadInt32();
                Assert.That(numLists, Is.Not.EqualTo(0));
                value.lists = new List<long>[numLists];
                for (var ii = 0; ii < numLists; ii++)
                {
                    int numItems = reader.ReadInt32();
                    Assert.That(numItems, Is.Not.EqualTo(0));
                    var list = new List<long>(numItems);
                    value.lists[ii] = list;
                    for (int jj = 0; jj < numItems; jj++)
                        list.Add(reader.ReadInt64());
                }
            }

            public override void Serialize(IHeapObject obj)
            {
                var value = (TestMultiListObjectValue)obj;
                writer.Write(value.objectIndex);
                writer.Write(value.lists.Length);
                var numLists = value.lists.Length;
                Assert.That(numLists, Is.Not.EqualTo(0));
                for (var ii = 0; ii < numLists; ii++)
                {
                    var list = value.lists[ii];
                    var numItems = list.Count;
                    Assert.That(numItems, Is.Not.EqualTo(0));
                    writer.Write(numItems);
                    for (int jj = 0; jj < numItems; jj++)
                        writer.Write(list[jj]);
                }
            }
        }
    }

    public struct TestMultiListObjectInput
    {
        public int objectIndex, listIndex, itemIndex;
        public long updateValue;

        public readonly long ExpectedOutputValue => TestMultiListObjectValue.CreateValue(objectIndex, listIndex, itemIndex);

        public override readonly string ToString() => $"objectIndex {objectIndex}, listIndex {listIndex}, itemIndex {itemIndex}, updateValue {updateValue}";
    }

    public class TestMultiListObjectOutput
    {
        public TestMultiListObjectValue valueObject;
        public long oldValue, newValue;

        public override string ToString() => $"oldValue {oldValue}, newValue {newValue}";
    }

    public class TestMultiListObjectFunctions : SessionFunctionsBase<TestMultiListObjectInput, TestMultiListObjectOutput, Empty>
    {
        public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestMultiListObjectInput input, ref TestMultiListObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.That(status.Found, Is.True);
        }

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestMultiListObjectInput input, ref TestMultiListObjectOutput output, ref ReadInfo readInfo)
        {
            output.valueObject = (TestMultiListObjectValue)srcLogRecord.ValueObject;
            output.oldValue = output.valueObject.lists[input.listIndex][input.itemIndex];
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestMultiListObjectInput input, IHeapObject srcValue, ref TestMultiListObjectOutput output, ref UpsertInfo updateInfo)
        {
            output.valueObject = (TestMultiListObjectValue)logRecord.ValueObject;
            output.oldValue = output.valueObject.lists[input.listIndex][input.itemIndex];
            output.valueObject.lists[input.listIndex][input.itemIndex] = input.updateValue;
            output.newValue = output.valueObject.lists[input.listIndex][input.itemIndex];
            return true;
        }

        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestMultiListObjectInput input, IHeapObject srcValue, ref TestMultiListObjectOutput output, ref UpsertInfo updateInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue)) // We should always be non-inline
                return false;
            if (output is not null)
                output.valueObject = (TestMultiListObjectValue)srcValue;
            return true;
        }

        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestMultiListObjectInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestMultiListObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestMultiListObjectInput input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        public override RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TestMultiListObjectInput input)
            => new()
            {
                KeySize = key.Length,
                ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = inputLogRecord.Info.ValueIsObject,
                HasETag = inputLogRecord.Info.HasETag,
                HasExpiration = inputLogRecord.Info.HasExpiration
            };
    }
}