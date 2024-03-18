// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    public class MyKey : ITsavoriteEqualityComparer<MyKey>
    {
        public int key;

        public long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key);

        public bool Equals(ref MyKey k1, ref MyKey k2) => k1.key == k2.key;

        public override string ToString() => key.ToString();
    }

    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Deserialize(out MyKey obj) => obj = new MyKey { key = reader.ReadInt32() };

        public override void Serialize(ref MyKey obj) => writer.Write(obj.key);
    }

    public class MyValue : ITsavoriteEqualityComparer<MyValue>
    {
        public int value;

        public long GetHashCode64(ref MyValue k) => Utility.GetHashCode(k.value);

        public bool Equals(ref MyValue k1, ref MyValue k2) => k1.value == k2.value;

        public override string ToString() => value.ToString();
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Deserialize(out MyValue obj) => obj = new MyValue { value = reader.ReadInt32() };

        public override void Serialize(ref MyValue obj) => writer.Write(obj.value);
    }

    public class MyInput
    {
        public int value;

        public override string ToString() => value.ToString();
    }

    public class MyOutput
    {
        public MyValue value;

        public override string ToString() => value.ToString();
    }

    public class MyFunctions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, Empty>
    {
        public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = new MyValue { value = input.value };
            return true;
        }

        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
            return true;
        }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (dst == default)
                dst = new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            dst.value = src.value;
            return true;
        }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(output.value.value, key.key);
        }

        public override void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
        }

        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            if (dst == default)
                dst = new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }
    }

    public class MyFunctions2 : FunctionsBase<MyValue, MyValue, MyInput, MyOutput, Empty>
    {
        public override bool InitialUpdater(ref MyValue key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = new MyValue { value = input.value };
            return true;
        }

        public override bool InPlaceUpdater(ref MyValue key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate(ref MyValue key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref MyValue key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
            return true;
        }

        public override bool ConcurrentReader(ref MyValue key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (dst == default)
                dst = new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyValue key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            dst.value = src.value;
            return true;
        }

        public override void ReadCompletionCallback(ref MyValue key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.value, output.value.value);
        }

        public override void RMWCompletionCallback(ref MyValue key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
        }

        public override bool SingleReader(ref MyValue key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            if (dst == default)
                dst = new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref MyValue key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }
    }

    public class MyFunctionsDelete : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, int>
    {
        public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = new MyValue { value = input.value };
            return true;
        }

        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
            return true;
        }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst ??= new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key.key, output.value.value);
            }
            else if (ctx == 1)
            {
                Assert.IsFalse(status.Found);
            }
        }

        public override void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                Assert.IsTrue(status.Found);
                Assert.IsTrue(status.Record.CopyUpdated);
            }
            else if (ctx == 1)
                Assert.IsFalse(status.Found);
        }

        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            dst ??= new MyOutput();
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }
    }

    public class MixedFunctions : FunctionsBase<int, MyValue, MyInput, MyOutput, Empty>
    {
        public override bool InitialUpdater(ref int key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = new MyValue { value = input.value };
            return true;
        }

        public override bool InPlaceUpdater(ref int key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value += input.value;
            return true;
        }

        public override bool NeedCopyUpdate(ref int key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref int key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
            return true;
        }

        public override bool ConcurrentReader(ref int key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref int key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo updateInfo, ref RecordInfo recordInfo)
        {
            dst.value = src.value;
            return true;
        }

        public override bool SingleReader(ref int key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref int key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo updateInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }
    }

    public class MyLargeValue
    {
        public byte[] value;

        public MyLargeValue()
        {
        }

        public MyLargeValue(int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
            {
                value[i] = (byte)(size + i);
            }
        }
    }

    public class MyLargeValueSerializer : BinaryObjectSerializer<MyLargeValue>
    {
        public override void Deserialize(out MyLargeValue obj)
        {
            obj = new MyLargeValue();
            int size = reader.ReadInt32();
            obj.value = reader.ReadBytes(size);
        }

        public override void Serialize(ref MyLargeValue obj)
        {
            writer.Write(obj.value.Length);
            writer.Write(obj.value);
        }
    }

    public class MyLargeOutput
    {
        public MyLargeValue value;
    }

    public class MyLargeFunctions : FunctionsBase<MyKey, MyLargeValue, MyInput, MyLargeOutput, Empty>
    {
        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyLargeOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                Assert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
            }
        }

        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyLargeValue value, ref MyLargeOutput dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyLargeValue value, ref MyLargeOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyLargeValue src, ref MyLargeValue dst, ref MyLargeOutput output, ref UpsertInfo updateInfo, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }

        public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyLargeValue src, ref MyLargeValue dst, ref MyLargeOutput output, ref UpsertInfo updateInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }
    }
}