// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;

    [AllureNUnit]
    [TestFixture]
    public class ObjectRecoveryTests2 : AllureTestBase
    {
        int iterations;

        [SetUp]
        public void Setup()
        {
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask ObjectRecoveryTest2(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Range(300, 700, 300)] int iterations,
            [Values] bool isAsync)
        {
            this.iterations = iterations;
            Prepare(out IDevice log, out IDevice objlog, out var store, out MyContext context);

            var session1 = store.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            Write(session1, context, store, checkpointType);
            Read(session1, context, false);
            session1.Dispose();

            _ = store.TryInitiateFullCheckpoint(out _, checkpointType);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            Destroy(log, objlog, store);

            Prepare(out log, out objlog, out store, out context);

            if (isAsync)
                _ = await store.RecoverAsync();
            else
                _ = store.Recover();

            var session2 = store.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            Read(session2, context, true);
            session2.Dispose();

            Destroy(log, objlog, store);
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store, out MyContext context)
        {
            log = Devices.CreateLogDevice(Path.Combine(TestUtils.MethodTestDir, "RecoverTests.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(TestUtils.MethodTestDir, "RecoverTests_HEAP.log"));
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 12,
                MemorySize = 1L << 12,
                PageSize = 1L << 9,
                CheckpointDir = Path.Combine(TestUtils.MethodTestDir, "check-points")
            }, StoreFunctions<MyKey, MyValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
             );
            context = new MyContext();
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store)
        {
            // Dispose Tsavorite instance and log
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }

        private void Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, ClassStoreFunctions, ClassAllocator> session, MyContext context,
                TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = i.ToString() };
                var value = new MyValue { value = i.ToString() };
                _ = bContext.Upsert(ref _key, ref value, context);

                if (i % 100 == 0)
                {
                    _ = store.TryInitiateFullCheckpoint(out _, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        private void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, ClassStoreFunctions, ClassAllocator> session, MyContext context, bool delete)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < iterations; i++)
            {
                MyKey key = new() { key = i, name = i.ToString() };
                MyInput input = default;
                MyOutput g1 = new();
                var status = bContext.Read(ref key, ref input, ref g1, context);

                if (status.IsPending)
                {
                    _ = bContext.CompletePending(true);
                    context.FinalizeRead(ref status, ref g1);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(i.ToString(), g1.value.value);
            }

            if (delete)
            {
                MyKey key = new() { key = 1, name = "1" };
                MyInput input = default;
                MyOutput output = new();
                _ = bContext.Delete(ref key, context);
                var status = bContext.Read(ref key, ref input, ref output, context);

                if (status.IsPending)
                {
                    _ = bContext.CompletePending(true);
                    context.FinalizeRead(ref status, ref output);
                }

                ClassicAssert.IsFalse(status.Found);
            }
        }
    }

    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Serialize(ref MyKey key)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key.name);
            writer.Write(4 + bytes.Length);
            writer.Write(key.key);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyKey key)
        {
            key = new MyKey();
            var size = reader.ReadInt32();
            key.key = reader.ReadInt32();
            var bytes = new byte[size - 4];
            _ = reader.Read(bytes, 0, size - 4);
            key.name = System.Text.Encoding.UTF8.GetString(bytes);

        }
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Serialize(ref MyValue value)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(value.value);
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyValue value)
        {
            value = new MyValue();
            var size = reader.ReadInt32();
            var bytes = new byte[size];
            _ = reader.Read(bytes, 0, size);
            value.value = System.Text.Encoding.UTF8.GetString(bytes);
        }
    }

    public class MyKey
    {
        public int key;
        public string name;

        public struct Comparer : IKeyComparer<MyKey>
        {
            public readonly long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key);
            public readonly bool Equals(ref MyKey key1, ref MyKey key2) => key1.key == key2.key && key1.name == key2.name;
        }
    }

    public class MyValue { public string value; }
    public class MyInput { public string value; }
    public class MyOutput { public MyValue value; }

    public class MyContext
    {
        private Status _status;
        private MyOutput _g1;

        internal void Populate(ref Status status, ref MyOutput g1)
        {
            _status = status;
            _g1 = g1;
        }
        internal void FinalizeRead(ref Status status, ref MyOutput g1)
        {
            status = _status;
            g1 = _g1;
        }
    }


    public class MyFunctions : SessionFunctionsBase<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { value.value = input.value; return true; }
        public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;
        public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { newValue = oldValue; return true; }
        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (value.value.Length < input.value.Length)
                return false;
            value.value = input.value;
            return true;
        }


        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) { dst = src; return true; }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            if (src == null)
                return false;

            if (dst.value.Length != src.value.Length)
                return false;

            dst = src;
            return true;
        }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status, RecordMetadata recordMetadata) => ctx.Populate(ref status, ref output);
    }
}