// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;

    [TestFixture]
    public class ObjectRecoveryTests2
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
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
}

#endif // LOGRECORD_TODO
