// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public class ObjectRecoveryTests3 : AllureTestBase
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
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ObjectRecoveryTest3(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values(1000)] int iterations,
            [Values] bool isAsync)
        {
            this.iterations = iterations;
            ObjectRecoveryTests3.Prepare(out IDevice log, out IDevice objlog, out var store, out MyContext context);

            var session1 = store.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            var tokens = Write(session1, context, store, checkpointType);
            Read(session1, context, false, iterations);
            session1.Dispose();

            _ = store.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            tokens.Add((iterations, token));
            Destroy(log, objlog, store);

            foreach (var item in tokens)
            {
                ObjectRecoveryTests3.Prepare(out log, out objlog, out store, out context);

                if (isAsync)
                    _ = await store.RecoverAsync(default, item.Item2);
                else
                    _ = store.Recover(default, item.Item2);

                var session2 = store.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
                Read(session2, context, false, item.Item1);
                session2.Dispose();

                Destroy(log, objlog, store);
            }
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

        private List<(int, Guid)> Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, ClassStoreFunctions, ClassAllocator> session, MyContext context,
                TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType)
        {
            var bContext = session.BasicContext;

            var tokens = new List<(int, Guid)>();
            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
                var value = new MyValue { value = i.ToString() };
                _ = bContext.Upsert(ref _key, ref value, context);

                if (i % 1000 == 0 && i > 0)
                {
                    _ = store.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                    tokens.Add((i, token));
                }
            }
            return tokens;
        }

        private static void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, ClassStoreFunctions, ClassAllocator> session, MyContext context, bool delete, int iter)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < iter; i++)
            {
                var key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
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
                var key = new MyKey { key = 1, name = "1" };
                var input = default(MyInput);
                var output = new MyOutput();
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