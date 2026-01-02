// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [TestFixture]
    public class ObjectRecoveryTests3
    {
        int iterations;

        [SetUp]
        public void Setup()
        {
            RecreateDirectory(MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ObjectRecoveryTest3(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values(1000)] int iterations,
            [Values] bool isAsync)
        {
            this.iterations = iterations;
            Prepare(out IDevice log, out IDevice objlog, out var store);

            var session1 = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var tokens = Write(session1, store, checkpointType);
            Read(session1, false, iterations);
            session1.Dispose();

            _ = store.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            tokens.Add((iterations, token));
            Destroy(log, objlog, store);

            foreach (var item in tokens)
            {
                Prepare(out log, out objlog, out store);

                if (isAsync)
                    _ = await store.RecoverAsync(default, item.Item2);
                else
                    _ = store.Recover(default, item.Item2);

                var session2 = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                Read(session2, false, item.Item1);
                session2.Dispose();

                Destroy(log, objlog, store);
            }
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "RecoverTests.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "RecoverTests_HEAP.log"));
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 12,
                MemorySize = 1L << 12,
                PageSize = 1L << 9,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            // Dispose Tsavorite instance and log
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }

        private List<(int, Guid)> Write(ClientSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
                TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType)
        {
            var bContext = session.BasicContext;

            var tokens = new List<(int, Guid)>();
            for (int i = 0; i < iterations; i++)
            {
                var _key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref _key), value);

                if (i % 1000 == 0 && i > 0)
                {
                    _ = store.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                    tokens.Add((i, token));
                }
            }
            return tokens;
        }

        private static void Read(ClientSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session, bool delete, int iter)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < iter; i++)
            {
                var key = new TestObjectKey { key = i };
                TestObjectInput input = default;
                TestObjectOutput g1 = new();
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref g1);

                if (status.IsPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, g1) = GetSinglePendingResult(completedOutputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(i, g1.value.value);
            }

            if (delete)
            {
                var key = new TestObjectKey { key = 1 };
                var input = default(TestObjectInput);
                var output = new TestObjectOutput();
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output);

                if (status.IsPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                ClassicAssert.IsFalse(status.Found);
            }
        }
    }
}
#endif // LOGRECORD_TODO
