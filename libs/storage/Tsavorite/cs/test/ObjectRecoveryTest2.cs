// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    public class ObjectRecoveryTests2 : AllureTestBase
    {
        int numberOfRecords;

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
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask ObjectRecoveryTest2(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Range(300, 700, 300)] int numberOfRecords,
            [Values] CompletionSyncMode syncMode)
        {
            this.numberOfRecords = numberOfRecords;

            // Populate and checkpoint
            {
                Prepare(out var log, out var objlog, out var store);

                var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                Write(session, store, checkpointType);
                Read(session, delete: false);
                session.Dispose();

                _ = store.TryInitiateFullCheckpoint(out _, checkpointType);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                Destroy(log, objlog, store);
            }

            // Restore and verify
            {
                Prepare(out var log, out var objlog, out var store);

                if (syncMode == CompletionSyncMode.Async)
                    _ = await store.RecoverAsync();
                else
                    _ = store.Recover();

                var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                Read(session, delete: true);
                session.Dispose();

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
                LogMemorySize = 1L << 12,
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

        private void Write(ClientSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
                TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < numberOfRecords; i++)
            {
                var _key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref _key), value);
                if (i % 100 == 0)
                {
                    _ = store.TryInitiateFullCheckpoint(out _, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        private void Read(ClientSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session, bool delete)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < numberOfRecords; i++)
            {
                TestObjectKey key = new() { key = i };
                TestObjectInput input = default;
                TestObjectOutput output = new();

                if (i == 196)
                { }
                
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output);

                if (status.IsPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                ClassicAssert.IsTrue(status.Found, $"key: {key.key}");
                ClassicAssert.AreEqual(i, output.value.value);
            }

            if (delete)
            {
                TestObjectKey key = new() { key = 1 };
                TestObjectInput input = default;
                TestObjectOutput output = new();
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output);

                ClassicAssert.IsFalse(status.IsPending, $"key: {key.key}");
                ClassicAssert.IsFalse(status.Found, $"key: {key.key}");
            }
        }
    }
}