// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    [TestFixture]
    public class ObjectRecoveryTests2 : TestBase
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
            TestUtils.OnTearDown();
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

                var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                Write(session, store, checkpointType);
                Read(session, delete: false);
                session.Dispose();

                _ = store.TryInitiateFullCheckpoint(out var guid, checkpointType);  // guid is useful for debugging, but not otherwise used in this test
                await store.CompleteCheckpointAsync();

                Destroy(log, objlog, store);
            }

            // Restore and verify
            {
                Prepare(out var log, out var objlog, out var store);

                if (syncMode == CompletionSyncMode.Async)
                    _ = await store.RecoverAsync().ConfigureAwait(false);
                else
                    _ = store.Recover();

                var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
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
                CheckpointDir = Path.Combine(MethodTestDir, "checkpoints")
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

        private void Write(ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
                TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < numberOfRecords; i++)
            {
                var _key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(_key, value);
                if (i > 0 && i % 100 == 0)
                {
                    _ = store.TryInitiateFullCheckpoint(out _, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        private void Read(ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session, bool delete)
        {
            var bContext = session.BasicContext;

            for (int i = 0; i < numberOfRecords; i++)
            {
                TestObjectKey key = new() { key = i };
                TestObjectInput input = default;
                TestObjectOutput output = new();

                var status = bContext.Read(key, ref input, ref output);
                bool wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                ClassicAssert.IsTrue(status.Found, $"key: {key.key}; status {status}; wasPending {wasPending}");
                ClassicAssert.AreEqual(i, output.value.value);
            }

            if (delete)
            {
                TestObjectKey key = new() { key = 1 };
                TestObjectInput input = default;
                TestObjectOutput output = new();
                _ = bContext.Delete(key);
                var status = bContext.Read(key, ref input, ref output);

                ClassicAssert.IsFalse(status.IsPending, $"key: {key.key}; status {status}");
                ClassicAssert.IsFalse(status.Found, $"key: {key.key}; status {status}");
            }
        }
    }
}