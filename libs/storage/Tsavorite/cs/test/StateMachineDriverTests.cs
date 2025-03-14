// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    using static Tsavorite.test.TestUtils;
    using LongAllocator = SpanByteAllocator<StoreFunctions<SpanByte, LongKeyComparer, DefaultRecordDisposer<SpanByte>>>;
    using LongStoreFunctions = StoreFunctions<SpanByte, LongKeyComparer, DefaultRecordDisposer<SpanByte>>;

    public abstract class StateMachineDriverTestsBase
    {
        readonly int numOpThreads = 2;
        IDevice log;

        protected bool opsDone;
        protected long[] expectedV1Count;
        protected long[] expectedV2Count;
        protected readonly int numKeys = 4;

        protected void BaseSetup()
        {
            opsDone = false;
            expectedV1Count = new long[numKeys];
            expectedV2Count = new long[numKeys];
            log = CreateTestDevice(DeviceType.LSD, Path.Join(MethodTestDir, "Test.log"));
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir, true);
        }

        protected abstract void OperationThread(int thread_id, TsavoriteKV<SpanByte, LongStoreFunctions, LongAllocator> store);

        public async ValueTask DoCheckpointVersionSwitchEquivalenceCheck(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] bool isAsync, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            // Create the original store
            using var store1 = new TsavoriteKV<SpanByte, LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                PageSize = 1L << 10,
                MemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions<SpanByte>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Start operation threads
            var opTasks = new Task[numOpThreads];
            for (int i = 0; i < numOpThreads; i++)
            {
                var thread_id = i;
                opTasks[i] = Task.Run(() => OperationThread(thread_id, store1));
            }

            // Wait for some operations to complete in v1
            await Task.Delay(500);

            // Initiate checkpoint concurrent to the operation threads
            var task = store1.TakeFullCheckpointAsync(checkpointType);

            // Wait for the checkpoint to complete
            Guid token;
            if (isAsync)
            {
                (var status, token) = await task;
            }
            else
            {
                (var status, token) = task.AsTask().GetAwaiter().GetResult();
            }

            // Wait for some operations to complete in v2
            await Task.Delay(500);

            // Signal operation threads to stop, and wait for them to finish
            opsDone = true;
            await Task.WhenAll(opTasks);

            // Verify the final state of the old store
            using var s1 = store1.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < numKeys; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByteFrom(ref key), ref output);
                if (status.IsPending)
                {
                    var completed = bc1.CompletePendingWithOutputs(out var completedOutputs, true);
                    ClassicAssert.IsTrue(completed);
                    bool result = completedOutputs.Next();
                    ClassicAssert.IsTrue(result);
                    status = completedOutputs.Current.Status;
                    output = completedOutputs.Current.Output;
                    result = completedOutputs.Next();
                    ClassicAssert.IsFalse(result);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");

                // The old store should have the latest state
                ClassicAssert.AreEqual(expectedV2Count[key], output, $"output = {output}");
            }

            // Recover new store from the checkpoint
            using var store2 = new TsavoriteKV<SpanByte, LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                MemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions<SpanByte>.Create(LongKeyComparer.Instance)
            , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            if (isAsync)
            {
                _ = await store2.RecoverAsync(default, token);
            }
            else
            {
                _ = store2.Recover(default, token);
            }

            // Verify the state of the new store
            using var s2 = store2.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < numKeys; key++)
            {
                long output = default;
                var status = bc2.Read(SpanByteFrom(ref key), ref output);
                if (status.IsPending)
                {
                    var completed = bc2.CompletePendingWithOutputs(out var completedOutputs, true);
                    ClassicAssert.IsTrue(completed);
                    bool result = completedOutputs.Next();
                    ClassicAssert.IsTrue(result);
                    status = completedOutputs.Current.Status;
                    output = completedOutputs.Current.Output;
                    result = completedOutputs.Next();
                    ClassicAssert.IsFalse(result);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");

                // The new store should have state as of V1, and not the latest state of the old store
                ClassicAssert.AreEqual(expectedV1Count[key], output, $"output = {output}");
            }
        }

        public class SumFunctions : SimpleLongSimpleFunctions
        {
            public SumFunctions() : base((l, r) => l.AsRef<long>() + r.AsRef<long>()) { }
        }
    }

    [TestFixture]
    public class CheckpointVersionSwitchRmw : StateMachineDriverTestsBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        protected override void OperationThread(int thread_id, TsavoriteKV<SpanByte, LongStoreFunctions, LongAllocator> store)
        {
            using var s = store.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var bc = s.BasicContext;
            var r = new Random(thread_id);

            long key = 0;
            long input = 1;
            var v1count = new long[numKeys];
            var v2count = new long[numKeys];
            while (!opsDone)
            {
                // Generate input for RMW
                key = r.Next(numKeys);

                // Run the RMW operation
                _ = bc.RMW(SpanByteFrom(ref key), ref input);

                // Update expected counts for the old and new version of store
                if (bc.Session.Version == 1)
                    v1count[key]++;
                v2count[key]++;
            }

            // Update the global expected counts
            for (int i = 0; i < numKeys; i++)
            {
                _ = Interlocked.Add(ref expectedV1Count[i], v1count[i]);
                _ = Interlocked.Add(ref expectedV2Count[i], v2count[i]);
            }
        }

        [Test]
        public async ValueTask CheckpointVersionSwitchRmwTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] bool isAsync,
            [Values(1L << 13, 1L << 16)] long indexSize)
            => await DoCheckpointVersionSwitchEquivalenceCheck(checkpointType, isAsync, indexSize);
    }

    [TestFixture]
    public class CheckpointVersionSwitchTxn : StateMachineDriverTestsBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        protected override void OperationThread(int thread_id, TsavoriteKV<SpanByte, LongStoreFunctions, LongAllocator> store)
        {
            using var s = store.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var lc = s.TransactionalContext;
            var r = new Random(thread_id);

            ClassicAssert.IsTrue(numKeys > 1);
            long key1 = 0, key2 = 0;
            long input = 1;
            var v1count = new long[numKeys];
            var v2count = new long[numKeys];
            while (!opsDone)
            {
                // Generate input for transaction
                key1 = r.Next(numKeys);
                do
                {
                    key2 = r.Next(numKeys);
                } while (key2 == key1);

                var exclusiveVec = new FixedLengthTransactionalKeyStruct[] {
                    new(SpanByteFrom(ref key1), LockType.Exclusive, lc),
                    new(SpanByteFrom(ref key2), LockType.Exclusive, lc)
                };

                // Start transaction, session does not acquire version in this call
                lc.BeginTransaction();

                // Lock keys, session acquires version in this call
                lc.Lock(exclusiveVec);

                // We have determined the version of the transaction
                var txnVersion = lc.Session.Version;

                // Run transaction
                _ = lc.RMW(SpanByteFrom(ref key1), ref input);
                _ = lc.RMW(SpanByteFrom(ref key2), ref input);

                // Unlock keys
                lc.Unlock(exclusiveVec);

                // End transaction
                lc.EndTransaction();

                // Update expected counts for the old and new version of store
                if (txnVersion == 1)
                {
                    v1count[key1]++;
                    v1count[key2]++;
                }
                v2count[key1]++;
                v2count[key2]++;
            }

            // Update the global expected counts
            for (int i = 0; i < numKeys; i++)
            {
                _ = Interlocked.Add(ref expectedV1Count[i], v1count[i]);
                _ = Interlocked.Add(ref expectedV2Count[i], v2count[i]);
            }
        }

        [Test]
        public async ValueTask CheckpointVersionSwitchTxnTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] bool isAsync,
            [Values(1L << 13, 1L << 16)] long indexSize)
            => await DoCheckpointVersionSwitchEquivalenceCheck(checkpointType, isAsync, indexSize);
    }
}