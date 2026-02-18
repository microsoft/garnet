// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    using static Tsavorite.test.TestUtils;
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, DefaultRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, DefaultRecordDisposer>;

    public enum TimeFuzzMode { TimeFuzz, NoTimeFuzz };

    public abstract class StateMachineDriverTestsBase : AllureTestBase
    {
        readonly int numOpThreads = 2;
        protected readonly int numKeys = 4;
        readonly int numIterations = 3;

        IDevice log;
        protected bool opsDone;
        protected long[] expectedV1Count;
        protected long[] expectedV2Count;
        protected int currentIteration;

        protected void BaseSetup()
        {
            opsDone = false;
            expectedV1Count = new long[numKeys];
            expectedV2Count = new long[numKeys];
            log = CreateTestDevice(TestDeviceType.LSD, Path.Join(MethodTestDir, "Test.log"));
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir, true);
        }

        protected abstract void OperationThread(int thread_id, bool useTimingFuzzing, TsavoriteKV<LongStoreFunctions, LongAllocator> store);

        public async ValueTask DoCheckpointVersionSwitchEquivalenceCheck(CheckpointType checkpointType, long indexSize, bool useTimingFuzzing)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                System.Diagnostics.Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1}, name = {TestContext.CurrentContext.Test.Name} ***");

            // Create the original store
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (currentIteration = 0; currentIteration < numIterations; currentIteration++)
            {
                opsDone = false;

                // Start operation threads
                var opTasks = new Task[numOpThreads];
                for (int i = 0; i < numOpThreads; i++)
                {
                    var thread_id = i;
                    opTasks[i] = Task.Run(() => OperationThread(thread_id, useTimingFuzzing, store1));
                }

                // Wait for some operations to complete in v1
                await Task.Delay(500);

                // Initiate checkpoint concurrent to the operation threads
                var task = store1.TakeFullCheckpointAsync(checkpointType);

                // Wait for the checkpoint to complete
                (var checkpointStatus, var checkpointToken) = await task;

                // Wait for some operations to complete in v2
                await Task.Delay(500);

                // Signal operation threads to stop, and wait for them to finish
                opsDone = true;
                await Task.WhenAll(opTasks);

                // Verify the final state of the old store
                using var s1 = store1.NewSession<long, long, Empty, SumFunctions>(new SumFunctions(0, false));
                var bc1 = s1.BasicContext;
                for (long key = 0; key < numKeys; key++)
                {
                    long output = default;
                    var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
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
                using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1,
                    PageSize = 1L << 10,
                    LogMemorySize = 1L << 20,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

                _ = await store2.RecoverAsync(default, checkpointToken);

                // Verify the state of the new store
                using var s2 = store2.NewSession<long, long, Empty, SumFunctions>(new SumFunctions(0, false));
                var bc2 = s2.BasicContext;
                for (long key = 0; key < numKeys; key++)
                {
                    long output = default;
                    var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
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

                // Copy V2 counts to V1 counts for the next iteration
                for (int i = 0; i < numKeys; i++)
                {
                    expectedV1Count[i] = expectedV2Count[i];
                }
            }
        }

        public async ValueTask DoGrowIndexVersionSwitchEquivalenceCheck(long indexSize, bool useTimingFuzzing)
        {
            // Create the original store
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (currentIteration = 0; currentIteration < numIterations; currentIteration++)
            {
                opsDone = false;

                // Start operation threads
                var opTasks = new Task[numOpThreads];
                for (int i = 0; i < numOpThreads; i++)
                {
                    var thread_id = i;
                    opTasks[i] = Task.Run(() => OperationThread(thread_id, useTimingFuzzing, store1));
                }

                // Wait for some operations to complete in v1
                await Task.Delay(500);

                // Grow index concurrent to the operation threads
                var growIndexStatus = await store1.GrowIndexAsync();
                ClassicAssert.IsTrue(growIndexStatus);

                // Wait for some operations to complete in v2
                await Task.Delay(500);

                // Signal operation threads to stop, and wait for them to finish
                opsDone = true;
                await Task.WhenAll(opTasks);

                // Verify the final state of the store
                using var s1 = store1.NewSession<long, long, Empty, SumFunctions>(new SumFunctions(0, false));
                var bc1 = s1.BasicContext;
                for (long key = 0; key < numKeys; key++)
                {
                    long output = default;
                    var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
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

                    // The store should have the latest expected state
                    ClassicAssert.AreEqual(expectedV2Count[key], output, $"output = {output}");
                }
            }
        }

        public class SumFunctions : SimpleLongSimpleFunctions
        {
            readonly Random fuzzer;

            public SumFunctions(int thread_id, bool useTimingFuzzing) : base((l, r) => l + r)
            {
                if (useTimingFuzzing) fuzzer = new Random(thread_id);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                Fuzz();
                var ret = base.InPlaceUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
                Fuzz();
                return ret;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                Fuzz();
                var ret = base.CopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
                Fuzz();
                return ret;
            }

            void Fuzz()
            {
                if (fuzzer != null)
                    Thread.Sleep(fuzzer.Next(30));
            }
        }
    }

    [AllureNUnit]
    [TestFixture]
    public class CheckpointVersionSwitchRmw : StateMachineDriverTestsBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        protected override void OperationThread(int thread_id, bool useTimingFuzzing, TsavoriteKV<LongStoreFunctions, LongAllocator> store)
        {
            using var s = store.NewSession<long, long, Empty, SumFunctions>(new SumFunctions(thread_id, useTimingFuzzing));
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
                _ = bc.RMW(SpanByte.FromPinnedVariable(ref key), ref input);

                // Update expected counts for the old and new version of store
                if (bc.Session.Version == currentIteration + 1)
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
        //[Repeat(1000)]
        public async ValueTask CheckpointVersionSwitchRmwTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values(1L << 13, 1L << 16)] long indexSize,
            [Values] TimeFuzzMode timeFuzzMode)
            => await DoCheckpointVersionSwitchEquivalenceCheck(checkpointType, indexSize, timeFuzzMode == TimeFuzzMode.TimeFuzz);

        [Test]
        //[Repeat(1000)]
        public async ValueTask GrowIndexVersionSwitchRmwTest(
            [Values(1L << 13, 1L << 16)] long indexSize,
            [Values] TimeFuzzMode timeFuzzMode)
            => await DoGrowIndexVersionSwitchEquivalenceCheck(indexSize, timeFuzzMode == TimeFuzzMode.TimeFuzz);
    }

    [AllureNUnit]
    [TestFixture]
    public class CheckpointVersionSwitchTxn : StateMachineDriverTestsBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        protected override void OperationThread(int thread_id, bool useTimingFuzzing, TsavoriteKV<LongStoreFunctions, LongAllocator> store)
        {
            using var s = store.NewSession<long, long, Empty, SumFunctions>(new SumFunctions(thread_id, useTimingFuzzing));
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
                    new(SpanByte.FromPinnedVariable(ref key1), LockType.Exclusive, lc),
                    new(SpanByte.FromPinnedVariable(ref key2), LockType.Exclusive, lc)
                };

                var txnVersion = store.stateMachineDriver.AcquireTransactionVersion();

                // Start transaction, session does not acquire version in this call
                lc.BeginTransaction();

                // Lock keys, session acquires version in this call
                lc.Lock<FixedLengthTransactionalKeyStruct>(exclusiveVec);

                txnVersion = store.stateMachineDriver.VerifyTransactionVersion(txnVersion);
                lc.LocksAcquired(txnVersion);

                // Run transaction
                _ = lc.RMW(SpanByte.FromPinnedVariable(ref key1), ref input);
                _ = lc.RMW(SpanByte.FromPinnedVariable(ref key2), ref input);

                // Unlock keys
                lc.Unlock<FixedLengthTransactionalKeyStruct>(exclusiveVec);

                // End transaction
                lc.EndTransaction();

                store.stateMachineDriver.EndTransaction(txnVersion);

                // Update expected counts for the old and new version of store
                if (txnVersion == currentIteration + 1)
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
            [Values(1L << 13, 1L << 16)] long indexSize,
            [Values] TimeFuzzMode timeFuzzMode)
            => await DoCheckpointVersionSwitchEquivalenceCheck(checkpointType, indexSize, timeFuzzMode == TimeFuzzMode.TimeFuzz);

        [Test]
        public async ValueTask GrowIndexVersionSwitchTxnTest(
            [Values(1L << 13, 1L << 16)] long indexSize,
            [Values] TimeFuzzMode timeFuzzMode)
            => await DoGrowIndexVersionSwitchEquivalenceCheck(indexSize, timeFuzzMode == TimeFuzzMode.TimeFuzz);
    }
}