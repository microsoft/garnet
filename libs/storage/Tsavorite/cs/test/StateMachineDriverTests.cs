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
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>>;
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;

    class SumFunctions : SimpleSimpleFunctions<long, long>
    {
        public SumFunctions() : base((l, r) => l + r) { }
    }

    [TestFixture]
    public class CheckpointVersionSwitchTest
    {
        IDevice log;
        bool rmwDone;
        long[] expectedV1Count;
        long[] expectedV2Count;
        readonly int numKeys = 2;

        [SetUp]
        public void Setup()
        {
            rmwDone = false;
            expectedV1Count = new long[numKeys];
            expectedV2Count = new long[numKeys];
            log = CreateTestDevice(DeviceType.LSD, Path.Join(MethodTestDir, "Test.log"));
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir, true);
        }

        void RmwOperationThread(int thread_id, TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store)
        {
            using var s = store.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var bc = s.BasicContext;
            var r = new Random(thread_id);

            long key = 0;
            long input = 1;
            var v1count = new long[numKeys];
            var v2count = new long[numKeys];
            while (!rmwDone)
            {
                key = r.Next(numKeys);
                _ = bc.RMW(ref key, ref input);
                if (bc.Session.Version == 1)
                    v1count[key]++;
                v2count[key]++;
            }
            for (int i = 0; i < numKeys; i++)
            {
                _ = Interlocked.Add(ref expectedV1Count[i], v1count[i]);
                _ = Interlocked.Add(ref expectedV2Count[i], v2count[i]);
            }
        }

        [Test]
        public async ValueTask CheckpointVersionSwitchTest1(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] bool isAsync, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            // Create the original store
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                PageSize = 1L << 10,
                MemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Start RMW operation threads
            int NumRmwThreads = 2;
            var rmwTasks = new Task[NumRmwThreads];
            for (int i = 0; i < NumRmwThreads; i++)
            {
                var thread_id = i;
                rmwTasks[i] = Task.Run(() => RmwOperationThread(thread_id, store1));
            }

            // Wait for some RMWs to complete in v1
            await Task.Delay(500);

            // Initiate checkpoint concurrent to the RMW operation threads
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

            // Wait for some RMWs to complete in v2
            await Task.Delay(500);

            // Signal RMW threads to stop
            rmwDone = true;
            await Task.WhenAll(rmwTasks);

            // Verify the final state of the old store
            using var s1 = store1.NewSession<long, long, Empty, SumFunctions>(new SumFunctions());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < numKeys; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
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
            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                MemorySize = 1L << 20,
                CheckpointDir = MethodTestDir
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
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
                var status = bc2.Read(ref key, ref output);
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
    }
}