// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable  // Add parentheses for clarity

namespace Tsavorite.test.InsertAtTailStressTests
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparerModulo, SpanByteRecordDisposer>;

    // Number of mutable pages for this test
    public enum MutablePages
    {
        Zero,
        One,
        Two
    }

    [AllureNUnit]
    [TestFixture]
    class SpanByteInsertAtTailChainTests : AllureTestBase
    {
        private TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private IDevice log;
        SpanByteComparerModulo comparer;

        const long ValueAdd = 1_000_000_000;
        const long NumKeys = 2_000;

        long GetMutablePageCount(MutablePages mp) => mp switch
        {
            MutablePages.Zero => 0,
            MutablePages.One => 1,
            MutablePages.Two => 2,
            _ => 8
        };

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = Path.Join(MethodTestDir, $"{GetType().Name}.log");
            log = new NullDevice();

            HashModulo modRange = HashModulo.NoMod;
            long mutablePages = GetMutablePageCount(MutablePages.Two);
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo cr)
                {
                    modRange = cr;
                    continue;
                }
                if (arg is MutablePages mp)
                {
                    mutablePages = GetMutablePageCount(mp);
                    continue;
                }
            }

            // Make the main log mutable region small enough that we force the readonly region to stay close to tail, causing inserts.
            int pageBits = 15, memoryBits = 34;
            KVSettings<SpanByte, SpanByte> kvSettings = new()
            {
                LogDevice = log,
                PageSize = 1L << pageBits,
                MemorySize = 1L << memoryBits,
                MutableFraction = 8.0 / (1 << (memoryBits - pageBits)),
            };
            store = new(kvSettings
                , StoreFunctions<SpanByte, SpanByte>.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            comparer = new SpanByteComparerModulo(modRange);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        internal class RmwSpanByteFunctions : SpanByteFunctions<Empty>
        {
            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                src.CopyTo(ref dst);
                src.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                src.CopyTo(ref dst);
                src.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                input.CopyTo(ref newValue);
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                // The default implementation of IPU simply writes input to destination, if there is space
                base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                Assert.Fail("For these tests, InitialUpdater should never be called");
                return false;
            }
        }

        unsafe void PopulateAndSetReadOnlyToTail()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
            var bContext = session.BasicContext;

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var key = SpanByte.FromPinnedSpan(keyVec);

            for (long ii = 0; ii < NumKeys; ii++)
            {
                ClassicAssert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                var status = bContext.Upsert(ref key, ref key);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            bContext.CompletePending(true);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(300)]
        public void SpanByteTailInsertMultiThreadTest([Values] HashModulo modRange, [Values(0, 1, 2, 8)] int numReadThreads, [Values(0, 1, 2, 8)] int numWriteThreads,
                                                [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] MutablePages mutablePages)
        {
            if (numReadThreads == 0 && numWriteThreads == 0)
                Assert.Ignore("Skipped due to 0 threads for both read and update");
            if ((numReadThreads > 2 || numWriteThreads > 2) && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            // Initial population so we know we can read the keys.
            PopulateAndSetReadOnlyToTail();

            const int numIterations = 10;
            unsafe void runReadThread(int tid)
            {
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                Span<byte> keyVec = stackalloc byte[sizeof(long)];
                var key = SpanByte.FromPinnedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                        var status = bContext.Read(ref key, ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                            ++numPending;
                        else
                        {
                            ++numCompleted;

                            ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}, wasPending {false}, pt 1");
                            ClassicAssert.IsNotNull(output.Memory, $"tid {tid}, key {ii}, wasPending {false}, pt 2");
                            long value = BitConverter.ToInt64(output.AsReadOnlySpan());
                            ClassicAssert.AreEqual(ii, value % ValueAdd, $"tid {tid}, key {ii}, wasPending {false}, pt 3");
                            output.Memory.Dispose();
                        }

                        if (numPending > 0)
                        {
                            bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;

                                    status = completedOutputs.Current.Status;
                                    output = completedOutputs.Current.Output;
                                    // Note: do NOT overwrite 'key' here
                                    long keyLong = BitConverter.ToInt64(completedOutputs.Current.Key.AsReadOnlySpan());

                                    ClassicAssert.AreEqual(completedOutputs.Current.RecordMetadata.Address == Constants.kInvalidAddress, status.Record.CopiedToReadCache, $"key {keyLong}: {status}");

                                    ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {keyLong}, {status}, wasPending {true}, pt 1");
                                    ClassicAssert.IsNotNull(output.Memory, $"tid {tid}, key {keyLong}, wasPending {true}, pt 2");
                                    long value = BitConverter.ToInt64(output.AsReadOnlySpan());
                                    ClassicAssert.AreEqual(keyLong, value % ValueAdd, $"tid {tid}, key {keyLong}, wasPending {true}, pt 3");
                                    output.Memory.Dispose();
                                }
                            }
                        }
                    }
                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new RmwSpanByteFunctions());
                var bContext = session.BasicContext;

                Span<byte> keyVec = stackalloc byte[sizeof(long)];
                var key = SpanByte.FromPinnedSpan(keyVec);
                Span<byte> inputVec = stackalloc byte[sizeof(long)];
                var input = SpanByte.FromPinnedSpan(inputVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(inputVec, ii + ValueAdd));
                        var status = updateOp == UpdateOp.RMW
                                        ? bContext.RMW(ref key, ref input, ref output)
                                        : bContext.Upsert(ref key, ref input, ref input, ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                        {
                            ClassicAssert.AreNotEqual(UpdateOp.Upsert, updateOp, "Upsert should not go pending");
                            ++numPending;
                        }
                        else
                        {
                            ++numCompleted;
                            if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}");

                            long value = BitConverter.ToInt64(output.AsReadOnlySpan());
                            ClassicAssert.AreEqual(ii + ValueAdd, value, $"tid {tid}, key {ii}, wasPending {false}");

                            output.Memory?.Dispose();
                        }

                        if (numPending > 0)
                        {
                            bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;

                                    status = completedOutputs.Current.Status;
                                    output = completedOutputs.Current.Output;
                                    // Note: do NOT overwrite 'key' here
                                    long keyLong = BitConverter.ToInt64(completedOutputs.Current.Key.AsReadOnlySpan());

                                    if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                        ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {keyLong}, {status}");

                                    long value = BitConverter.ToInt64(output.AsReadOnlySpan());
                                    ClassicAssert.AreEqual(keyLong + ValueAdd, value, $"tid {tid}, key {keyLong}, wasPending {true}");

                                    output.Memory?.Dispose();
                                }
                            }
                        }
                    }
                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 1; t <= numReadThreads + numWriteThreads; t++)
            {
                var tid = t;
                if (t <= numReadThreads)
                    tasks.Add(Task.Factory.StartNew(() => runReadThread(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}