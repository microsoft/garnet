// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.LowMemory
{
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>>;
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;

    [AllureNUnit]
    [TestFixture]
    public class LowMemoryTests : AllureTestBase
    {
        IDevice log;
        TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store1;
        const int NumOps = 2000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = new LocalMemoryDevice(1L << 28, 1L << 25, 1, latencyMs: 20, fileName: Path.Join(TestUtils.MethodTestDir, "test.log"));
            _ = Directory.CreateDirectory(TestUtils.MethodTestDir);
            store1 = new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                MemorySize = 1L << 12,
                SegmentSize = 1L << 26,
                CheckpointDir = TestUtils.MethodTestDir
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store1?.Dispose();
            store1 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private static void Populate(ClientSession<long, long, long, long, Empty, SimpleSimpleFunctions<long, long>, LongStoreFunctions, LongAllocator> s1)
        {
            var bContext1 = s1.BasicContext;
            for (long key = 0; key < NumOps; key++)
                _ = bContext1.Upsert(ref key, ref key);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category(TestUtils.StressTestCategory)]
        public void LowMemConcurrentUpsertReadTest()
        {
            using var s1 = store1.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>((a, b) => a + b));
            var bContext1 = s1.BasicContext;

            Populate(s1);

            // Read all keys
            var numCompleted = 0;
            for (long key = 0; key < NumOps; key++)
            {
                var (status, output) = bContext1.Read(key);
                if (!status.IsPending)
                {
                    ++numCompleted;
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(key, output);
                }
            }

            _ = bContext1.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            using (completedOutputs)
            {
                while (completedOutputs.Next())
                {
                    ++numCompleted;
                    ClassicAssert.IsTrue(completedOutputs.Current.Status.Found, $"{completedOutputs.Current.Status}");
                    ClassicAssert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output);
                }
            }
            ClassicAssert.AreEqual(NumOps, numCompleted, "numCompleted");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category(TestUtils.StressTestCategory)]
        public void LowMemConcurrentUpsertRMWReadTest([Values] bool completeSync)
        {
            using var s1 = store1.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>((a, b) => a + b));
            var bContext1 = s1.BasicContext;

            Populate(s1);

            // RMW all keys
            int numPending = 0;
            for (long key = 0; key < NumOps; key++)
            {
                var status = bContext1.RMW(ref key, ref key);
                if (status.IsPending && (++numPending % 256) == 0)
                {
                    _ = bContext1.CompletePending(wait: true);
                    numPending = 0;
                }
            }
            if (numPending > 0)
                _ = bContext1.CompletePending(wait: true);

            // Then Read all keys
            var numCompleted = 0;
            for (long key = 0; key < NumOps; key++)
            {
                var (status, output) = bContext1.Read(key);
                if (!status.IsPending)
                {
                    ++numCompleted;
                    ClassicAssert.IsTrue(status.Found, $"{status}");
                    ClassicAssert.AreEqual(key + key, output);
                }
            }

            _ = bContext1.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            using (completedOutputs)
            {
                while (completedOutputs.Next())
                {
                    ++numCompleted;
                    ClassicAssert.IsTrue(completedOutputs.Current.Status.Found, $"{completedOutputs.Current.Status}");
                    ClassicAssert.AreEqual(completedOutputs.Current.Key * 2, completedOutputs.Current.Output);
                }
            }
            ClassicAssert.AreEqual(NumOps, numCompleted, "numCompleted");
        }
    }
}