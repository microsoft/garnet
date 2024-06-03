// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.LowMem
{
    [TestFixture]
    public class LowMemTests
    {
        IDevice log;
        TsavoriteKV<long, long> store1;
        const int numOps = 2000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = new LocalMemoryDevice(1L << 28, 1L << 25, 1, latencyMs: 20, fileName: Path.Join(TestUtils.MethodTestDir, "test.log"));
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            store1 = new TsavoriteKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 26 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir }
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

        private static void Populate(ClientSession<long, long, long, long, Empty, SimpleSimpleFunctions<long, long>> s1)
        {
            var bContext1 = s1.BasicContext;
            for (long key = 0; key < numOps; key++)
                bContext1.Upsert(ref key, ref key);
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
            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = bContext1.Read(key);
                if (!status.IsPending)
                {
                    ++numCompleted;
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(key, output);
                }
            }

            bContext1.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            using (completedOutputs)
            {
                while (completedOutputs.Next())
                {
                    ++numCompleted;
                    Assert.IsTrue(completedOutputs.Current.Status.Found, $"{completedOutputs.Current.Status}");
                    Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output);
                }
            }
            Assert.AreEqual(numOps, numCompleted, "numCompleted");
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
            for (long key = 0; key < numOps; key++)
            {
                var status = bContext1.RMW(ref key, ref key);
                if (status.IsPending && (++numPending % 256) == 0)
                { 
                    bContext1.CompletePending(wait: true);
                    numPending = 0;
                }
            }
            if (numPending > 0)
                bContext1.CompletePending(wait: true);

            // Then Read all keys
            var numCompleted = 0;
            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = bContext1.Read(key);
                if (!status.IsPending)
                { 
                    ++numCompleted;
                    Assert.IsTrue(status.Found, $"{status}");
                    Assert.AreEqual(key + key, output);
                }
            }

            bContext1.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            using (completedOutputs)
            {
                while (completedOutputs.Next())
                {
                    ++numCompleted;
                    Assert.IsTrue(completedOutputs.Current.Status.Found, $"{completedOutputs.Current.Status}");
                    Assert.AreEqual(completedOutputs.Current.Key * 2, completedOutputs.Current.Output);
                }
            }
            Assert.AreEqual(numOps, numCompleted, "numCompleted");
        }
    }
}