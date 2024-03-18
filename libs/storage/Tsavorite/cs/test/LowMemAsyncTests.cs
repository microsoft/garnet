// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.async
{
    [TestFixture]
    public class LowMemAsyncTests
    {
        IDevice log;
        TsavoriteKV<long, long> store1;
        const int numOps = 2000;
        string path;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir;
            TestUtils.DeleteDirectory(path, wait: true);
            log = new LocalMemoryDevice(1L << 28, 1L << 25, 1, latencyMs: 20, fileName: path + "/test.log");
            Directory.CreateDirectory(path);
            store1 = new TsavoriteKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 26 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );
        }

        [TearDown]
        public void TearDown()
        {
            store1?.Dispose();
            store1 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

        private static async Task Populate(ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1)
        {
            var tasks = new ValueTask<TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                tasks[key] = s1.UpsertAsync(ref key, ref key);
            }

            for (var done = false; !done; /* set in loop */)
            {
                done = true;
                for (long key = 0; key < numOps; key++)
                {
                    var result = await tasks[key].ConfigureAwait(false);
                    if (result.Status.IsPending)
                    {
                        done = false;
                        tasks[key] = result.CompleteAsync();
                    }
                }
            }

            // This should return immediately, if we have no async concurrency issues in pending count management.
            s1.CompletePending(true);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category(TestUtils.StressTestCategory)]
        public async Task LowMemConcurrentUpsertReadAsyncTest()
        {
            await Task.Yield();
            using var s1 = store1.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>((a, b) => a + b));

            await Populate(s1).ConfigureAwait(false);

            // Read all keys
            var readtasks = new ValueTask<TsavoriteKV<long, long>.ReadAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
                readtasks[key] = s1.ReadAsync(ref key, ref key);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key, output);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category(TestUtils.StressTestCategory)]
        public async Task LowMemConcurrentUpsertRMWReadAsyncTest([Values] bool completeSync)
        {
            await Task.Yield();
            using var s1 = store1.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>((a, b) => a + b));

            await Populate(s1).ConfigureAwait(false);

            // RMW all keys
            var rmwtasks = new ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
                rmwtasks[key] = s1.RMWAsync(ref key, ref key);

            for (var done = false; !done; /* set in loop */)
            {
                done = true;
                for (long key = 0; key < numOps; key++)
                {
                    var result = await rmwtasks[key].ConfigureAwait(false);
                    if (result.Status.IsPending)
                    {
                        if (completeSync)
                        {
                            result.Complete();
                            continue;
                        }
                        done = false;
                        rmwtasks[key] = result.CompleteAsync();
                    }
                }
            }

            // Then Read all keys
            var readtasks = new ValueTask<TsavoriteKV<long, long>.ReadAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
                readtasks[key] = s1.ReadAsync(ref key, ref key);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key + key, output);
            }
        }
    }
}