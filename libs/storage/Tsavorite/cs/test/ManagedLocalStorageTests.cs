// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class ManageLocalStorageTests
    {
        private TsavoriteLog log;
        private IDevice device;
        private TsavoriteLog logFullParams;
        private IDevice deviceFullParams;
        static readonly byte[] entry = new byte[100];
        private string path;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            // We loop to ensure clean-up as deleteOnClose does not always work for MLSD
            TestUtils.DeleteDirectory(path, wait: true);

            // Create devices \ log for test
            device = new ManagedLocalStorageDevice(path + "ManagedLocalStore.log", deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 14 });

            deviceFullParams = new ManagedLocalStorageDevice(path + "ManagedLocalStoreFullParams.log", deleteOnClose: false, recoverDevice: true, preallocateFile: true, capacity: 1 << 30);
            logFullParams = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 14 });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;
            logFullParams?.Dispose();
            logFullParams = null;
            deviceFullParams?.Dispose();
            deviceFullParams = null;

            // Clean up log 
            TestUtils.DeleteDirectory(path);
        }


        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ManagedLocalStoreBasicTest()
        {
            int entryLength = 20;
            int numEntries = 1000;
            int numEnqueueThreads = 1;
            int numIterThreads = 1;
            bool commitThread = false;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            bool disposeCommitThread = false;
            var commit =
                new Thread(() =>
                {
                    while (!disposeCommitThread)
                    {
                        log.Commit(true);
                    }
                });

            if (commitThread)
                commit.Start();

            Thread[] th = new Thread[numEnqueueThreads];
            for (int t = 0; t < numEnqueueThreads; t++)
            {
                th[t] =
                new Thread(() =>
                {
                    // Enqueue but set each Entry in a way that can differentiate between entries
                    for (int i = 0; i < numEntries; i++)
                    {
                        // Flag one part of entry data that corresponds to index
                        entry[0] = (byte)i;

                        // Default is add bytes so no need to do anything with it
                        log.Enqueue(entry);
                    }
                });
            }
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Start();
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Join();

            if (commitThread)
            {
                disposeCommitThread = true;
                commit.Join();
            }

            // Final commit to the log
            log.Commit(true);

            int currentEntry = 0;

            Thread[] th2 = new Thread[numIterThreads];
            for (int t = 0; t < numIterThreads; t++)
            {
                th2[t] =
                    new Thread(() =>
                    {
                        // Read the log - Look for the flag so know each entry is unique
                        using (var iter = log.Scan(0, long.MaxValue))
                        {
                            while (iter.GetNext(out byte[] result, out _, out _))
                            {
                                if (numEnqueueThreads == 1)
                                    Assert.AreEqual((byte)currentEntry, result[0]);
                                currentEntry++;
                            }
                        }

                        Assert.AreEqual(numEntries * numEnqueueThreads, currentEntry);
                    });
            }

            for (int t = 0; t < numIterThreads; t++)
                th2[t].Start();
            for (int t = 0; t < numIterThreads; t++)
                th2[t].Join();

            // Make sure number of entries is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(numEntries, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ManagedLocalStoreFullParamsTest()
        {

            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                logFullParams.Enqueue(entry);
            }

            // Commit to the log
            logFullParams.Commit(true);

            // Verify  
            Assert.IsTrue(File.Exists(path + "/log-commits/commit.1.0"));
            Assert.IsTrue(File.Exists(path + "/ManagedLocalStore.log.0"));

            // Read the log just to verify can actually read it
            int currentEntry = 0;
            using var iter = logFullParams.Scan(0, 100_000_000);
            while (iter.GetNext(out byte[] result, out _, out _))
            {
                Assert.AreEqual(currentEntry, result[currentEntry]);
                currentEntry++;
            }
        }
    }
}