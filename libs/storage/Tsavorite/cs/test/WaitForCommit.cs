// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class WaitForCommitTests
    {
        static TsavoriteLog log;
        public IDevice device;
        private string path;
        static readonly byte[] entry = new byte[10];
        static readonly AutoResetEvent ev = new(false);
        static readonly AutoResetEvent done = new(false);

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "WaitForCommit", deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.DeleteDirectory(path);
        }

        [TestCase("Sync")]  // use string here instead of Bool so shows up in Test Explorer with more descriptive name
        [TestCase("Async")]
        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void WaitForCommitBasicTest(string SyncTest)
        {
            CancellationTokenSource cts = new();
            CancellationToken token = cts.Token;

            // make it small since launching each on separate threads 
            int entryLength = 10;
            int expectedEntries = 3;  // Not entry length because this is number of enqueues called

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Enqueue / WaitForCommit on a task (that will be waited) until the Commit on the separate thread is done
            if (SyncTest == "Sync")
            {
                new Thread(new ThreadStart(LogWriter)).Start();
            }
            else
            {
                new Thread(new ThreadStart(LogWriterAsync)).Start();
            }

            ev.WaitOne();
            log.Commit(true);

            // Read the log to make sure all entries are put in
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        Assert.AreEqual((byte)currentEntry, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected entries is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(expectedEntries, currentEntry, $"expectedEntries:{expectedEntries} does not equal currentEntry:{currentEntry}");

            done.WaitOne();
        }

        static void LogWriter()
        {
            // Enter in some entries then wait on this separate thread
            log.Enqueue(entry);
            log.Enqueue(entry);
            log.Enqueue(entry);
            ev.Set();
            log.WaitForCommit(log.TailAddress);
            done.Set();
        }

        static void LogWriterAsync()
        {
            // Using "await" here will kick out of the calling thread once the first await is finished
            // Enter in some entries then wait on this separate thread
            log.EnqueueAsync(entry).AsTask().GetAwaiter().GetResult();
            log.EnqueueAsync(entry).AsTask().GetAwaiter().GetResult();
            log.EnqueueAsync(entry).AsTask().GetAwaiter().GetResult();
            ev.Set();
            log.WaitForCommitAsync(log.TailAddress).AsTask().GetAwaiter().GetResult();
            done.Set();
        }
    }
}