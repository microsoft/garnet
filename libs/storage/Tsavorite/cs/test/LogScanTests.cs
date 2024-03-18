// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogScanTests
    {
        private TsavoriteLog log;
        private IDevice device;
        private TsavoriteLog logUncommitted;
        private IDevice deviceUnCommitted;

        private string path;
        static byte[] entry;
        const int entryLength = 100;
        const int numEntries = 1000;
        static readonly int entryFlag = 9999;

        // Create and populate the log file so can do various scans
        [SetUp]
        public void Setup()
        {
            entry = new byte[100];
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;
            deviceUnCommitted?.Dispose();
            deviceUnCommitted = null;
            logUncommitted?.Dispose();
            logUncommitted = null;

            // Clean up log files
            TestUtils.DeleteDirectory(path);
        }

        public void PopulateLog(TsavoriteLog log)
        {
            //****** Populate log for Basic data for tests 
            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                // Flag one part of entry data that corresponds to index
                if (i < entryLength)
                    entry[i] = (byte)entryFlag;

                // puts back the previous entry value
                if ((i > 0) && (i < entryLength))
                    entry[i - 1] = (byte)(i - 1);

                // Add to TsavoriteLog
                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit(true);
        }

        public void PopulateUncommittedLog(TsavoriteLog logUncommitted)
        {
            //****** Populate uncommitted log / device for ScanUncommittedTest
            // Set Default entry data
            for (int j = 0; j < entryLength; j++)
                entry[j] = (byte)j;

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int j = 0; j < numEntries; j++)
            {
                // Flag one part of entry data that corresponds to index
                if (j < entryLength)
                    entry[j] = (byte)entryFlag;

                // puts back the previous entry value
                if ((j > 0) && (j < entryLength))
                    entry[j - 1] = (byte)(j - 1);

                // Add to TsavoriteLog
                logUncommitted.Enqueue(entry);
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBasicDefaultTest([Values] TestUtils.DeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanDefault" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBehindBeginAddressTest([Values] TestUtils.DeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanDefault" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            using (var iter = log.Scan(0, 100_000_000))
            {
                var next = iter.GetNext(out byte[] result, out _, out _);
                Assert.IsTrue(next);

                // Verify result
                Assert.AreEqual((byte)entryFlag, result[0]);

                // truncate log to tail
                log.TruncateUntil(log.TailAddress);
                log.Commit(true);
                Assert.AreEqual(log.TailAddress, log.BeginAddress);

                // Wait for allocator to realize the new BeginAddress
                // Needed as this is done post-commit
                while (log.AllocatorBeginAddress < log.TailAddress)
                    Thread.Yield();

                // Iterator will skip ahead to tail
                next = iter.GetNext(out result, out _, out _);
                Assert.IsFalse(next);

                // WaitAsync should not complete, as we are at end of iteration
                var tcs = new CancellationTokenSource();
                var task = iter.WaitAsync(tcs.Token);
                Assert.IsFalse(task.IsCompleted);
                tcs.Cancel();
                try
                {
                    task.GetAwaiter().GetResult();
                }
                catch { }
            }
        }


        internal class TestConsumer : ILogEntryConsumer
        {
            internal int currentEntry = 0;

            public void Consume(ReadOnlySpan<byte> entry, long currentAddress, long nextAddress)
            {
                if (currentEntry < entryLength)
                {
                    // Span Batch only added first entry several times so have separate verification
                    Assert.AreEqual((byte)entryFlag, entry[currentEntry]);
                    currentEntry++;
                }
            }
        }
        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanConsumerTest([Values] TestUtils.DeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanDefault" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            var consumer = new TestConsumer();
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.TryConsumeNext(consumer)) { }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, consumer.currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ScanNoDefaultTest([Values] TestUtils.DeviceType deviceType)
        {
            // Test where all params are set just to make sure handles it ok

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanNoDefault" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, name: null, recover: true, scanBufferingMode: ScanBufferingMode.DoublePageBuffering, scanUncommitted: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanByNameTest([Values] TestUtils.DeviceType deviceType)
        {
            //You can persist iterators(or more precisely, their CompletedUntilAddress) as part of a commit by simply naming them during their creation. 

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanByName" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, name: "TestScan", recover: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanWithoutRecoverTest([Values] TestUtils.DeviceType deviceType)
        {
            // You may also force an iterator to start at the specified begin address, i.e., without recovering: recover parameter = false

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanWithoutRecover" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Read the log 
            int currentEntry = 9;   // since starting at specified address of 1000, need to set current entry as 9 so verification starts at proper spot
            using (var iter = log.Scan(1000, 100_000_000, recover: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBufferingModeDoublePageTest([Values] TestUtils.DeviceType deviceType)
        {
            // Same as default, but do it just to make sure have test in case default changes

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanDoublePage" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, scanBufferingMode: ScanBufferingMode.DoublePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBufferingModeSinglePageTest([Values] TestUtils.DeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScanSinglePage" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanUncommittedTest([Values] TestUtils.DeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = path + "LogScan" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = path, AutoRefreshSafeTailAddress = true });
            PopulateUncommittedLog(log);

            // Setting scanUnCommitted to true is actual test here.
            // Read the log - Look for the flag so know each entry is unique and still reads uncommitted
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        Assert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(entryLength, currentEntry);
        }
    }
}