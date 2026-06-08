// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogScanTests : TestBase
    {
        private TsavoriteLog log;
        private IDevice device;
        private TsavoriteLog logUncommitted;
        private IDevice deviceUnCommitted;

        static byte[] entry;
        const int entryLength = 100;
        const int numEntries = 1000;
        static readonly int entryFlag = 9999;

        // Create and populate the log file so can do various scans
        [SetUp]
        public void Setup()
        {
            entry = new byte[100];
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
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
            TestUtils.OnTearDown();
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
                _ = log.Enqueue(entry);
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
                _ = logUncommitted.Enqueue(entry);
            }

            // Wait for safe tail to catch up
            while (logUncommitted.RefreshSafeTailAddress() < logUncommitted.TailAddress)
                _ = Thread.Yield();
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBasicDefaultTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanDefault" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBehindBeginAddressTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanDefault" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress))
            {
                var next = iter.GetNext(out byte[] result, out _, out _);
                ClassicAssert.IsTrue(next);

                // Verify result
                ClassicAssert.AreEqual((byte)entryFlag, result[0]);

                // truncate log to tail
                log.TruncateUntil(log.TailAddress);
                log.Commit(true);
                ClassicAssert.AreEqual(log.TailAddress, log.BeginAddress);

                // Wait for allocator to realize the new BeginAddress
                // Needed as this is done post-commit
                while (log.AllocatorBeginAddress < log.TailAddress)
                    _ = Thread.Yield();

                // Iterator will skip ahead to tail
                next = iter.GetNext(out result, out _, out _);
                ClassicAssert.IsFalse(next);

                // WaitAsync should not complete, as we are at end of iteration
                var tcs = new CancellationTokenSource();
                var task = iter.WaitAsync(tcs.Token);
                ClassicAssert.IsFalse(task.IsCompleted);
                tcs.Cancel();
                try
                {
                    _ = task.GetAwaiter().GetResult();
                }
                catch { }
            }
        }


        internal class TestConsumer : ILogEntryConsumer
        {
            internal int currentEntry = 0;

            public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
            {
                if (currentEntry < entryLength)
                {
                    // Span Batch only added first entry several times so have separate verification
                    ClassicAssert.AreEqual((byte)entryFlag, *(payloadPtr + currentEntry));
                    currentEntry++;
                }
            }
        }
        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanConsumerTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanDefault" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // Read the log - Look for the flag so know each entry is unique
            var consumer = new TestConsumer();
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress))
            {
                while (iter.TryConsumeNext(consumer)) { }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, consumer.currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ScanNoDefaultTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Test where all params are set just to make sure handles it ok

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanNoDefault" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress, recover: true, scanBufferingMode: DiskScanBufferingMode.DoublePageBuffering, scanUncommitted: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanByNameTest([Values] TestUtils.TestDeviceType deviceType)
        {
            //You can persist iterators(or more precisely, their CompletedUntilAddress) as part of a commit by simply naming them during their creation. 

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanByName" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress, recover: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanWithoutRecoverTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // You may also force an iterator to start at the specified begin address, i.e., without recovering: recover parameter = false

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanWithoutRecover" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Read the log 
            int currentEntry = 9;   // since starting at specified address of 1000, need to set current entry as 9 so verification starts at proper spot
            using (var iter = log.Scan(1000, LogAddress.MaxValidAddress, recover: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBufferingModeDoublePageTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Same as default, but do it just to make sure have test in case default changes

            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanDoublePage" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress, scanBufferingMode: DiskScanBufferingMode.DoublePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanBufferingModeSinglePageTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScanSinglePage" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateLog(log);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress, scanBufferingMode: DiskScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void ScanUncommittedTest([Values] TestUtils.TestDeviceType deviceType)
        {
            // Create log and device here (not in setup) because using DeviceType Enum which can't be used in Setup
            string filename = Path.Join(TestUtils.MethodTestDir, "LogScan" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
            PopulateUncommittedLog(log);

            // Setting scanUnCommitted to true is actual test here.
            // Read the log - Look for the flag so know each entry is unique and still reads uncommitted
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 20)]
        struct FakeStreamHeader
        {
            public long ms;
            public long seq;
            public int numPairs;
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ScanAcrossPageBoundaryWithHeaderTest()
        {
            // Verifies that Enqueue<THeader> + Scan works correctly across page boundaries
            // with a NullDevice. This exercises the same code path as Garnet Streams.
            var nullDevice = new NullDevice();
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = nullDevice,
                PageSize = 1L << 22,     // 4MB pages (same as Stream default)
                MemorySize = 1L << 30,   // 1GB
                SafeTailRefreshFrequencyMs = 0,
                AutoCommit = false,
            });

            // Simulate Stream entries: FakeStreamHeader (20 bytes) + serialized span (4+14=18 bytes)
            // Record size: 4 (header) + Align(38) = 4 + 40 = 44 bytes
            // 100K entries span ~2 pages at 4MB/page
            byte[] spanData = new byte[14];
            for (int i = 0; i < spanData.Length; i++)
                spanData[i] = (byte)(0x41 + (i % 26));

            const int count = 100000;
            long[] addresses = new long[count];

            for (int i = 0; i < count; i++)
            {
                var header = new FakeStreamHeader
                {
                    ms = i + 1,
                    seq = 0,
                    numPairs = 2
                };
                log.Enqueue(header, new ReadOnlySpan<byte>(spanData), out long addr);
                addresses[i] = addr;
            }

            while (log.SafeTailAddress < log.TailAddress)
                _ = Thread.Yield();

            long pageSize = 1L << 22;
            long firstValidPage1 = pageSize + PageHeader.Size;

            // Find the first entry on page 1
            int firstOnPage1 = -1;
            for (int i = 0; i < count; i++)
            {
                if (addresses[i] >= firstValidPage1)
                {
                    firstOnPage1 = i;
                    break;
                }
            }
            ClassicAssert.IsTrue(firstOnPage1 > 0, "Should have entries on page 1");

            // Verify entries on page 1 start after the page header
            ClassicAssert.AreEqual(firstValidPage1, addresses[firstOnPage1],
                "First entry on page 1 should start at offset PageHeader.Size");

            // Scan across the page boundary
            long scanStart = addresses[firstOnPage1 - 10];
            long scanEnd = addresses[Math.Min(firstOnPage1 + 10, count - 1)];

            int scannedCount = 0;
            int expectedHeaderSize = Marshal.SizeOf<FakeStreamHeader>();
            using (var iter = log.Scan(scanStart, scanEnd + 100, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] resultEntry, out int resultLen, out long currentAddr, out long nextAddr))
                {
                    ClassicAssert.IsTrue(resultLen >= expectedHeaderSize,
                        $"Entry at addr {currentAddr} has length {resultLen}, expected >= {expectedHeaderSize}");
                    scannedCount++;
                }
            }

            ClassicAssert.IsTrue(scannedCount >= 20, $"Expected at least 20 entries, got {scannedCount}");

            nullDevice.Dispose();
        }
    }
}