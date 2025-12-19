// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [AllureNUnit]
    [TestFixture]
    internal class EnqueueTests : AllureTestBase
    {
        private TsavoriteLog log;
        private IDevice device;
        static byte[] entry;

        public enum EnqueueIteratorType
        {
            Byte,
            SpanBatch,
            SpanByte,
            IEntry
        }

        private class ByteArrayEnqueueEntry : ILogEnqueueEntry
        {
            public int SerializedLength => entry.Length;

            public void SerializeTo(Span<byte> dest)
            {
                entry.CopyTo(dest);
            }
        }

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => entry;
            public int TotalEntries() => batchSize;
        }

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

            // Clean up log files
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void EnqueueBasicTest([Values] EnqueueIteratorType iteratorType, [Values] TestUtils.TestDeviceType deviceType)
        {

            int entryLength = 20;
            int numEntries = 500;
            int entryFlag = 9999;

            string filename = Path.Join(TestUtils.MethodTestDir, "Enqueue" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir }); // Needs to match what is set in TestUtils.CreateTestDevice 

            // Reduce SpanBatch to make sure entry fits on page
            if (iteratorType == EnqueueIteratorType.SpanBatch)
            {
                entryLength = 5;
                numEntries = 200;
            }

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);
            var ientry = new ByteArrayEnqueueEntry();
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
                switch (iteratorType)
                {
                    case EnqueueIteratorType.Byte:
                        // Default is add bytes so no need to do anything with it
                        log.Enqueue(entry);
                        break;
                    case EnqueueIteratorType.SpanByte:
                        // Could slice the span but for basic test just pass span of full entry - easier verification
                        Span<byte> spanEntry = entry;
                        log.Enqueue(spanEntry);
                        break;
                    case EnqueueIteratorType.SpanBatch:
                        log.Enqueue(spanBatch);
                        break;
                    case EnqueueIteratorType.IEntry:
                        log.Enqueue(ientry);
                        break;
                    default:
                        Assert.Fail("Unknown EnqueueIteratorType");
                        break;
                }
            }

            // Commit to the log
            log.Commit(true);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        if (iteratorType == EnqueueIteratorType.SpanBatch)
                        {
                            ClassicAssert.AreEqual((byte)entryFlag, result[0]);
                        }
                        else
                        {
                            ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);
                        }

                        currentEntry++;
                    }
                }
            }

            // Make sure expected length (entryLength) is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);

        }


        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async Task EnqueueAsyncBasicTest([Values] TestUtils.TestDeviceType deviceType)
        {

            const int expectedEntryCount = 11;

            string filename = Path.Join(TestUtils.MethodTestDir, "EnqueueAsyncBasic" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });

            if (OperatingSystem.IsWindows() && deviceType == TestUtils.TestDeviceType.EmulatedAzure)
                return;

            CancellationToken cancellationToken = default;
            ReadOnlyMemory<byte> readOnlyMemoryEntry = entry;
            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(5);
            var ientry = new ByteArrayEnqueueEntry();

            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };

            await log.EnqueueAsync(input1, cancellationToken);
            await log.EnqueueAsync(input2);
            await log.EnqueueAsync(input3);
            await log.EnqueueAsync(readOnlyMemoryEntry);
            await log.EnqueueAsync(ientry);
            await log.EnqueueAsync(spanBatch);
            await log.CommitAsync();

            // Read the log to make sure all entries are put in
            int currentEntry = 1;
            using (var iter = log.Scan(0, long.MaxValue))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {

                    // Verify based on which input read
                    switch (currentEntry)
                    {
                        case 1:
                            // result compared to input1
                            ClassicAssert.IsTrue(result.SequenceEqual(input1), "Fail - Result does not equal Input1. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;
                        case 2:
                            ClassicAssert.IsTrue(result.SequenceEqual(input2), "Fail - Result does not equal Input2. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;
                        case 3:
                            ClassicAssert.IsTrue(result.SequenceEqual(input3), "Fail - Result does not equal Input3. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;
                        case 4:
                            ClassicAssert.IsTrue(result.SequenceEqual(entry), "Fail - Result does not equal ReadOnlyMemoryEntry. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;
                        case 5:
                            ClassicAssert.IsTrue(result.SequenceEqual(entry), "Fail - Result does not equal SpanBatchEntry. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;
                        case 6:
                            ClassicAssert.IsTrue(result.SequenceEqual(entry), "Fail - Result does not equal SpanBatchEntry. result[0]=" + result[0].ToString() + "  result[1]=" + result[1].ToString());
                            break;

                    }
                    currentEntry++;

                }

                // Make sure expected length is same as current - also makes sure that data verification was not skipped
                ClassicAssert.AreEqual(expectedEntryCount, currentEntry);
            }
        }
    }
}