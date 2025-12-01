// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    //** Fundamental basic test for TryEnqueue that covers all the parameters in TryEnqueue
    //** Other tests in TsavoriteLog.cs provide more coverage for TryEnqueue

    [TestFixture]
    internal class TryEnqueueTests
    {
        private TsavoriteLog log;
        private IDevice device;
        static readonly byte[] entry = new byte[100];

        public enum TryEnqueueIteratorType
        {
            Byte,
            SpanBatch,
            SpanByte
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
        public void TryEnqueueBasicTest([Values] TryEnqueueIteratorType iteratorType, [Values] TestUtils.TestDeviceType deviceType)
        {
            int entryLength = 50;
            int numEntries = 10000;
            int entryFlag = 9999;

            // Create devices \ log for test
            string filename = Path.Join(TestUtils.MethodTestDir, "TryEnqueue" + deviceType.ToString() + ".log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });

            // Issue with Non Async Commit and Emulated Azure so don't run it - at least put after device creation to see if crashes doing that simple thing
            if (OperatingSystem.IsWindows() && deviceType == TestUtils.TestDeviceType.EmulatedAzure)
                return;

            // Reduce SpanBatch to make sure entry fits on page
            if (iteratorType == TryEnqueueIteratorType.SpanBatch)
            {
                entryLength = 10;
                numEntries = 50;
            }

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);

            // TryEnqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                bool appendResult = false;
                long logicalAddress = 0;
                long ExpectedOutAddress = 0;

                // Flag one part of entry data that corresponds to index
                if (i < entryLength)
                    entry[i] = (byte)entryFlag;

                // puts back the previous entry value
                if ((i > 0) && (i < entryLength))
                    entry[i - 1] = (byte)(i - 1);

                // Add to TsavoriteLog
                switch (iteratorType)
                {
                    case TryEnqueueIteratorType.Byte:
                        // Default is add bytes so no need to do anything with it
                        appendResult = log.TryEnqueue(entry, out logicalAddress);
                        break;
                    case TryEnqueueIteratorType.SpanByte:
                        // Could slice the span but for basic test just pass span of full entry - easier verification
                        Span<byte> spanEntry = entry;
                        appendResult = log.TryEnqueue(spanEntry, out logicalAddress);
                        break;
                    case TryEnqueueIteratorType.SpanBatch:
                        appendResult = log.TryEnqueue(spanBatch, out logicalAddress);
                        break;
                    default:
                        Assert.Fail("Unknown TryEnqueueIteratorType");
                        break;
                }

                // Verify each Enqueue worked
                ClassicAssert.IsTrue(appendResult, "Fail - TryEnqueue failed with a 'false' result for entry: " + i.ToString());

                // logical address has new entry every x bytes which is one entry less than the TailAddress
                if (iteratorType == TryEnqueueIteratorType.SpanBatch)
                    ExpectedOutAddress = log.TailAddress - 5200;
                else
                    ExpectedOutAddress = log.TailAddress - 104;

                ClassicAssert.AreEqual(ExpectedOutAddress, logicalAddress);
            }

            // Commit to the log
            log.Commit(true);

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, LogAddress.MaxValidAddress))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // Span Batch only added first entry several times so have separate verification
                        if (iteratorType == TryEnqueueIteratorType.SpanBatch)
                            ClassicAssert.AreEqual((byte)entryFlag, result[0]);
                        else
                            ClassicAssert.AreEqual((byte)entryFlag, result[currentEntry]);

                        currentEntry++;
                    }
                }
            }

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

    }
}