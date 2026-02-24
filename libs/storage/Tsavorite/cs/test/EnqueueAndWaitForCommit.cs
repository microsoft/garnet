// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.IO;
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
    internal class EnqWaitCommitTest : AllureTestBase
    {
        const int entryLength = 500;
        const int numEntries = 100;

        public TsavoriteLog log;
        public IDevice device;
        static byte[] entry;
        static ReadOnlySpanBatch spanBatch;

        public enum EnqueueIteratorType
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
            entry = new byte[entryLength];
            spanBatch = new(numEntries);

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "EnqueueAndWaitForCommit.log"), deleteOnClose: true);
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask EnqueueWaitCommitBasicTest([Values] EnqueueIteratorType iteratorType)
        {
            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            // Add to TsavoriteLog on a separate thread, which will wait for the commit from this thread
            var currentTask = Task.Run(() => LogWriter(log, entry, iteratorType));

            // Delay so LogWriter's call to EnqueueAndWaitForCommit gets into its spinwait for the Commit.
            await Task.Delay(100);

            // Commit to the log and wait for task to finish
            log.Commit(true);
            await currentTask;

            // Read the log - Look for the flag so know each entry is unique
            using var iter = log.Scan(0, LogAddress.MaxValidAddress);
            int currentEntry = 0;
            while (iter.GetNext(out byte[] result, out _, out _))
            {
                ClassicAssert.Less(currentEntry, entryLength);
                ClassicAssert.AreEqual((byte)currentEntry, result[currentEntry]);
                currentEntry++;
            }

            ClassicAssert.AreNotEqual(0, currentEntry, "Failure -- data loop after log.Scan never entered so wasn't verified.");
        }

        public static void LogWriter(TsavoriteLog log, byte[] entry, EnqueueIteratorType iteratorType)
        {
            try
            {
                long returnLogicalAddress = iteratorType switch
                {
                    EnqueueIteratorType.Byte => log.EnqueueAndWaitForCommit(entry),
                    EnqueueIteratorType.SpanByte => // Could slice the span but for basic test just pass span of full entry - easier verification
                                                    log.EnqueueAndWaitForCommit((Span<byte>)entry),
                    EnqueueIteratorType.SpanBatch => log.EnqueueAndWaitForCommit(spanBatch),
                    _ => throw new ApplicationException("Test failure: Unknown EnqueueIteratorType")
                };

                ClassicAssert.AreNotEqual(0, returnLogicalAddress, "LogWriter: Returned Logical Address = 0");
            }
            catch (Exception ex)
            {
                Assert.Fail("EnqueueAndWaitForCommit had exception:" + ex.Message);
            }
        }

    }
}