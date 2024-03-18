// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogShiftTailStressTest : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public void TsavoriteLogShiftTailStressTest()
        {
            // Get an excruciatingly slow storage device to maximize chance of clogging the flush pipeline
            device = new LocalMemoryDevice(1L << 28, 1 << 28, 2, sector_size: 512, latencyMs: 50, fileName: "stress.log");
            var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.None, LogCommitManager = manager, SegmentSizeBits = 28 };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < 5 * numEntries; i++)
                log.Enqueue(entry);

            // for comparison, insert some entries without any commit records
            var referenceTailLength = log.TailAddress;

            var enqueueDone = new ManualResetEventSlim();
            var commitThreads = new List<Thread>();
            // Make sure to spin up many commit threads to expose lots of interleavings
            for (var i = 0; i < Math.Max(1, Environment.ProcessorCount / 2); i++)
            {
                commitThreads.Add(new Thread(() =>
                {
                    // Otherwise, absolutely clog the commit pipeline
                    while (!enqueueDone.IsSet)
                        log.Commit();
                }));
            }

            foreach (var t in commitThreads)
                t.Start();
            for (int i = 0; i < 5 * numEntries; i++)
            {
                log.Enqueue(entry);
            }
            enqueueDone.Set();

            foreach (var t in commitThreads)
                t.Join();

            // We expect the test to finish and not get stuck somewhere

            // Ensure clean shutdown
            log.Commit(true);
        }
    }
}