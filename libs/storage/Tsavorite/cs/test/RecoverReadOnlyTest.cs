// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

//** Note - this test is based on TsavoriteLogPubSub sample found in the samples directory.

namespace Tsavorite.test
{
    [TestFixture]
    internal class BasicRecoverReadOnly
    {
        private TsavoriteLog log;
        private IDevice device;
        private TsavoriteLog logReadOnly;
        private IDevice deviceReadOnly;

        private static string path;
        const int commitPeriodMs = 2000;
        const int restorePeriodMs = 1000;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "Recover", deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 });
            deviceReadOnly = Devices.CreateLogDevice(path + "RecoverReadOnly");
            logReadOnly = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;
            logReadOnly?.Dispose();
            logReadOnly = null;
            deviceReadOnly?.Dispose();
            deviceReadOnly = null;

            // Clean up log files
            TestUtils.DeleteDirectory(path);
        }


        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void RecoverReadOnlyAsyncBasicTest()
        {
            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var commiter = CommitterAsync(log, cts.Token);

            // Run consumer on SEPARATE read-only TsavoriteLog instance
            var consumer = SeparateConsumerAsync(cts.Token);

            //** Give it some time to run a bit
            //** Acceptable use of using sleep for this spot
            //** Similar to waiting for things to run before manually hitting cancel from a command prompt
            Thread.Sleep(3000);
            cts.Cancel();

            producer.Wait();
            // commiter.Wait();  // cancel token took care of this one
            // consumer.Wait();  // cancel token took care of this one

        }


        //**** Helper Functions - based off of TsavoriteLogPubSub sample ***
        static async Task CommitterAsync(TsavoriteLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(commitPeriodMs), cancellationToken);
                await log.CommitAsync(token: cancellationToken);
            }
        }

        static async Task ProducerAsync(TsavoriteLog log, CancellationToken cancellationToken)
        {
            var i = 0L;
            while (!cancellationToken.IsCancellationRequested)
            {
                log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));

                i++;

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        // This creates a separate TsavoriteLog over the same log file, using RecoverReadOnly to continuously update
        // to the primary TsavoriteLog's commits.
        public async Task SeparateConsumerAsync(CancellationToken cancellationToken)
        {
            var _ = BeginRecoverReadOnlyLoop(logReadOnly, cancellationToken);

            // This enumerator waits asynchronously when we have reached the committed tail of the duplicate TsavoriteLog. When RecoverReadOnly
            // reads new data committed by the primary TsavoriteLog, it signals commit completion to let iter continue to the new tail.
            using var iter = logReadOnly.Scan(logReadOnly.BeginAddress, long.MaxValue);
            await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
                iter.CompleteUntil(nextAddress);
            }
        }

        static async Task BeginRecoverReadOnlyLoop(TsavoriteLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Delay for a while before checking again.
                await Task.Delay(TimeSpan.FromMilliseconds(restorePeriodMs), cancellationToken);
                await log.RecoverReadOnlyAsync(cancellationToken);
            }
        }
    }
}