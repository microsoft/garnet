// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class FlakyDeviceTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup(false);

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask FlakyLogTestCleanFailure([Values] bool isAsync)
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.1,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(path + "tsavoritelog.log", deleteOnClose: true),
                errorOptions);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            try
            {
                // Ensure we execute long enough to trigger errors
                for (int j = 0; j < 100; j++)
                {
                    for (int i = 0; i < numEntries; i++)
                    {
                        log.Enqueue(entry);
                    }

                    if (isAsync)
                        await log.CommitAsync();
                    else
                        log.Commit();
                }
            }
            catch (CommitFailureException e)
            {
                var errorRangeStart = e.LinkedCommitInfo.CommitInfo.FromAddress;
                Assert.LessOrEqual(log.CommittedUntilAddress, errorRangeStart);
                Assert.LessOrEqual(log.FlushedUntilAddress, errorRangeStart);
                return;
            }

            // Should not ignore failures
            Assert.Fail();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void FlakyLogTestConcurrentWriteFailure()
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.05,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(path + "tsavoritelog.log", deleteOnClose: true),
                errorOptions);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            var failureList = new List<CommitFailureException>();
            ThreadStart runTask = () =>
            {
                var random = new Random();
                try
                {
                    // Ensure we execute long enough to trigger errors
                    for (int j = 0; j < 100; j++)
                    {
                        for (int i = 0; i < numEntries; i++)
                        {
                            log.Enqueue(entry);
                            // create randomly interleaved concurrent writes
                            if (random.NextDouble() < 0.1)
                                log.Commit();
                        }
                    }
                }
                catch (CommitFailureException e)
                {
                    lock (failureList)
                        failureList.Add(e);
                }
            };

            var threads = new List<Thread>();
            for (var i = 0; i < Environment.ProcessorCount / 2; i++)
            {
                var t = new Thread(runTask);
                t.Start();
                threads.Add(t);
            }

            foreach (var thread in threads)
                thread.Join();

            // Every thread observed the failure
            Assert.IsTrue(failureList.Count == threads.Count);
            // They all observed the same failure
            foreach (var failure in failureList)
            {
                Assert.AreEqual(failure.LinkedCommitInfo.CommitInfo, failureList[0].LinkedCommitInfo.CommitInfo);
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask FlakyLogTestTolerateFailure([Values] IteratorType iteratorType)
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.1,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(path + "tsavoritelog.log", deleteOnClose: true),
                errorOptions);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, TolerateDeviceFailure = true };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            // Ensure we write enough to trigger errors
            for (int i = 0; i < 1000; i++)
            {
                log.Enqueue(entry);
                try
                {
                    if (IsAsync(iteratorType))
                        await log.CommitAsync();
                    else
                        log.Commit();
                }
                catch (CommitFailureException)
                {
                    // Ignore failure
                }
            }

            // For surviving entries, scan should still work best-effort
            // If endAddress > log.TailAddress then GetAsyncEnumerable() will wait until more entries are added.
            var endAddress = IsAsync(iteratorType) ? log.CommittedUntilAddress : long.MaxValue;
            var recoveredLog = new TsavoriteLog(logSettings);
            using var iter = recoveredLog.Scan(0, endAddress);
            switch (iteratorType)
            {
                case IteratorType.AsyncByteVector:
                    await foreach ((byte[] result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable())
                        Assert.IsTrue(result.SequenceEqual(entry));
                    break;
                case IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                    {
                        Assert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                    }
                    break;
                case IteratorType.Sync:
                    while (iter.GetNext(out byte[] result, out _, out _))
                        Assert.IsTrue(result.SequenceEqual(entry));
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }
            recoveredLog.Dispose();
        }

    }
}