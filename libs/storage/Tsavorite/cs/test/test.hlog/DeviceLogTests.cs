// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.test
{
    [TestFixture]
    internal class DeviceLogTests : TestBase
    {
        const int entryLength = 100;
        const int numEntries = 1000;
        private TsavoriteLog log;
        static readonly byte[] entry = new byte[100];

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask PageBlobTsavoriteLogTest1([Values] LogChecksumType logChecksum, [Values] TsavoriteLogTestBase.IteratorType iteratorType)
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "Tsavoritelog.log", deleteOnClose: true, logger: TestUtils.TestLoggerFactory.CreateLogger("asd"));
            var checkpointManager = new DeviceLogCommitCheckpointManager(
                TestUtils.AzureStorageNamedDeviceFactoryCreator,
                new AzureCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            await TsavoriteLogTest1(logChecksum, device, checkpointManager, iteratorType).ConfigureAwait(false);
            device.Dispose();
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask PageBlobTsavoriteLogTestWithLease([Values] LogChecksumType logChecksum, [Values] TsavoriteLogTestBase.IteratorType iteratorType)
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "TsavoritelogLease.log", deleteOnClose: true, underLease: true, blobManager: null, logger: TestUtils.TestLoggerFactory.CreateLogger("asd"));
            var checkpointManager = new DeviceLogCommitCheckpointManager(
                TestUtils.AzureStorageNamedDeviceFactoryCreator,
                new AzureCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            await TsavoriteLogTest1(logChecksum, device, checkpointManager, iteratorType).ConfigureAwait(false);
            device.Dispose();
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }


        [Test]
        [Category("TsavoriteLog")]
        public void BasicHighLatencyDeviceTest()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Create devices \ log for test for in memory device
            using var device = new LocalMemoryDevice(capacity: 1L << 28, segmentSize: 1L << 25, 2, latencyUs: TestUtils.DefaultLocalMemoryDeviceLatencyUs, fileName: Path.Join(TestUtils.MethodTestDir, "test.log"));
            using var LocalMemorylog = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null });

            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                _ = LocalMemorylog.Enqueue(entry);
            }

            // Commit to the log
            LocalMemorylog.Commit(true);

            // Read the log just to verify was actually committed
            int currentEntry = 0;
            using var iter = LocalMemorylog.Scan(0, 100_000_000);
            while (iter.GetNext(out byte[] result, out _, out _))
            {
                ClassicAssert.IsTrue(result[currentEntry] == currentEntry, "Fail - Result[" + currentEntry.ToString() + "]: is not same as " + currentEntry.ToString());
                currentEntry++;
            }
        }

        private async ValueTask TsavoriteLogTest1(LogChecksumType logChecksum, IDevice device, ILogCommitManager logCommitManager, TsavoriteLogTestBase.IteratorType iteratorType)
        {
            var logSettings = new TsavoriteLogSettings { PageSizeBits = 20, SegmentSizeBits = 20, LogDevice = device, LogChecksum = logChecksum, LogCommitManager = logCommitManager, TryRecoverLatest = false };
            log = TsavoriteLogTestBase.IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings).ConfigureAwait(false) : new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                _ = log.Enqueue(entry);
            }

            log.CompleteLog(true);

            // MoveNextAsync() would hang at TailAddress, waiting for more entries (that we don't add).
            // Note: If this happens and the test has to be canceled, there may be a leftover blob from the log.Commit(), because
            // the log device isn't Dispose()d; the symptom is currently a numeric string format error in DefaultCheckpointNamingScheme.
            using (var iter = log.Scan(0, long.MaxValue))
            {
                var counter = new TsavoriteLogTestBase.Counter(log);

                switch (iteratorType)
                {
                    case TsavoriteLogTestBase.IteratorType.AsyncByteVector:
                        await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable().ConfigureAwait(false))
                        {
                            ClassicAssert.IsTrue(result.SequenceEqual(entry));
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);
                        }
                        break;
                    case TsavoriteLogTestBase.IteratorType.AsyncMemoryOwner:
                        await foreach ((IMemoryOwner<byte> result, _, _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                        {
                            ClassicAssert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                            result.Dispose();
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);
                        }
                        break;
                    case TsavoriteLogTestBase.IteratorType.Sync:
                        while (iter.GetNext(out byte[] result, out _, out _))
                        {
                            ClassicAssert.IsTrue(result.SequenceEqual(entry));
                            counter.IncrementAndMaybeTruncateUntil(iter.NextAddress);
                        }
                        break;
                    default:
                        Assert.Fail("Unknown IteratorType");
                        break;
                }
                ClassicAssert.IsTrue(counter.count == numEntries);
            }

            log.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void LocalMemoryDeviceReentrantWriteDoesNotDeadlock()
        {
            // A device IO-completion callback that synchronously re-issues IO runs on the device's
            // single drain thread (this mirrors Tsavorite's AsyncFlushPageCallback flush chains). With a
            // bounded submission ring, that re-entrant submit must NOT block waiting on a full ring that
            // only this same thread can drain — otherwise the drain thread waits on itself (deadlock).
            // A tiny ring plus a flooding producer keep the ring full so the condition is exercised.
            var device = new LocalMemoryDevice(capacity: 1L << 20, segmentSize: 1L << 20, parallelism: 1,
                latencyUs: 50, sectorSize: 64, ringCapacity: 8,
                fileName: Path.Join(TestUtils.MethodTestDir, "reentrant.log"));

            var pin = GCHandle.Alloc(new byte[4096], GCHandleType.Pinned);
            var addr = pin.AddrOfPinnedObject();
            long completed = 0;
            var reentrantLeft = 200;
            DeviceIOCompletionCallback cb = null;
            cb = (errorCode, numBytes, context) =>
            {
                _ = Interlocked.Increment(ref completed);
                // Re-issue from within the completion (runs on the drain thread) — the deadlock trigger.
                if (Interlocked.Decrement(ref reentrantLeft) >= 0)
                    device.WriteAsync(addr, 0, 0, 64, cb, null);
            };

            var stop = false;
            var flood = new Thread(() =>
            {
                while (!Volatile.Read(ref stop))
                    device.WriteAsync(addr, 0, 0, 64, cb, null);
            })
            { IsBackground = true, Name = "reentrant-flood" };
            flood.Start();

            var sw = Stopwatch.StartNew();
            var deadlocked = false;
            while (Interlocked.Read(ref completed) < 5000)
            {
                if (sw.Elapsed.TotalSeconds > 15) { deadlocked = true; break; }
                Thread.Sleep(20);
            }
            Volatile.Write(ref stop, true);

            // On deadlock, the drain thread is wedged: skip Dispose/Free (both could block or race the
            // stuck thread) and let the daemon threads die with the test host. Only clean up on success.
            if (!deadlocked)
            {
                _ = flood.Join(TimeSpan.FromSeconds(5));
                device.Dispose();
                pin.Free();
            }
            ClassicAssert.IsFalse(deadlocked, "LocalMemoryDevice deadlocked on a re-entrant write into a full ring");
        }
    }
}