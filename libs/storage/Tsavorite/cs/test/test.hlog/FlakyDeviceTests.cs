// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
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
        //[Repeat(3000)]
        public async ValueTask FlakyLogTestCleanFailure([Values] bool isAsync)
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0.5,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.5,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "tsavoritelog.log"), deleteOnClose: true),
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
                        _ = log.Enqueue(entry);

                    if (isAsync)
                        await log.CommitAsync().ConfigureAwait(false);
                    else
                        log.Commit();
                }
            }
            catch (CommitFailureException e)
            {
                var errorRangeStart = e.LinkedCommitInfo.CommitInfo.FromAddress;
                ClassicAssert.LessOrEqual(log.CommittedUntilAddress, errorRangeStart);
                ClassicAssert.LessOrEqual(log.FlushedUntilAddress, errorRangeStart);
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
                readPermanentErrorRate = 0.5,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.5,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "tsavoritelog.log"), deleteOnClose: true),
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
                            _ = log.Enqueue(entry);
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
            ClassicAssert.IsTrue(failureList.Count == threads.Count);
            // They all observed the same failure
            foreach (var failure in failureList)
            {
                ClassicAssert.AreEqual(failure.LinkedCommitInfo.CommitInfo, failureList[0].LinkedCommitInfo.CommitInfo);
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask FlakyLogTestTolerateFailure([Values] IteratorType iteratorType)
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0.5,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.5,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "tsavoritelog.log"), deleteOnClose: true),
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
                _ = log.Enqueue(entry);
                try
                {
                    if (IsAsync(iteratorType))
                        await log.CommitAsync().ConfigureAwait(false);
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
                    await foreach ((byte[] result, int _, long _, long _ /*nextAddress*/) in iter.GetAsyncEnumerable().ConfigureAwait(false))
                        ClassicAssert.IsTrue(result.SequenceEqual(entry));
                    break;
                case IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, int _, long _, long _ /*nextAddress*/) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).ConfigureAwait(false))
                    {
                        ClassicAssert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                    }
                    break;
                case IteratorType.Sync:
                    while (iter.GetNext(out byte[] result, out _, out _))
                        ClassicAssert.IsTrue(result.SequenceEqual(entry));
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }
            recoveredLog.Dispose();
        }

        /// <summary>
        /// A device that throws synchronously from ReadAsync never delivers a completion callback for that read. The
        /// scan iterator must surface it as a failed page load rather than leaving the frame claimed but never loaded,
        /// which strands the scanning thread and any later waiter.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void ScanTerminatesWhenPageReadThrowsSynchronously([Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            var flakyDevice = new SyncThrowOnReadDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "tsavoritelog.log"), deleteOnClose: true));
            device = flakyDevice;
            var epoch = new LightEpoch();
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                LogChecksum = LogChecksumType.PerEntry,
                LogCommitManager = manager,
                PageSizeBits = 12,
                MemorySizeBits = 14,
                SegmentSizeBits = 20,
                Epoch = epoch
            });

            for (var i = 0; i < 1000; i++)
                _ = log.Enqueue(entry);
            log.Commit(spinWait: true);

            // The scan must read from disk: everything but the last couple of pages has been evicted from memory.
            ClassicAssert.Greater(log.TailAddress, 1 << 14);

            // Hold an older epoch on another thread so the iterator's page-read action is deferred through the drain
            // list and runs on that thread, as it does in a live server. A failure there must not escape into the
            // unrelated thread's drain pass.
            using var holderProtected = new ManualResetEventSlim(false);
            using var holderRelease = new ManualResetEventSlim(false);
            Exception drainException = null;
            var holderThread = new Thread(() =>
            {
                epoch.Resume();
                holderProtected.Set();
                _ = holderRelease.Wait(TimeSpan.FromSeconds(3));
                try
                {
                    for (var i = 0; i < 500 && !holderRelease.IsSet; i++)
                    {
                        epoch.ProtectAndDrain();
                        Thread.Sleep(1);
                    }
                }
                catch (Exception ex)
                {
                    drainException = ex;
                }
                finally
                {
                    epoch.Suspend();
                }
            })
            { IsBackground = true };
            holderThread.Start();
            ClassicAssert.IsTrue(holderProtected.Wait(TimeSpan.FromSeconds(10)));

            // Scan on a separate thread with a bounded wait so a stranded scanning thread fails the test
            // rather than hanging the run.
            Exception firstPass = null, secondPass = null;
            var entriesReadAfterRecovery = 0;
            using var scanDone = new ManualResetEventSlim(false);
            var scanThread = new Thread(() =>
            {
                try
                {
                    using var iter = log.Scan(0, log.TailAddress, scanBufferingMode: scanBufferingMode);
                    try
                    {
                        while (iter.GetNext(out _, out _, out _))
                            ;
                    }
                    catch (Exception ex)
                    {
                        firstPass = ex;
                    }

                    // The iterator must remain usable after the failed frames are released. Pages whose read was
                    // already in flight when the device recovered are still skipped, so tolerate cancellation while
                    // requiring the scan to make forward progress and terminate.
                    flakyDevice.ArmReadFailure = false;
                    for (var scanning = true; scanning;)
                    {
                        try
                        {
                            scanning = iter.GetNext(out _, out _, out _);
                            if (scanning)
                                ++entriesReadAfterRecovery;
                        }
                        catch (OperationCanceledException)
                        {
                        }
                    }
                }
                catch (Exception ex)
                {
                    secondPass = ex;
                }
                finally
                {
                    scanDone.Set();
                }
            })
            { IsBackground = true };

            flakyDevice.ArmReadFailure = true;
            scanThread.Start();

            var completed = scanDone.Wait(TimeSpan.FromSeconds(30));
            holderRelease.Set();
            _ = holderThread.Join(TimeSpan.FromSeconds(10));

            log.Dispose();
            log = null;
            epoch.Dispose();

            ClassicAssert.IsTrue(completed, "Scan did not terminate after the device failed the page read");
            ClassicAssert.IsInstanceOf<OperationCanceledException>(firstPass, $"Expected the failed page read to cancel the scan, got: {firstPass?.ToString() ?? "no exception"}");
            ClassicAssert.IsNull(secondPass, $"Iterator was left unusable after a failed page read: {secondPass}");
            ClassicAssert.Greater(entriesReadAfterRecovery, 0, "Iterator made no forward progress after the device recovered");
            ClassicAssert.IsNull(drainException, $"Page read failure escaped into an unrelated thread's epoch drain: {drainException}");
        }

        /// <summary>
        /// A page read that fails while it is still only a read-ahead (frame index &gt; 0) is published into
        /// <c>loadedPages</c> as though it had loaded, so the claim in <c>nextLoadedPages</c> does not stall the CAS
        /// loop. The failed frame must still refuse to hand back its contents when iteration reaches it: its buffer
        /// holds either nothing or the previous page's bytes. Requires double-page buffering, the only mode that
        /// issues read-ahead.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void ScanDoesNotReturnStaleDataWhenReadAheadPageFails([Values(0, 1, 2, 3, 4, 5, 6, 7)] int failingReadOrdinal)
        {
            var flakyDevice = new SyncThrowOnReadDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "tsavoritelog.log"), deleteOnClose: true));
            device = flakyDevice;
            var epoch = new LightEpoch();
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                LogChecksum = LogChecksumType.PerEntry,
                LogCommitManager = manager,
                PageSizeBits = 12,
                MemorySizeBits = 14,
                SegmentSizeBits = 20,
                Epoch = epoch
            });

            const int entryCount = 1000;
            ClassicAssert.GreaterOrEqual(entryLength, sizeof(int), "Entries must be wide enough to carry a unique index");

            // Give every entry a unique, self-identifying payload so the assertions can distinguish a skipped page
            // from a page backfilled with records from a different page.
            var payload = new byte[entryLength];
            for (var i = 0; i < entryCount; i++)
            {
                BitConverter.TryWriteBytes(payload, i);
                _ = log.Enqueue(payload);
            }
            log.Commit(spinWait: true);
            ClassicAssert.Greater(log.TailAddress, 1 << 14);

            var entriesRead = 0;
            var sawFailure = false;
            using var scanDone = new ManualResetEventSlim(false);
            Exception unexpected = null;
            string ordering = null;

            var scanThread = new Thread(() =>
            {
                try
                {
                    var lastIndex = -1;
                    using var iter = log.Scan(0, log.TailAddress, scanBufferingMode: DiskScanBufferingMode.DoublePageBuffering);
                    for (var scanning = true; scanning;)
                    {
                        try
                        {
                            scanning = iter.GetNext(out var result, out var length, out _);
                            if (!scanning)
                                continue;
                            ++entriesRead;

                            // Records may be missing when a page cannot be read, but each one delivered must be a
                            // real record, delivered once, in order.
                            var index = BitConverter.ToInt32(result, 0);
                            if (length != entryLength || index <= lastIndex || index >= entryCount)
                            {
                                ordering ??= $"Scan returned index {index} (length {length}) after {lastIndex}, which is duplicate, out of order, or not a valid record; a stale frame was surfaced as valid data";
                                scanning = false;
                                continue;
                            }
                            lastIndex = index;
                        }
                        catch (Exception ex) when (ex is OperationCanceledException or TsavoriteException)
                        {
                            sawFailure = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    unexpected = ex;
                }
                finally
                {
                    scanDone.Set();
                }
            })
            { IsBackground = true };

            flakyDevice.ThrowOnReadOrdinal = failingReadOrdinal;
            scanThread.Start();
            var completed = scanDone.Wait(TimeSpan.FromSeconds(30));

            log.Dispose();
            log = null;
            epoch.Dispose();

            ClassicAssert.IsTrue(completed, "Scan did not terminate after a read-ahead page read failed");
            ClassicAssert.IsNull(unexpected, $"Scan threw an unexpected exception: {unexpected}");
            ClassicAssert.IsNull(ordering, ordering);
            ClassicAssert.IsTrue(flakyDevice.ReadFailureInjected, "Fault injection never fired, so this test asserted nothing");

            // Entries may be lost when a page cannot be read, but never silently.
            if (entriesRead < entryCount)
                ClassicAssert.IsTrue(sawFailure, $"Scan silently returned {entriesRead} of {entryCount} entries after a read-ahead page read failed, with no error surfaced to the caller");
        }

        /// <summary>
        /// Exposes the protected metadata read so the failure path can be exercised directly.
        /// </summary>
        private sealed class TestableCheckpointManager : DeviceLogCommitCheckpointManager
        {
            public TestableCheckpointManager(INamedDeviceFactoryCreator creator, ICheckpointNamingScheme scheme)
                : base(creator, scheme) { }

            public byte[] ReadMetadata(IDevice device, int size)
            {
                ReadInto(device, 0, out var buffer, size);
                return buffer;
            }

            public void WriteMetadata(IDevice device, byte[] metadata)
                => WriteInto(device, 0, metadata, metadata.Length);
        }

        [Test]
        [Category("TsavoriteLog")]
        public void MetadataReadFailureIsReportedRatherThanReturningGarbage()
        {
            // A failed metadata read leaves the pooled buffer holding whatever it previously contained, so returning
            // it would have the caller parse arbitrary bytes as checkpoint metadata. The read must fail instead.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata.dat"), deleteOnClose: true);
            var failing = new ErrorCodeOnReadDevice(backing);
            using (failing)
            {
                failing.Initialize(1 << 20);
                manager.WriteMetadata(failing, [1, 2, 3, 4]);

                // The same read succeeds while the device is healthy, so the assertion below is about the error code
                // and not about the device being unusable.
                var healthy = manager.ReadMetadata(failing, sizeof(int));
                ClassicAssert.IsNotNull(healthy);

                failing.ReadErrorCode = 22;
                var ex = Assert.Throws<TsavoriteException>(() => manager.ReadMetadata(failing, sizeof(int)));
                StringAssert.Contains("22", ex.Message);
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public void MetadataReadToleratesEndOfFile()
        {
            // ReadInto rounds its length up to a sector, so it routinely asks for more bytes than the metadata file
            // holds. Windows reports that over-read as ERROR_HANDLE_EOF while Linux returns a short read. EOF must be
            // tolerated, and the untransferred bytes must read back as zeros rather than stale pooled-buffer content.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata-eof.dat"), deleteOnClose: true);
            var failing = new ErrorCodeOnReadDevice(backing);
            using (failing)
            {
                failing.Initialize(1 << 20);

                // Prime the pooled buffer with non-zero bytes so a stale-buffer regression cannot masquerade as zeros.
                manager.WriteMetadata(failing, [0x7F, 0x7F, 0x7F, 0x7F]);
                var primed = manager.ReadMetadata(failing, sizeof(int));
                ClassicAssert.AreNotEqual(0, BitConverter.ToInt32(primed, 0));

                const uint ErrorHandleEof = 38;
                failing.ReadErrorCode = ErrorHandleEof;

                byte[] afterEof = null;
                Assert.DoesNotThrow(() => afterEof = manager.ReadMetadata(failing, sizeof(int)));
                ClassicAssert.IsNotNull(afterEof);
                CollectionAssert.AreEqual(new byte[afterEof.Length], afterEof, "untransferred bytes must be zeroed, not stale pool contents");
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public void MetadataReadRejectsShortRead()
        {
            // A device that reports success but transfers fewer bytes than the caller asked for leaves the tail of
            // the buffer zeroed. Those zeros parse as metadata, so the short read must be reported instead.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata-short.dat"), deleteOnClose: true);
            var truncating = new ErrorCodeOnReadDevice(backing);
            using (truncating)
            {
                truncating.Initialize(1 << 20);

                var metadata = new byte[64];
                for (var i = 0; i < metadata.Length; i++)
                    metadata[i] = (byte)(i + 1);
                manager.WriteMetadata(truncating, metadata);

                // A read that stops short of the requested size but still covers it is the normal sector-rounded case.
                truncating.ShortReadBytes = metadata.Length;
                var full = manager.ReadMetadata(truncating, metadata.Length);
                ClassicAssert.IsNotNull(full);

                truncating.ShortReadBytes = metadata.Length - 1;
                var ex = Assert.Throws<TsavoriteException>(() => manager.ReadMetadata(truncating, metadata.Length));
                StringAssert.Contains("truncated or corrupt", ex.Message);
            }
        }
    }
}