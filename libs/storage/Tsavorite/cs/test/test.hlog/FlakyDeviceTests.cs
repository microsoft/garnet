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

            public byte[] ReadMetadata(IDevice device, int size, ulong address = 0)
            {
                ReadInto(device, address, out var buffer, size);
                return buffer;
            }

            public void WriteMetadata(IDevice device, byte[] metadata, ulong address = 0)
                => WriteInto(device, address, metadata, metadata.Length);
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
        public void ZeroLengthCommitMetadataIsRejected()
        {
            // ReadInto clears its buffer before reading, so a commit file that is empty or shorter than its length
            // prefix reads back as a zero length. Accepting that would present an empty commit as a valid one.
            // TestUtils.MethodTestDir is keyed on the method name alone, so give this manager its own directory.
            // Commit and GetCommitMetadata each open and close their own device, so the commit file must outlive
            // the device that wrote it.
            var directory = Path.Join(TestUtils.MethodTestDir, Guid.NewGuid().ToString("N"));
            using var manager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(directory));

            var metadata = new byte[64];
            for (var i = 0; i < metadata.Length; i++)
                metadata[i] = (byte)(i + 1);
            manager.Commit(beginAddress: 0, untilAddress: 64, metadata, commitNum: 0, forceWriteMetadata: true);

            // The reader returns the sector-padded body, so compare only the metadata that was written.
            var roundTripped = manager.GetCommitMetadata(0);
            CollectionAssert.AreEqual(metadata, roundTripped.AsSpan(0, metadata.Length).ToArray(), "a healthy commit must round-trip");

            manager.Commit(beginAddress: 0, untilAddress: 64, [], commitNum: 1, forceWriteMetadata: true);

            var ex = Assert.Throws<TsavoriteException>(() => manager.GetCommitMetadata(1));
            StringAssert.Contains("truncated or corrupt", ex.Message);
        }

        private static readonly TimeSpan MetadataIoTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Runs <paramref name="operation"/> on its own thread, recording the exception it threw, if any, and the
        /// sequence stamp at which it returned so its return can be ordered against its own IO completion.
        /// </summary>
        private static Thread StartMetadataOperation(GatedCompletionDevice gated, Action operation, Action<int> onReturn, Action<Exception> onFailure)
            => new(() =>
            {
                try
                {
                    operation();
                    onReturn(gated.NextSequence());
                }
                catch (Exception ex)
                {
                    onFailure(ex);
                }
            })
            { IsBackground = true };

        private static void JoinMetadataOperations(params Thread[] threads)
        {
            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(MetadataIoTimeout), "a metadata operation never completed; it is waiting for a completion that will not arrive");
        }

        /// <summary>
        /// Blocks until the operation thread has parked, which once its IO has reached the gate can only be the wait on
        /// its own completion. Establishes the order in which the threads entered that wait.
        /// </summary>
        private static void WaitUntilWaitingForCompletion(Thread thread)
        {
            var deadline = DateTime.UtcNow + MetadataIoTimeout;
            var spinWait = new SpinWait();
            while (!thread.ThreadState.HasFlag(ThreadState.WaitSleepJoin))
            {
                if (DateTime.UtcNow >= deadline)
                    Assert.Fail("a metadata operation never reached its completion wait");
                spinWait.SpinOnce();
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ConcurrentMetadataReadsDoNotConsumeEachOthersCompletions()
        {
            // Each metadata read must wait for its own completion. Sharing one completion signal across reads lets a
            // read wake on another read's completion, copy a buffer the device has not filled yet, and return that
            // buffer to the pool while its own read is still outstanding.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata-concurrent-reads.dat"), deleteOnClose: true);
            using var gated = new GatedCompletionDevice(backing);
            gated.Initialize(1 << 20);

            byte[] metadata = [1, 2, 3, 4];
            manager.WriteMetadata(gated, metadata);

            gated.Gate = true;

            var results = new byte[2][];
            var returnStamps = new int[2];
            var failures = new Exception[2];
            var threads = new Thread[2];
            for (var i = 0; i < threads.Length; i++)
            {
                var index = i;
                threads[index] = StartMetadataOperation(gated,
                    () => results[index] = manager.ReadMetadata(gated, sizeof(int)),
                    stamp => returnStamps[index] = stamp,
                    ex => failures[index] = ex);
            }

            // Start the reads one at a time so each read's gate index matches its thread index. The first read must
            // be parked in its wait before the second is issued.
            threads[0].Start();
            gated.WaitForPending(1, MetadataIoTimeout);
            WaitUntilWaitingForCompletion(threads[0]);

            // Hold the second reader inside the device call, after its read is captured but before it can reach its
            // own wait. SemaphoreSlim does not specify which waiter a release wakes, so without this the shared
            // semaphore could hand completion 1 to read 1 and leave the theft unobserved; with exactly one waiter
            // there is no such choice to make.
            gated.HoldCallersAfterCapture();
            threads[1].Start();
            gated.WaitForPending(2, MetadataIoTimeout);

            // Deliver the second read's completion while only the first read is waiting. A shared completion signal
            // releases the first read here, with its own IO not yet issued and its buffer still zeroed.
            gated.Release(1);
            gated.WaitForCompletion(1, MetadataIoTimeout);

            gated.ReleaseHeldCallers();
            gated.Release(0);

            JoinMetadataOperations(threads);

            for (var i = 0; i < threads.Length; i++)
            {
                ClassicAssert.IsNull(failures[i], $"read {i} failed: {failures[i]}");
                CollectionAssert.AreEqual(metadata, results[i].AsSpan(0, metadata.Length).ToArray(),
                    $"read {i} returned a buffer that its own read had not filled");
                ClassicAssert.Greater(returnStamps[i], gated.CompletionStamp(i),
                    $"read {i} returned before its own completion fired, so its pooled buffer was recycled while its read was still outstanding");
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public void MetadataWriteDoesNotConsumeAConcurrentReadsCompletion()
        {
            // Reads and writes share the same completion machinery, so a write can wake on a read's completion and
            // return its pooled buffer while the device is still reading from it. The pool zeroes a returned buffer,
            // so the write then lands as zeros over valid metadata.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata-read-write.dat"), deleteOnClose: true);
            using var gated = new GatedCompletionDevice(backing);
            gated.Initialize(1 << 20);

            // The read and the write target different sectors so neither test expectation depends on which of the two
            // reaches the device first.
            var writeAddress = (ulong)gated.SectorSize;
            byte[] seed = [9, 9, 9, 9];
            byte[] written = [1, 2, 3, 4];
            manager.WriteMetadata(gated, seed);

            gated.Gate = true;

            byte[] readResult = null;
            var returnStamps = new int[2];
            var failures = new Exception[2];

            var writer = StartMetadataOperation(gated,
                () => manager.WriteMetadata(gated, written, writeAddress),
                stamp => returnStamps[0] = stamp,
                ex => failures[0] = ex);
            var reader = StartMetadataOperation(gated,
                () => readResult = manager.ReadMetadata(gated, sizeof(int)),
                stamp => returnStamps[1] = stamp,
                ex => failures[1] = ex);

            writer.Start();
            gated.WaitForPending(1, MetadataIoTimeout);
            WaitUntilWaitingForCompletion(writer);

            // As in the concurrent-reads case, hold the reader after capture so the write is the only waiter when the
            // read's completion arrives. Relying on the writer having waited first would depend on SemaphoreSlim's
            // unspecified wake order.
            gated.HoldCallersAfterCapture();
            reader.Start();
            gated.WaitForPending(2, MetadataIoTimeout);

            // Deliver the read's completion while only the write is waiting.
            gated.Release(1);
            gated.WaitForCompletion(1, MetadataIoTimeout);

            gated.ReleaseHeldCallers();
            gated.Release(0);

            JoinMetadataOperations(writer, reader);

            ClassicAssert.IsNull(failures[0], $"the metadata write failed: {failures[0]}");
            ClassicAssert.IsNull(failures[1], $"the metadata read failed: {failures[1]}");
            ClassicAssert.Greater(returnStamps[0], gated.CompletionStamp(0),
                "the metadata write returned before its own completion fired, so its pooled buffer was recycled while the device was still reading from it");
            ClassicAssert.Greater(returnStamps[1], gated.CompletionStamp(1),
                "the metadata read returned before its own completion fired");
            CollectionAssert.AreEqual(seed, readResult.AsSpan(0, seed.Length).ToArray(),
                "the metadata read returned a buffer that its own read had not filled");

            gated.Gate = false;
            var roundTripped = manager.ReadMetadata(gated, sizeof(int), writeAddress);
            CollectionAssert.AreEqual(written, roundTripped.AsSpan(0, written.Length).ToArray(),
                "the metadata write landed corrupted, so its buffer was recycled before the device finished with it");
        }

        [Test]
        [Category("TsavoriteLog")]
        public void MetadataOperationDoesNotConsumeAnotherOperationsError()
        {
            // A failing operation's error code belongs to that operation alone. Holding it on the manager lets a
            // healthy read report a failure that happened to a different read, and lets one operation reset an error
            // another operation has not observed yet.
            using var manager = new TestableCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: true),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir));

            var backing = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "metadata-error-isolation.dat"), deleteOnClose: true);
            using var gated = new GatedCompletionDevice(backing);
            gated.Initialize(1 << 20);

            byte[] metadata = [1, 2, 3, 4];
            manager.WriteMetadata(gated, metadata);

            gated.Gate = true;

            var results = new byte[2][];
            var failures = new Exception[2];
            var threads = new Thread[2];
            for (var i = 0; i < threads.Length; i++)
            {
                var index = i;
                threads[index] = StartMetadataOperation(gated,
                    () => results[index] = manager.ReadMetadata(gated, sizeof(int)),
                    _ => { },
                    ex => failures[index] = ex);
            }

            threads[0].Start();
            gated.WaitForPending(1, MetadataIoTimeout);
            WaitUntilWaitingForCompletion(threads[0]);
            threads[1].Start();
            gated.WaitForPending(2, MetadataIoTimeout);
            WaitUntilWaitingForCompletion(threads[1]);

            // 22 is neither success nor the tolerated ERROR_HANDLE_EOF, so only the first read may report it.
            gated.Release(0, errorCode: 22);
            gated.WaitForCompletion(0, MetadataIoTimeout);
            gated.Release(1);

            JoinMetadataOperations(threads);

            ClassicAssert.IsNotNull(failures[0], "the failed read did not report its own error");
            StringAssert.Contains("22", failures[0].Message);
            ClassicAssert.IsNull(failures[1], $"a healthy read reported another operation's error: {failures[1]}");
            CollectionAssert.AreEqual(metadata, results[1].AsSpan(0, metadata.Length).ToArray(),
                "the healthy read returned a buffer that its own read had not filled");
        }
    }
}