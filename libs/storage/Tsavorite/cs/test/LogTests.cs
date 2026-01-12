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
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.test
{
    [TestFixture]
    internal class TsavoriteLogStandAloneTests
    {
        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void TestDisposeReleasesFileLocksWithCompletedCommit([Values] TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, "TestDisposeRelease" + deviceType.ToString() + ".log");

            _ = Directory.CreateDirectory(MethodTestDir);
            IDevice device = CreateTestDevice(deviceType, filename);
            TsavoriteLog log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                SegmentSizeBits = 22,
                LogCommitDir = MethodTestDir,
                LogChecksum = LogChecksumType.PerEntry
            });

            ClassicAssert.IsTrue(log.TryEnqueue(new byte[100], out _));

            log.Commit(spinWait: true);
            log.Dispose();
            device.Dispose();
            DeleteDirectory(MethodTestDir, wait: true);
        }
    }

    // This test base class allows splitting up the tests into separate fixtures that can be run in parallel
    internal class TsavoriteLogTestBase
    {
        protected const int entryLength = 100;
        protected const int numEntries = 10000; //1000000;
        protected const int numSpanEntries = 500; // really slows down if go too many
        protected TsavoriteLog log;
        protected IDevice device;
        protected DeviceLogCommitCheckpointManager manager;

        protected static readonly byte[] entry = new byte[100];
        protected static readonly ReadOnlySpanBatch spanBatch = new(10000);

        private bool deleteOnClose;

        protected struct ReadOnlySpanBatch(int batchSize) : IReadOnlySpanBatch
        {
            private readonly int batchSize = batchSize;

            public readonly ReadOnlySpan<byte> Get(int index) => entry;
            public readonly int TotalEntries() => batchSize;
        }

        protected void BaseSetup(bool deleteOnClose = true)
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            manager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(deleteOnClose: deleteOnClose),
                new DefaultCheckpointNamingScheme(MethodTestDir), false);
            this.deleteOnClose = deleteOnClose;
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            if (!deleteOnClose)
                manager.RemoveAllCommits();
            manager?.Dispose();
            manager = null;
            device?.Dispose();
            device = null;

            DeleteDirectory(MethodTestDir);
        }

        internal class Counter
        {
            internal int count;
            private readonly TsavoriteLog log;

            internal Counter(TsavoriteLog log)
            {
                count = 0;
                this.log = log;
            }

            internal void IncrementAndMaybeTruncateUntil(long nextAddr)
            {
                count++;
                if (count % 100 == 0)
                    log.TruncateUntil(nextAddr);
            }

            public override string ToString() => $"{count}";
        }

        public enum IteratorType
        {
            AsyncByteVector,
            AsyncMemoryOwner,
            Sync,
        }

        internal static bool IsAsync(IteratorType iterType) =>
            iterType is IteratorType.AsyncByteVector or IteratorType.AsyncMemoryOwner;

        protected async ValueTask AssertGetNext(
            IAsyncEnumerator<(byte[] entry, int entryLength, long currentAddress, long nextAddress)>
                asyncByteVectorIter,
            IAsyncEnumerator<(IMemoryOwner<byte> entry, int entryLength, long currentAddress, long nextAddress)>
                asyncMemoryOwnerIter,
            TsavoriteLogScanIterator iter, byte[] expectedData = default, bool verifyAtEnd = false)
        {
            if (asyncByteVectorIter is not null)
            {
                ClassicAssert.IsTrue(await asyncByteVectorIter.MoveNextAsync());
                if (expectedData is not null)
                    ClassicAssert.IsTrue(asyncByteVectorIter.Current.entry.SequenceEqual(expectedData));

                // MoveNextAsync() would hang here waiting for more entries
                if (verifyAtEnd)
                    ClassicAssert.AreEqual(log.TailAddress, asyncByteVectorIter.Current.nextAddress);
                return;
            }

            if (asyncMemoryOwnerIter is not null)
            {
                ClassicAssert.IsTrue(await asyncMemoryOwnerIter.MoveNextAsync());
                if (expectedData is not null)
                    ClassicAssert.IsTrue(asyncMemoryOwnerIter.Current.entry.Memory.Span.ToArray().Take(expectedData.Length)
                        .SequenceEqual(expectedData));
                asyncMemoryOwnerIter.Current.entry.Dispose();

                // MoveNextAsync() would hang here waiting for more entries
                if (verifyAtEnd)
                    ClassicAssert.AreEqual(log.TailAddress, asyncMemoryOwnerIter.Current.nextAddress);
                return;
            }

            ClassicAssert.IsTrue(iter.GetNext(out byte[] result, out _, out _));
            if (expectedData is not null)
                ClassicAssert.IsTrue(result.SequenceEqual(expectedData));
            if (verifyAtEnd)
                ClassicAssert.IsFalse(iter.GetNext(out _, out _, out _));
        }

        protected static async Task LogWriterAsync(TsavoriteLog log, byte[] entry)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            // Enter in some entries then wait on this separate thread
            _ = await log.EnqueueAsync(entry);
            _ = await log.EnqueueAsync(entry);
            var commitTask = await log.CommitAsync(null, null, token);
            _ = await log.EnqueueAsync(entry);
            _ = await log.CommitAsync(commitTask, null, token);
        }
    }

    [TestFixture]
    internal class TsavoriteLogGeneralTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TsavoriteLogTest1([Values] LogChecksumType logChecksum, [Values] IteratorType iteratorType)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);

            // If endAddress > log.TailAddress then GetAsyncEnumerable() will wait until more entries are added.
            var endAddress = IsAsync(iteratorType) ? log.TailAddress : long.MaxValue;
            using var iter = log.Scan(0, endAddress);
            var counter = new Counter(log);
            switch (iteratorType)
            {
                case IteratorType.AsyncByteVector:
                    await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
                    {
                        ClassicAssert.IsTrue(result.SequenceEqual(entry));
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }

                    break;
                case IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, _, _, long nextAddress) in iter
                                       .GetAsyncEnumerable(MemoryPool<byte>.Shared))
                    {
                        ClassicAssert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }

                    break;
                case IteratorType.Sync:
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

            ClassicAssert.AreEqual(numEntries, counter.count);
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TsavoriteLogTest2([Values] LogChecksumType logChecksum)
        {
            var iteratorType = IteratorType.Sync;

            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            log.Initialize(1000000L, 1000000L, 323);

            log.Commit(true);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);

            // If endAddress > log.TailAddress then GetAsyncEnumerable() will wait until more entries are added.
            var endAddress = IsAsync(iteratorType) ? log.TailAddress : long.MaxValue;
            using var iter = log.Scan(0, endAddress);
            var counter = new Counter(log);
            switch (iteratorType)
            {
                case IteratorType.AsyncByteVector:
                    await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
                    {
                        ClassicAssert.IsTrue(result.SequenceEqual(entry));
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }

                    break;
                case IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, _, _, long nextAddress) in iter
                                       .GetAsyncEnumerable(MemoryPool<byte>.Shared))
                    {
                        ClassicAssert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }

                    break;
                case IteratorType.Sync:
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

            ClassicAssert.AreEqual(numEntries, counter.count);
        }

        internal class TestConsumer : ILogEntryConsumer
        {
            private readonly Counter counter;
            private readonly byte[] entry;

            internal TestConsumer(Counter counter, byte[] entry)
            {
                this.counter = counter;
                this.entry = entry;
            }

            public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
            {
                ClassicAssert.IsTrue(new ReadOnlySpan<byte>(payloadPtr, payloadLength).SequenceEqual(entry));
                counter.IncrementAndMaybeTruncateUntil(nextAddress);
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public void TsavoriteLogConsumerTest([Values] LogChecksumType logChecksum)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);

            using var iter = log.Scan(0, long.MaxValue);
            var counter = new Counter(log);
            var consumer = new TestConsumer(counter, entry);

            while (iter.TryConsumeNext(consumer))
            {
            }

            ClassicAssert.AreEqual(numEntries, counter.count);
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TsavoriteLogAsyncConsumerTest([Values] LogChecksumType logChecksum)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = await TsavoriteLog.CreateAsync(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);
            log.CompleteLog(true);

            using var iter = log.Scan(0, long.MaxValue);
            var counter = new Counter(log);
            var consumer = new TestConsumer(counter, entry);
            await iter.ConsumeAllAsync(consumer);
            ClassicAssert.AreEqual(numEntries, counter.count);
        }
    }


    [TestFixture]
    internal class TsavoriteLogEnqueueTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TryEnqueue1([Values] LogChecksumType logChecksum, [Values] IteratorType iteratorType)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            const int dataLength = 1000;
            byte[] data1 = new byte[dataLength];
            for (int i = 0; i < dataLength; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: DiskScanBufferingMode.SinglePageBuffering))
            {
                var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                    ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                    : default;
                var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                    ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                    : default;
                int i = 0;
                while (i++ < 500)
                {
                    var waitingReader = iter.WaitAsync();
                    ClassicAssert.IsTrue(!waitingReader.IsCompleted);

                    while (!log.TryEnqueue(data1, out _)) ;

                    // We might have auto-committed at page boundary
                    // Ensure we don't find new entry in iterator
                    while (waitingReader.IsCompleted)
                    {
                        var _next = iter.GetNext(out _, out _, out _);
                        ClassicAssert.IsFalse(_next);
                        waitingReader = iter.WaitAsync();
                    }

                    ClassicAssert.IsFalse(waitingReader.IsCompleted);

                    await log.CommitAsync(token: token);
                    while (!waitingReader.IsCompleted) ;
                    ClassicAssert.IsTrue(waitingReader.IsCompleted);

                    await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd: true);
                }
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async ValueTask TryEnqueue2([Values] LogChecksumType logChecksum, [Values] IteratorType iteratorType, [Values] TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, "TryEnqueue2" + deviceType.ToString() + ".log");
            device = CreateTestDevice(deviceType, filename);

            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSizeBits = 14,
                LogChecksum = logChecksum,
                LogCommitManager = manager,
                SegmentSizeBits = 22,
                TryRecoverLatest = false
            };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            const int dataLength = 10000;
            byte[] data1 = new byte[dataLength];
            for (int i = 0; i < dataLength; i++) data1[i] = (byte)i;

            using var iter = log.Scan(0, long.MaxValue, scanBufferingMode: DiskScanBufferingMode.SinglePageBuffering);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                : default;

            var appendResult = log.TryEnqueue(data1, out _);
            ClassicAssert.IsTrue(appendResult);
            await log.CommitAsync();
            _ = await iter.WaitAsync();

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1);

            // This no longer fails in latest TryAllocate improvement
            appendResult = log.TryEnqueue(data1, out _);
            ClassicAssert.IsTrue(appendResult);
            await log.CommitAsync();
            _ = await iter.WaitAsync();

            switch (iteratorType)
            {
                case IteratorType.Sync:
                    ClassicAssert.IsTrue(iter.GetNext(out _, out _, out _));
                    break;
                case IteratorType.AsyncByteVector:
                    {
                        // No more hole
                        var moveNextTask = asyncByteVectorIter.MoveNextAsync();

                        // Now the data is available.
                        ClassicAssert.IsTrue(await moveNextTask);
                    }
                    break;
                case IteratorType.AsyncMemoryOwner:
                    {
                        // No more hole
                        var moveNextTask = asyncMemoryOwnerIter.MoveNextAsync();

                        // Now the data is available, and must be disposed.
                        ClassicAssert.IsTrue(await moveNextTask);
                        asyncMemoryOwnerIter.Current.entry.Dispose();
                    }
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }
        }
    }

    [TestFixture]
    internal class TsavoriteLogTruncateTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async ValueTask TruncateUntilBasic([Values] LogChecksumType logChecksum,
            [Values] IteratorType iteratorType, [Values] TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, "TruncateUntilBasic" + deviceType.ToString() + ".log");
            device = CreateTestDevice(deviceType, filename);

            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSizeBits = 14,
                LogChecksum = logChecksum,
                LogCommitManager = manager,
                SegmentSizeBits = 22,
                TryRecoverLatest = false
            };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            byte[] data1 = new byte[100];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
                _ = log.Enqueue(data1);

            ClassicAssert.AreEqual(log.BeginAddress, log.CommittedUntilAddress);
            await log.CommitAsync();

            ClassicAssert.AreEqual(log.TailAddress, log.CommittedUntilAddress);
            ClassicAssert.AreEqual(log.BeginAddress, log.CommittedBeginAddress);

            using var iter = log.Scan(0, long.MaxValue);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                : default;

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1);

            log.TruncateUntil(iter.NextAddress);

            ClassicAssert.AreEqual(log.TailAddress, log.CommittedUntilAddress);
            ClassicAssert.Less(log.CommittedBeginAddress, log.BeginAddress);
            ClassicAssert.AreEqual(log.BeginAddress, iter.NextAddress);

            await log.CommitAsync();

            ClassicAssert.AreEqual(log.TailAddress, log.CommittedUntilAddress);
            ClassicAssert.AreEqual(log.BeginAddress, log.CommittedBeginAddress);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async ValueTask EnqueueAndWaitForCommitAsyncBasicTest([Values] LogChecksumType logChecksum,
            [Values] TestDeviceType deviceType)
        {
            CancellationToken cancellationToken = default;

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numSpanEntries);

            string filename = Path.Join(MethodTestDir, "EnqueueAndWaitForCommitAsyncBasicTest" + deviceType.ToString() + ".log");
            device = CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSizeBits = 16,
                MemorySizeBits = 17,
                LogChecksum = logChecksum,
                LogCommitManager = manager,
                SegmentSizeBits = 22
            });

            int headerSize = logChecksum == LogChecksumType.None ? 4 : 12;
            bool _disposed = false;
            var commit = new Thread(() =>
            {
                while (!_disposed)
                {
                    log.Commit(true);
                    Thread.Sleep(1);
                }
            });

            // create the read only memory byte that will enqueue and commit async
            ReadOnlyMemory<byte> readOnlyMemoryByte = new byte[65536 - headerSize - 64];

            commit.Start();

            // 65536=page size|headerSize|64=log header - add cancellation token on end just so not assuming default on at least one 
            _ = await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize - 64], cancellationToken);

            // 65536=page size|headerSize
            _ = await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize]);

            // 65536=page size|headerSize
            _ = await log.EnqueueAndWaitForCommitAsync(spanBatch);

            // 65536=page size|headerSize
            _ = await log.EnqueueAndWaitForCommitAsync(spanBatch, cancellationToken);

            // 65536=page size|headerSize
            _ = await log.EnqueueAndWaitForCommitAsync(readOnlyMemoryByte);

            // 65536=page size|headerSize
            _ = await log.EnqueueAndWaitForCommitAsync(readOnlyMemoryByte, cancellationToken);

            // TO DO: Probably do more verification - could read it but in reality, if fails it locks up waiting

            _disposed = true;

            commit.Join();
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TruncateUntil2([Values] LogChecksumType logChecksum, [Values] IteratorType iteratorType)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                MemorySizeBits = 20,
                PageSizeBits = 14,
                LogChecksum = logChecksum,
                LogCommitManager = manager,
                TryRecoverLatest = false,
                SafeTailRefreshFrequencyMs = 0
            };
            log = IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

            byte[] data1 = new byte[1000];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
                _ = log.Enqueue(data1);

            // Wait for safe tail to catch up
            while (log.SafeTailAddress < log.TailAddress)
                await Task.Yield();

            ClassicAssert.AreEqual(log.TailAddress, log.SafeTailAddress);

            ClassicAssert.Less(log.CommittedUntilAddress, log.SafeTailAddress);

            using var iter = log.Scan(0, long.MaxValue, scanUncommitted: true);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                : default;

            switch (iteratorType)
            {
                case IteratorType.Sync:
                    while (iter.GetNext(out _, out _, out _))
                        log.TruncateUntil(iter.NextAddress);
                    ClassicAssert.AreEqual(log.SafeTailAddress, iter.NextAddress);
                    break;
                case IteratorType.AsyncByteVector:
                    {
                        while (await asyncByteVectorIter.MoveNextAsync() &&
                               asyncByteVectorIter.Current.nextAddress != log.SafeTailAddress)
                            log.TruncateUntil(asyncByteVectorIter.Current.nextAddress);
                    }
                    break;
                case IteratorType.AsyncMemoryOwner:
                    {
                        while (await asyncMemoryOwnerIter.MoveNextAsync())
                        {
                            log.TruncateUntil(asyncMemoryOwnerIter.Current.nextAddress);
                            asyncMemoryOwnerIter.Current.entry.Dispose();
                            if (asyncMemoryOwnerIter.Current.nextAddress == log.SafeTailAddress)
                                break;
                        }
                    }
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }

            // Enqueue data, becomes auto-visible
            _ = log.Enqueue(data1);

            // Wait for safe tail to catch up
            while (log.SafeTailAddress < log.TailAddress)
                await Task.Yield();

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd: true);

            log.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TruncateUntilPageStart([Values] LogChecksumType logChecksum,
            [Values] IteratorType iteratorType)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "tsavoritelog.log"), deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                MemorySizeBits = 20,
                PageSizeBits = 14,
                LogChecksum = logChecksum,
                LogCommitManager = manager,
                SafeTailRefreshFrequencyMs = 0
            });
            byte[] data1 = new byte[1000];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
                _ = log.Enqueue(data1);

            // Wait for safe tail to catch up
            while (log.SafeTailAddress < log.TailAddress)
                await Task.Yield();

            ClassicAssert.AreEqual(log.TailAddress, log.SafeTailAddress);
            ClassicAssert.Less(log.CommittedUntilAddress, log.SafeTailAddress);

            using (var iter = log.Scan(0, long.MaxValue, scanUncommitted: true))
            {
                var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                    ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                    : default;
                var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                    ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                    : default;

                switch (iteratorType)
                {
                    case IteratorType.Sync:
                        while (iter.GetNext(out _, out _, out _))
                            log.TruncateUntilPageStart(iter.NextAddress);
                        ClassicAssert.AreEqual(log.SafeTailAddress, iter.NextAddress);
                        break;
                    case IteratorType.AsyncByteVector:
                        {
                            while (await asyncByteVectorIter.MoveNextAsync() &&
                                   asyncByteVectorIter.Current.nextAddress != log.SafeTailAddress)
                                log.TruncateUntilPageStart(asyncByteVectorIter.Current.nextAddress);
                        }
                        break;
                    case IteratorType.AsyncMemoryOwner:
                        {
                            while (await asyncMemoryOwnerIter.MoveNextAsync())
                            {
                                log.TruncateUntilPageStart(asyncMemoryOwnerIter.Current.nextAddress);
                                asyncMemoryOwnerIter.Current.entry.Dispose();
                                if (asyncMemoryOwnerIter.Current.nextAddress == log.SafeTailAddress)
                                    break;
                            }
                        }
                        break;
                    default:
                        Assert.Fail("Unknown IteratorType");
                        break;
                }

                // Enqueue data, becomes auto-visible
                _ = log.Enqueue(data1);

                // Wait for safe tail to catch up
                while (log.SafeTailAddress < log.TailAddress)
                    await Task.Yield();

                await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd: true);
            }

            log.Dispose();
        }


        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void CommitNoSpinWait([Values] TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, "CommitNoSpinWait" + deviceType.ToString() + ".log");
            device = CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings
            { LogDevice = device, LogCommitManager = manager, SegmentSizeBits = 22 });

            int commitFalseEntries = 100;

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < commitFalseEntries; i++)
                _ = log.Enqueue(entry);

            //*******
            // Main point of the test ... If commit(true) (like other tests do) it waits until commit completes before moving on.
            // If set to false, it will fire and forget the commit and return immediately (which is the way this test is set up).
            // There won't be that much difference from True to False here as the True case is so quick but there can be issues
            // if start checking right after commit without giving time to commit.
            // Also, it is a good basic check to make sure it isn't crashing and that it does actually commit it
            //*******

            log.Commit(false);
            while (log.CommittedUntilAddress < log.TailAddress)
                _ = Thread.Yield();

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        ClassicAssert.AreEqual((byte)currentEntry, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            log.Dispose();

            // Make sure expected length is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(entryLength, currentEntry);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async ValueTask CommitAsyncPrevTask([Values] TestDeviceType deviceType)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            string filename = Path.Join(MethodTestDir, $"CommitAsyncPrevTask_{deviceType}.log");
            device = CreateTestDevice(deviceType, filename);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogCommitManager = manager, SegmentSizeBits = 22, TryRecoverLatest = false };
            log = await TsavoriteLog.CreateAsync(logSettings);

            // make it small since launching each on separate threads 
            const int entryLength = 10;
            int expectedEntries = 3; // Not entry length because this is number of enqueues called

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Enqueue and AsyncCommit in a separate thread (wait there until commit is done though).
            Task currentTask = Task.Run(() => LogWriterAsync(log, entry), token);

            // Commit to the log
            _ = currentTask.Wait(4000, token);

            // double check to make sure finished - seen cases where timing kept running even after commit done
            bool wasCanceled = false;
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                wasCanceled = true;
                cts.Cancel();
            }

            // Read the log to make sure all entries are put in
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        ClassicAssert.AreEqual((byte)currentEntry, result[currentEntry]);
                        currentEntry++;
                    }
                }
            }

            // Make sure expected entries is same as current - also makes sure that data verification was not skipped
            ClassicAssert.AreEqual(expectedEntries, currentEntry);

            // NOTE: seeing issues where task is not running to completion on Release builds
            // This is a final check to make sure task finished. If didn't then assert
            // One note - if made it this far, know that data was Enqueue and read properly, so just
            // case of task not stopping
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                Assert.Fail(
                    $"Final Status check Failure -- Task should be 'RanToCompletion' but current Status is: {currentTask.Status}; wasCanceled = {wasCanceled}");
            }
        }


        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        [Explicit("Dispose() sequencing issues result free pagePointers while operations are ongoing")]
        public async ValueTask RefreshUncommittedAsyncTest([Values] IteratorType iteratorType, [Values] TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, "RefreshUncommittedAsyncTest" + deviceType.ToString() + ".log");
            device = CreateTestDevice(deviceType, filename);

            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                MemorySizeBits = 20,
                PageSizeBits = 14,
                LogCommitManager = manager,
                SegmentSizeBits = 22,
                SafeTailRefreshFrequencyMs = 0
            });
            byte[] data1 = new byte[1000];
            for (int i = 0; i < 100; i++)
                data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
                _ = log.Enqueue(data1);

            // Wait for safe tail to catch up
            while (log.SafeTailAddress < log.TailAddress)
                await Task.Yield();

            ClassicAssert.AreEqual(log.TailAddress, log.SafeTailAddress);
            ClassicAssert.Less(log.CommittedUntilAddress, log.SafeTailAddress);

            using (var iter = log.Scan(0, long.MaxValue, scanUncommitted: true))
            {
                var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector
                    ? iter.GetAsyncEnumerable().GetAsyncEnumerator()
                    : default;
                var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner
                    ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator()
                    : default;

                switch (iteratorType)
                {
                    case IteratorType.Sync:
                        while (iter.GetNext(out _, out _, out _))
                            log.TruncateUntilPageStart(iter.NextAddress);
                        ClassicAssert.AreEqual(log.SafeTailAddress, iter.NextAddress);
                        break;
                    case IteratorType.AsyncByteVector:
                        {
                            while (await asyncByteVectorIter.MoveNextAsync() &&
                                   asyncByteVectorIter.Current.nextAddress != log.SafeTailAddress)
                                log.TruncateUntilPageStart(asyncByteVectorIter.Current.nextAddress);
                        }
                        break;
                    case IteratorType.AsyncMemoryOwner:
                        {
                            while (await asyncMemoryOwnerIter.MoveNextAsync())
                            {
                                log.TruncateUntilPageStart(asyncMemoryOwnerIter.Current.nextAddress);
                                asyncMemoryOwnerIter.Current.entry.Dispose();
                                if (asyncMemoryOwnerIter.Current.nextAddress == log.SafeTailAddress)
                                    break;
                            }
                        }
                        break;
                    default:
                        Assert.Fail("Unknown IteratorType");
                        break;
                }

                // Enqueue additional data item, becomes auto-visible
                _ = log.Enqueue(data1);

                // Wait for safe tail to catch up
                while (log.SafeTailAddress < log.TailAddress)
                    await Task.Yield();

                await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd: true);
            }

            log.Dispose();
        }
    }

    [TestFixture]
    internal class TsavoriteLogCustomCommitTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup(false);

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void TsavoriteLogSimpleCommitCookieTest([Values] bool fastCommit)
        {
            var cookie = new byte[100];
            new Random().NextBytes(cookie);

            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleCommitCookie" + fastCommit + ".log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                LogChecksum = LogChecksumType.PerEntry,
                LogCommitManager = manager,
                FastCommitMode = fastCommit
            };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            _ = log.CommitStrongly(out _, out _, true, cookie);

            var recoveredLog = new TsavoriteLog(logSettings);
            ClassicAssert.AreEqual(cookie, recoveredLog.RecoveredCookie);
            recoveredLog.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void TsavoriteLogManualCommitTest()
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "logManualCommitTest.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                LogChecksum = LogChecksumType.None,
                LogCommitManager = manager,
                TryRecoverLatest = false
            };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            var cookie1 = new byte[100];
            new Random().NextBytes(cookie1);
            var commitSuccessful = log.CommitStrongly(out var commit1Addr, out _, true, cookie1, 1);
            ClassicAssert.IsTrue(commitSuccessful);

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            var cookie2 = new byte[100];
            new Random().NextBytes(cookie2);
            commitSuccessful = log.CommitStrongly(out var commit2Addr, out _, true, cookie2, 2);
            ClassicAssert.IsTrue(commitSuccessful);

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            var cookie6 = new byte[100];
            new Random().NextBytes(cookie6);
            commitSuccessful = log.CommitStrongly(out var commit6Addr, out _, true, cookie6, 6);
            ClassicAssert.IsTrue(commitSuccessful);

            var recoveredLog = new TsavoriteLog(logSettings);
            recoveredLog.Recover(1);
            ClassicAssert.AreEqual(cookie1, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit1Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();

            recoveredLog = new TsavoriteLog(logSettings);
            recoveredLog.Recover(2);
            ClassicAssert.AreEqual(cookie2, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit2Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();

            // recovering to a non-existent commit should throw TsavoriteException
            try
            {
                recoveredLog = new TsavoriteLog(logSettings);
                recoveredLog.Recover(4);
                Assert.Fail();
            }
            catch (TsavoriteException)
            {
            }

            // Default argument should recover to most recent, with TryRecoverLatest set to true
            logSettings.TryRecoverLatest = true;
            recoveredLog = new TsavoriteLog(logSettings);
            ClassicAssert.AreEqual(cookie6, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit6Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        public async ValueTask TsavoriteLogAsyncConsumerTestAfterDisposeIterator([Values] LogChecksumType logChecksum)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "tsavoritelog.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager, TryRecoverLatest = false };
            log = await TsavoriteLog.CreateAsync(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);

            var nextAddress = 0L;
            using (var iter = log.Scan(0, long.MaxValue))
            {
                var count = 0;
                while (iter.GetNext(out _, out _, out _, out nextAddress)) count++;
                log.Commit(true);
                ClassicAssert.AreEqual(numEntries, count);
            }

            entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);

            log.Commit(true);
            log.CompleteLog(true);

            using (var iter = log.Scan(nextAddress, long.MaxValue))
            {
                var counter = new Counter(log);
                var consumer = new TsavoriteLogGeneralTests.TestConsumer(counter, entry);
                await iter.ConsumeAllAsync(consumer);
                ClassicAssert.AreEqual(numEntries, counter.count);
            }
        }
    }
}