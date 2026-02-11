// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.test
{
    [AllureNUnit]
    [TestFixture]
    internal class DeviceLogTests : AllureTestBase
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
            await TsavoriteLogTest1(logChecksum, device, checkpointManager, iteratorType);
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
            await TsavoriteLogTest1(logChecksum, device, checkpointManager, iteratorType);
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
            using var device = new LocalMemoryDevice(1L << 28, 1L << 25, 2, latencyMs: 20, fileName: Path.Join(TestUtils.MethodTestDir, "test.log"));
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
            log = TsavoriteLogTestBase.IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

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
                        await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
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
    }
}

#endif // LOGRECORD_TODO
