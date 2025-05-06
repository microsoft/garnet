// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    internal class LogResumeTests
    {
        private IDevice device;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "TsavoriteLog.log"), deleteOnClose: true);
        }

        [TearDown]
        public void TearDown()
        {
            device?.Dispose();
            device = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteLog")]
        public async Task TsavoriteLogResumePersistedReaderViaCookie([Values] LogChecksumType logChecksum)
        {
            CancellationToken cancellationToken = default;

            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };

            using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum }))
            {
                await l.EnqueueAsync(input1, cancellationToken);
                await l.EnqueueAsync(input2);
                await l.EnqueueAsync(input3);
                await l.CommitAsync();

                using var originalIterator = l.Scan(0, long.MaxValue);
                ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out _, out long recoveryAddress));
                ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out _, out _));  // move the reader ahead
                // convert recoveryAddress to byte[] for cookie
                await l.CommitAsync(cookie: BitConverter.GetBytes(recoveryAddress));
            }

            using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum }))
            {
                var recoveredAddress = BitConverter.ToInt64(l.RecoveredCookie);
                using var recoveredIterator = l.Scan(recoveredAddress, long.MaxValue);
                ClassicAssert.IsTrue(recoveredIterator.GetNext(out byte[] outBuf, out _, out _, out _));
                ClassicAssert.True(input2.SequenceEqual(outBuf));  // we should have read in input2, not input1 or input3
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public async Task TsavoriteLogResumeViaCompleteUntilRecordAtSpec([Values] LogChecksumType logChecksum)
        {
            CancellationToken cancellationToken = default;

            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };

            using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum }))
            {
                await l.EnqueueAsync(input1, cancellationToken);
                await l.EnqueueAsync(input2);
                await l.EnqueueAsync(input3);
                await l.CommitAsync();

                using var originalIterator = l.Scan(0, long.MaxValue);
                ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out long recordAddress, out long nextAddress));
                ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out _, out _));  // move the reader ahead
                await l.CommitAsync(cookie: BitConverter.GetBytes(nextAddress));
            }

            using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum }))
            {
                var recoveredAddress = BitConverter.ToInt64(l.RecoveredCookie);
                using var recoveredIterator = l.Scan(recoveredAddress, long.MaxValue);
                ClassicAssert.IsTrue(recoveredIterator.GetNext(out byte[] outBuf, out _, out _, out _));
                ClassicAssert.True(input2.SequenceEqual(outBuf));  // we should have read in input2, not input1 or input3
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public async Task TsavoriteLogResumePersistedReader2([Values] LogChecksumType logChecksum, [Values] bool removeOutdated)
        {
            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };

            using (var logCommitManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(), new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir), removeOutdated))
            {
                using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    await l.EnqueueAsync(input1);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input2);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input3);
                    await l.CommitAsync();

                    using var originalIterator = l.Scan(0, long.MaxValue);
                    ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out _, out long recoveryAddress));
                    ClassicAssert.IsTrue(originalIterator.GetNext(out _, out _, out _, out _));  // move the reader ahead
                    await l.CommitAsync(cookie: BitConverter.GetBytes(recoveryAddress));
                }

                using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    var recoveredAddress = BitConverter.ToInt64(l.RecoveredCookie);
                    using var recoveredIterator = l.Scan(recoveredAddress, long.MaxValue);
                    ClassicAssert.IsTrue(recoveredIterator.GetNext(out byte[] outBuf, out _, out _, out _));

                    // we should have read in input2, not input1 or input3
                    ClassicAssert.True(input2.SequenceEqual(outBuf), $"Original: {input2[0]}, Recovered: {outBuf[0]}");

                    // TestContext.Progress.WriteLine($"Original: {originalCompleted}, Recovered: {recoveredIterator.CompletedUntilAddress}"); 
                }
            }
        }

        [Test]
        [Category("TsavoriteLog")]
        public async Task TsavoriteLogResumePersistedReader3([Values] LogChecksumType logChecksum, [Values] bool removeOutdated)
        {
            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };

            using (var logCommitManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(), new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir), removeOutdated))
            {
                using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    await l.EnqueueAsync(input1);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input2);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input3);
                    await l.CommitAsync();

                    using var originalIterator = l.Scan(0, l.TailAddress);

                    int count = 0;
                    await foreach (var item in originalIterator.GetAsyncEnumerable())
                    {
                        if (count < 1) // we commit only 1st item read
                            await l.CommitAsync(cookie: BitConverter.GetBytes(item.nextAddress));

                        count++;
                    }
                }

                using (var l = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 17, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    var recoveredAddress = BitConverter.ToInt64(l.RecoveredCookie);
                    using var recoveredIterator = l.Scan(recoveredAddress, l.TailAddress);

                    int count = 0;
                    await foreach (var item in recoveredIterator.GetAsyncEnumerable())
                    {
                        if (count == 0) // resumed iterator will start at item2
                            ClassicAssert.True(input2.SequenceEqual(item.entry), $"Original: {input2[0]}, Recovered: {item.entry[0]}");
                        count++;
                    }
                    ClassicAssert.IsTrue(count == 2);
                }
            }
        }
    }
}