// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogFastCommitTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup(false);

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public async Task FastCommitRecoverToMissingCommitNumThrows()
        {
            // On the fast-commit path the requested commit can only be produced by a forward scan. A scan that fails
            // to find it must not fall through, because info still describes the closest earlier commit and recovery
            // would restore that commit's data instead.
            var filename = Path.Join(TestUtils.MethodTestDir, "fastCommitMissing.log");
            device = Devices.CreateLogDevice(filename, deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = device,
                LogChecksum = LogChecksumType.PerEntry,
                LogCommitManager = manager,
                FastCommitMode = true,
                TryRecoverLatest = false,
                SegmentSizeBits = 26
            };
            log = new TsavoriteLog(logSettings);

            var entry = new byte[entryLength];
            for (var i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (var i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);
            var cookie1 = new byte[100];
            new Random().NextBytes(cookie1);
            ClassicAssert.IsTrue(log.CommitStrongly(out var commit1Addr, out _, true, cookie1, 1));

            for (var i = 0; i < numEntries; i++)
                _ = log.Enqueue(entry);
            var cookie6 = new byte[100];
            new Random().NextBytes(cookie6);
            ClassicAssert.IsTrue(log.CommitStrongly(out _, out _, true, cookie6, 6));

            log.Dispose();
            log = null;
            manager.RemoveAllCommits();

            // Commit 4 never existed; the closest earlier commit is 1.
            var recoveredLog = new TsavoriteLog(logSettings);
            try
            {
                // Which exception reports the missing commit is platform dependent. Where the forward scan runs off
                // the end of the written region the read fails and WaitForFrameLoad rethrows the resulting
                // OperationCanceledException (Windows reports the over-read as an error, Linux returns a short read);
                // otherwise the scan completes without finding commit 4 and the commit-number check rejects it.
                var ex = Assert.CatchAsync(async () => await recoveredLog.RecoverAsync(4).ConfigureAwait(false));
                Assert.That(ex, Is.InstanceOf<TsavoriteException>().Or.InstanceOf<OperationCanceledException>());
                ClassicAssert.AreNotEqual(commit1Addr, recoveredLog.TailAddress,
                    "Recovery silently fell back to an earlier commit instead of failing the request");
            }
            finally
            {
                recoveredLog.Dispose();
            }

            // A commit that does exist must still recover normally.
            recoveredLog = new TsavoriteLog(logSettings);
            await recoveredLog.RecoverAsync(1).ConfigureAwait(false);
            ClassicAssert.AreEqual(cookie1, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit1Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public async Task TsavoriteLogSimpleFastCommitTest([Values] TestUtils.TestDeviceType deviceType)
        {
            var cookie = new byte[100];
            new Random().NextBytes(cookie);

            var filename = Path.Join(TestUtils.MethodTestDir, $"fastCommit{deviceType}.log");
            device = TestUtils.CreateTestDevice(deviceType, filename, deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true, TryRecoverLatest = false, SegmentSizeBits = 26 };
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

            // Wait for all metadata writes to be complete to avoid a concurrent access exception
            log.Dispose();
            log = null;

            // be a deviant and remove commit metadata files
            manager.RemoveAllCommits();

            // Recovery should still work
            var recoveredLog = new TsavoriteLog(logSettings);
            await recoveredLog.RecoverAsync(1).ConfigureAwait(false);
            ClassicAssert.AreEqual(cookie1, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit1Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();

            recoveredLog = new TsavoriteLog(logSettings);
            await recoveredLog.RecoverAsync(2).ConfigureAwait(false);
            ClassicAssert.AreEqual(cookie2, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit2Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();

            // Default argument should recover to most recent, if TryRecoverLatest is set
            logSettings.TryRecoverLatest = true;
            recoveredLog = new TsavoriteLog(logSettings);
            ClassicAssert.AreEqual(cookie6, recoveredLog.RecoveredCookie);
            ClassicAssert.AreEqual(commit6Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void CommitRecordBoundedGrowthTest([Values] TestUtils.TestDeviceType deviceType, [Values(1, -1)] int numThreads)
        {
            var cookie = new byte[100];
            new Random().NextBytes(cookie);

            var filename = Path.Join(TestUtils.MethodTestDir, $"boundedGrowth{deviceType}.log");
            device = TestUtils.CreateTestDevice(deviceType, filename, deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true, SegmentSizeBits = 26 };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < 5 * numEntries; i++)
                _ = log.Enqueue(entry);

            // for comparison, insert some entries without any commit records
            var referenceTailLength = log.TailAddress;

            var enqueueDone = new ManualResetEventSlim();
            var commitThreads = new List<Thread>();
            // Capture any exception thrown by a background commit thread. An unhandled throw on a raw Thread
            // (e.g. a device write error surfaced as CommitFailureException) would otherwise terminate the whole
            // test host process; instead we record it and fail this test assertively below.
            Exception commitException = null;
            // Make sure to not spin up too many commit threads, otherwise we might clog epochs and halt progress
            var commitThreadCount = numThreads == -1 ? Math.Max(1, Environment.ProcessorCount / 2) : numThreads;
            for (var i = 0; i < commitThreadCount; i++)
            {
                commitThreads.Add(new Thread(() =>
                {
                    try
                    {
                        // Otherwise, absolutely clog the commit pipeline
                        while (!enqueueDone.IsSet)
                            log.Commit();
                    }
                    catch (Exception ex)
                    {
                        // Keep the first failure and stop the remaining threads so we fail fast with the root cause.
                        _ = Interlocked.CompareExchange(ref commitException, ex, null);
                        enqueueDone.Set();
                    }
                }));
            }

            foreach (var t in commitThreads)
                t.Start();
            for (int i = 0; i < 5 * numEntries; i++)
                _ = log.Enqueue(entry);
            enqueueDone.Set();

            foreach (var t in commitThreads)
                t.Join();

            // A background commit failure must surface as a test failure (with the underlying device exception as
            // InnerException) rather than as a process-killing unhandled exception.
            ClassicAssert.IsNull(commitException, $"Commit thread failed: {commitException}");

            // TODO: Hardcoded constant --- if this number changes in TsavoriteLogRecoveryInfo, it needs to be updated here too
            var commitRecordSize = 44;
            var logTailGrowth = log.TailAddress - referenceTailLength;
            // Check that we are not growing the log more than one commit record per user entry
            ClassicAssert.IsTrue(logTailGrowth - referenceTailLength <= commitRecordSize * 5 * numEntries);

            // Ensure clean shutdown
            log.Commit(true);
        }
    }
}