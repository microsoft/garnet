// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
                // The forward scan reaches the end of the written log without finding commit 4
                _ = Assert.CatchAsync<TsavoriteException>(async () => await recoveredLog.RecoverAsync(4).ConfigureAwait(false));
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

        /// <summary>Where the fast-commit recovery scan for commit records starts</summary>
        public enum RecoveryScanStart
        {
            /// <summary>Nothing was written, so the scan starts at the beginning of an empty log</summary>
            EmptyLog,

            /// <summary>The latest commit has metadata, so the scan starts at the end of the log</summary>
            LatestCommitMetadata,

            /// <summary>No commit has metadata, so the scan starts at the beginning and finds every commit</summary>
            BeginningOfLog,
        }

        private const int SmallPageSizeBits = 12;

        private TsavoriteLogSettings SmallPageFastCommitSettings(IDevice logDevice) => new()
        {
            LogDevice = logDevice,
            LogChecksum = LogChecksumType.PerEntry,
            LogCommitManager = manager,
            FastCommitMode = true,
            TryRecoverLatest = false,
            PageSizeBits = SmallPageSizeBits,
            MemorySizeBits = 20,
            SegmentSizeBits = 22
        };

        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryEndsQuietlyAtTheEndOfTheLog(
            [Values(DeviceType.Native, DeviceType.RandomAccess, DeviceType.FileStream)] DeviceType deviceType,
            [Values] RecoveryScanStart scanStart)
            => RecoverToTheEndOfTheLog(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, $"endOfLog{deviceType}.log"), deviceType: deviceType, deleteOnClose: true), scanStart);

        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryEndsQuietlyAtTheEndOfANativeStorageDeviceLog([Values] RecoveryScanStart scanStart)
            // DeviceType.Native is LocalStorageDevice on Windows, so this covers NativeStorageDevice there
            => RecoverToTheEndOfTheLog(new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "endOfLog.log"), deleteOnClose: true), scanStart);

        async Task RecoverToTheEndOfTheLog(IDevice logDevice, RecoveryScanStart scanStart)
        {
            // The recovery scan for commit records has no end bound, so it always reads past the end of the written log.
            // LocalStorageDevice reports that read as Windows' end-of-file error where the other devices return no bytes;
            // neither is a failure.
            device = logDevice;
            var logSettings = SmallPageFastCommitSettings(device);

            byte[] lastCookie = null;
            var lastCommitAddress = 0L;
            if (scanStart != RecoveryScanStart.EmptyLog)
            {
                log = new TsavoriteLog(logSettings);

                // Span several pages, so the scan crosses page boundaries before it reaches the end of the log
                for (var commitNum = 1; commitNum <= 3; commitNum++)
                {
                    for (var i = 0; i < 100; i++)
                        _ = log.Enqueue(entry);
                    lastCookie = new byte[100];
                    new Random().NextBytes(lastCookie);
                    ClassicAssert.IsTrue(log.CommitStrongly(out lastCommitAddress, out _, true, lastCookie, commitNum));
                }
                ClassicAssert.Greater(lastCommitAddress, 3L << SmallPageSizeBits, "the log should span several pages");

                log.Dispose();
                log = null;
            }

            if (scanStart == RecoveryScanStart.BeginningOfLog)
                manager.RemoveAllCommits();

            var logger = new RecordingLogger();
            var recoveredLog = new TsavoriteLog(logSettings, logger);
            try
            {
                await recoveredLog.RecoverAsync().ConfigureAwait(false);
                ClassicAssert.AreEqual(lastCookie, recoveredLog.RecoveredCookie);
                if (scanStart != RecoveryScanStart.EmptyLog)
                    ClassicAssert.AreEqual(lastCommitAddress, recoveredLog.TailAddress);
            }
            finally
            {
                recoveredLog.Dispose();
            }

            Assert.That(logger.AtOrAbove(LogLevel.Warning), Is.Empty, "reaching the end of the log is how the recovery scan ends, not a failure");
        }

        [Test]
        [Category("TsavoriteLog")]
        public async Task FastCommitRecoveryFailsOnAReadErrorPastTheLastCommit([Values] bool tolerateDeviceFailure)
        {
            // Only the end of the written log ends the recovery scan quietly. A device error leaves the end of the log unknown,
            // so recovery fails rather than drop the commits the error may hide, unless device failures are tolerated.
            var failing = new ErrorCodeOnReadDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "readError.log"), deleteOnClose: true));
            device = failing;
            var logSettings = SmallPageFastCommitSettings(failing);
            log = new TsavoriteLog(logSettings);

            for (var i = 0; i < 100; i++)
                _ = log.Enqueue(entry);
            var cookie = new byte[100];
            new Random().NextBytes(cookie);
            ClassicAssert.IsTrue(log.CommitStrongly(out var commitAddress, out _, true, cookie, 1));

            var pageSize = 1L << SmallPageSizeBits;
            ClassicAssert.AreNotEqual(0L, commitAddress & (pageSize - 1), "the scan should start inside the last written page");
            var firstUnwrittenPage = (commitAddress | (pageSize - 1)) + 1;
            log.Dispose();
            log = null;

            // ERROR_IO_DEVICE: a genuine failure, unlike the end-of-file error Windows reports past the end of the log
            failing.ReadErrorFromAddress = firstUnwrittenPage;
            failing.ReadErrorCode = 1117;

            logSettings.TolerateDeviceFailure = tolerateDeviceFailure;
            var logger = new RecordingLogger();
            var recoveredLog = new TsavoriteLog(logSettings, logger);
            try
            {
                if (tolerateDeviceFailure)
                {
                    // Recovery keeps the last commit the scan found
                    await recoveredLog.RecoverAsync().ConfigureAwait(false);
                    ClassicAssert.AreEqual(cookie, recoveredLog.RecoveredCookie);
                    ClassicAssert.AreEqual(commitAddress, recoveredLog.TailAddress);
                }
                else
                {
                    _ = Assert.ThrowsAsync<TsavoriteIOException>(async () => await recoveredLog.RecoverAsync().ConfigureAwait(false));
                    ClassicAssert.AreEqual(recoveredLog.BeginAddress, recoveredLog.TailAddress, "a failed recovery leaves the log empty");
                }

                // Either way the log may extend past the tail, so clearing above the tail must not discard it
                _ = Assert.Throws<TsavoriteException>(recoveredLog.PrepareToAppend);
            }
            finally
            {
                recoveredLog.Dispose();
            }

            Assert.That(logger.AtOrAbove(LogLevel.Warning), Is.Not.Empty, "a device error during the recovery scan must be reported");
        }

        /// <summary>Records every log entry so a test can assert on what was reported</summary>
        private sealed class RecordingLogger : ILogger
        {
            private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

            public IDisposable BeginScope<TState>(TState state) => default!;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
                => entries.Enqueue((logLevel, $"{formatter(state, exception)} {exception?.GetType().Name}"));

            public string[] AtOrAbove(LogLevel level) => [.. entries.Where(e => e.Level >= level).Select(e => $"{e.Level}: {e.Message}")];
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
            // This test always starts from an empty log. TryRecoverLatest defaults to true, which would recover from
            // any commit present in MethodTestDir; that directory is keyed only on the method name, so it is shared by
            // every parameterization of this test and can still receive a commit from the previous one after setup has
            // cleaned it. Recovering that commit onto this run's empty device fails to read page 0.
            var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true, TryRecoverLatest = false, SegmentSizeBits = 26 };
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

        /// <summary>How a crash-image scenario prepares the recovered log for appending</summary>
        public enum ClearMode
        {
            /// <summary>Append without clearing above the tail, relying on the recovery scan's padding check</summary>
            None,

            /// <summary>Clear above the tail as part of recovery</summary>
            Recover,

            /// <summary>Recover, then clear above the tail with PrepareToAppend</summary>
            PrepareToAppend,
        }

        static readonly byte[] ReplacementCookie = [2, 4, 6, 8];

        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryRetainsNewTailWithEqualStaleCommit([Values] ClearMode clearMode)
            => RunStaleSuffixScenario(clearMode, intermediateCommit: false, replacementLength: 4, replacementCommitNum: 4, ReplacementCookie);

        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryRetainsNewTailWithDecreasingStaleCommit([Values] ClearMode clearMode)
            => RunStaleSuffixScenario(clearMode, intermediateCommit: false, replacementLength: 4, replacementCommitNum: 6, ReplacementCookie);

        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryRetainsNewTailWithIncreasingStaleCommit([Values] ClearMode clearMode)
            => RunStaleSuffixScenario(clearMode, intermediateCommit: true, replacementLength: 4, replacementCommitNum: 4, ReplacementCookie);

        // The replacement's flush zeroes the rest of the first page, so the padding check passes and only clearing stops the scan
        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryRetainsNewTailAfterFullTailPageRewrite([Values(ClearMode.Recover, ClearMode.PrepareToAppend)] ClearMode clearMode)
            => RunStaleSuffixScenario(clearMode, intermediateCommit: true, replacementLength: 3200, replacementCommitNum: 4, ReplacementCookie,
                replacementFillsTailPage: true);

        // The replacement ends where commit 4 ended, before padding, so the padding check passes and only clearing stops the scan
        [Test]
        [Category("TsavoriteLog")]
        public Task FastCommitRecoveryRetainsNewTailWhenReplacementMatchesStaleAddressAndNumber([Values(ClearMode.Recover, ClearMode.PrepareToAppend)] ClearMode clearMode)
            => RunStaleSuffixScenario(clearMode, intermediateCommit: true, replacementLength: 2048, replacementCommitNum: 4, replacementCookie: null,
                replacementMatchesIntermediateCommit: true);

        /// <summary>
        /// Builds a crash image in which recovery selects an acknowledged prefix while intact commit records remain on the
        /// device past it, appends and commits a replacement, then verifies that a second recovery ends at the replacement.
        /// </summary>
        /// <remarks>
        /// With 4 KB pages, the unacknowledged suffix is a 2048-byte entry, two 3500-byte entries that each need a page of their
        /// own (starting at 4096 and 8192), and a commit record on the third page. Before the crash every byte of it is
        /// written; the crash image restores one sector of the 2048-byte entry to its unwritten contents.
        /// </remarks>
        /// <param name="clearMode">How the first recovery prepares the log for the replacement</param>
        /// <param name="intermediateCommit">Whether commit 4 follows the 2048-byte entry, making the commit on the third page commit 5</param>
        /// <param name="replacementLength">Length of the entry appended after the first recovery</param>
        /// <param name="replacementCommitNum">Commit number of the replacement's commit</param>
        /// <param name="replacementCookie">Cookie of the replacement's commit</param>
        /// <param name="replacementFillsTailPage">Whether the replacement's flush is expected to zero the rest of the first page</param>
        /// <param name="replacementMatchesIntermediateCommit">Whether the replacement's commit is expected to end where commit 4 ended</param>
        private static async Task RunStaleSuffixScenario(ClearMode clearMode, bool intermediateCommit, int replacementLength, long replacementCommitNum,
            byte[] replacementCookie, bool replacementFillsTailPage = false, bool replacementMatchesIntermediateCommit = false)
        {
            const int PageSize = 1 << 12;
            const int HeaderSize = 12;
            const int SectorSize = 512;
            const int TornSectorStart = 1024;

            var logPath = Path.Join(TestUtils.MethodTestDir, "s.log");
            var segmentPath = logPath + ".0";

            var prefixEntry = new byte[100];
            new Random(42).NextBytes(prefixEntry);
            var payload = new byte[2048];
            new Random(123).NextBytes(payload);
            var suffixEntry = new byte[3500];
            new Random(456).NextBytes(suffixEntry);
            var replacement = new byte[replacementLength];
            new Random(789).NextBytes(replacement);

            long acknowledgedTail = 0, intermediateTail = 0;
            byte[] oldImage, commitMetadata;
            using (var original = new CrashImageLog(logPath, TestUtils.MethodTestDir))
            {
                // Acknowledged prefix. Commit-metadata files are throttled, so only commit 1 writes one.
                for (var i = 0; i < 3; i++)
                {
                    _ = original.Log.Enqueue(prefixEntry);
                    ClassicAssert.IsTrue(original.Log.CommitStrongly(out acknowledgedTail, out _, spinWait: true));
                }
                original.Commits.WaitForMetadata(3, acknowledgedTail);
                oldImage = ReadDeviceFile(segmentPath);
                Assert.That(original.Manager.ListCommits(), Is.EqualTo(new long[] { 1 }));
                commitMetadata = original.Manager.GetCommitMetadata(1);

                // Unacknowledged suffix
                ClassicAssert.AreEqual(acknowledgedTail, original.Log.Enqueue(payload));
                if (intermediateCommit)
                    ClassicAssert.IsTrue(original.Log.CommitStrongly(out intermediateTail, out _, spinWait: false, proposedCommitNum: 4));
                ClassicAssert.AreEqual(PageSize, original.Log.Enqueue(suffixEntry));
                ClassicAssert.AreEqual(2 * PageSize, original.Log.Enqueue(suffixEntry));
                var staleCommitNum = intermediateCommit ? 5 : 4;
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out var staleTail, out _, spinWait: true, proposedCommitNum: staleCommitNum));
                original.Commits.WaitForMetadata(staleCommitNum, staleTail);
                ClassicAssert.Greater(staleTail, 2 * PageSize + HeaderSize + suffixEntry.Length);
                ClassicAssert.LessOrEqual(staleTail, 3 * PageSize);
            }
            var newImage = ReadDeviceFile(segmentPath);

            // Crash image: one aligned sector inside the unacknowledged 2048-byte entry keeps its unwritten contents
            var payloadStart = acknowledgedTail + HeaderSize;
            Assert.That(TornSectorStart % SectorSize, Is.Zero);
            Assert.That(TornSectorStart, Is.GreaterThanOrEqualTo(payloadStart));
            Assert.That(TornSectorStart + SectorSize, Is.LessThanOrEqualTo(payloadStart + payload.Length));
            var crashImage = (byte[])newImage.Clone();
            for (var i = TornSectorStart; i < TornSectorStart + SectorSize; i++)
                crashImage[i] = i < oldImage.Length ? oldImage[i] : (byte)0;
            Assert.That(crashImage.AsSpan(TornSectorStart, SectorSize).SequenceEqual(newImage.AsSpan(TornSectorStart, SectorSize)), Is.False,
                "The torn sector must differ from what was written");
            Assert.That(crashImage.AsSpan(0, (int)acknowledgedTail).SequenceEqual(oldImage.AsSpan(0, (int)acknowledgedTail)), Is.True,
                "Acknowledged bytes must be unchanged");
            File.WriteAllBytes(segmentPath, crashImage);

            long replacementTail;
            using (var firstRecovery = new CrashImageLog(logPath, TestUtils.MethodTestDir))
            {
                Assert.That(firstRecovery.Manager.ListCommits(), Is.EqualTo(new long[] { 1 }));
                Assert.That(firstRecovery.Manager.GetCommitMetadata(1), Is.EqualTo(commitMetadata));

                // The scan stops at the torn entry, so recovery keeps exactly the acknowledged prefix
                await firstRecovery.Log.RecoverAsync(-1, clearAboveTail: clearMode == ClearMode.Recover).ConfigureAwait(false);
                if (clearMode == ClearMode.PrepareToAppend)
                    firstRecovery.Log.PrepareToAppend();
                ClassicAssert.AreEqual(acknowledgedTail, firstRecovery.Log.TailAddress);
                ClassicAssert.AreEqual(acknowledgedTail, firstRecovery.Log.CommittedUntilAddress);
                Assert.That(ReadEntries(firstRecovery.Log), Is.EqualTo([prefixEntry, prefixEntry, prefixEntry]));

                // Replace the abandoned suffix. This commit manager is new, so it writes this commit's commit-metadata file.
                ClassicAssert.AreEqual(acknowledgedTail, firstRecovery.Log.Enqueue(replacement));
                ClassicAssert.IsTrue(firstRecovery.Log.CommitStrongly(out replacementTail, out _, spinWait: true, replacementCookie, replacementCommitNum));
                firstRecovery.Commits.WaitForMetadata(replacementCommitNum, replacementTail);
            }

            var replacedImage = ReadDeviceFile(segmentPath);
            if (clearMode == ClearMode.None)
                Assert.That(replacedImage.AsSpan(PageSize).SequenceEqual(newImage.AsSpan(PageSize)), Is.True,
                    "Without clearing, the pages past the replacement's page still hold the abandoned entries and commit record");
            else
                Assert.That(replacedImage.AsSpan((int)replacementTail).IndexOfAnyExcept((byte)0), Is.EqualTo(-1),
                    "Clearing leaves nothing past the replacement");
            if (replacementFillsTailPage)
            {
                Assert.That(replacedImage.AsSpan((int)replacementTail, PageSize - (int)replacementTail).IndexOfAnyExcept((byte)0), Is.EqualTo(-1),
                    "The replacement's flush must zero the rest of the first page");
                Assert.That(replacementTail + HeaderSize + suffixEntry.Length, Is.GreaterThan(PageSize),
                    "A 3500-byte entry must not fit in the rest of the first page");
            }
            if (replacementMatchesIntermediateCommit)
                ClassicAssert.AreEqual(intermediateTail, replacementTail);

            using (var secondRecovery = new CrashImageLog(logPath, TestUtils.MethodTestDir))
            {
                var latestInfo = new TsavoriteLogRecoveryInfo();
                latestInfo.Initialize(secondRecovery.Manager.GetCommitMetadata(secondRecovery.Manager.ListCommits().First()));
                ClassicAssert.AreEqual(replacementTail, latestInfo.UntilAddress);

                // Recovery must end at the replacement and adopt none of the commit records beyond it
                await secondRecovery.Log.RecoverAsync().ConfigureAwait(false);
                ClassicAssert.AreEqual(replacementTail, secondRecovery.Log.TailAddress);
                ClassicAssert.AreEqual(replacementTail, secondRecovery.Log.CommittedUntilAddress);
                Assert.That(secondRecovery.Log.RecoveredCookie, Is.EqualTo(replacementCookie));
                Assert.That(ReadEntries(secondRecovery.Log), Is.EqualTo([prefixEntry, prefixEntry, prefixEntry, replacement]));
            }
        }

        private static byte[] ReadDeviceFile(string path)
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[stream.Length];
            stream.ReadExactly(bytes);
            return bytes;
        }

        private static List<byte[]> ReadEntries(TsavoriteLog log)
        {
            var entries = new List<byte[]>();
            using var iterator = log.Scan(log.BeginAddress, log.TailAddress, recover: false);
            while (iterator.GetNext(out var entry, out var length, out _))
                entries.Add(entry.AsSpan(0, length).ToArray());
            return entries;
        }

        /// <summary>
        /// A log with its own device and commit manager over one log file, configured as in the crash-image scenarios:
        /// 4 KB pages, per-entry checksums, and commit-metadata files throttled to every 256 commits.
        /// </summary>
        private sealed class CrashImageLog : IDisposable
        {
            internal readonly TsavoriteLog Log;
            internal readonly IDevice Device;
            internal readonly DeviceLogCommitCheckpointManager Manager;
            internal readonly NotifyingLogCommitManager Commits;
            bool disposed;

            internal CrashImageLog(string logPath, string commitDirectory)
            {
                Device = new RandomAccessLocalStorageDevice(logPath, disableFileBuffering: false, osReadBuffering: true);
                Manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(commitDirectory), removeOutdated: false, fastCommitThrottleFreq: 256);
                Commits = new NotifyingLogCommitManager(Manager);
                Log = new TsavoriteLog(new TsavoriteLogSettings
                {
                    LogDevice = Device,
                    LogCommitManager = Commits,
                    LogChecksum = LogChecksumType.PerEntry,
                    FastCommitMode = true,
                    TryRecoverLatest = false,
                    PageSizeBits = 12,
                    MemorySizeBits = 14,
                    SegmentSizeBits = 14
                });
            }

            public void Dispose()
            {
                if (disposed)
                    return;
                disposed = true;
                Log.Dispose();
                Manager.Dispose();
                Device.Dispose();
            }
        }
    }
}