// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for clearing what the device holds above a log's tail, by <see cref="TsavoriteLog.PrepareToAppend"/> or by recovering
    /// with clearAboveTail
    /// </summary>
    /// <remarks>
    /// Most tests write a log and close it, then open a new log over the same files to recover and clear it, and finally check
    /// the files directly. All logs in a test share one commit manager and its commit-metadata files. <see cref="WriteCommits"/>
    /// returns the tail of each commit indexed by its number, so tails[n] is where commit n ends.
    /// </remarks>
    [TestFixture]
    internal class LogClearAboveTailTests : TestBase
    {
        // 4 KB pages
        const int PageSizeBits = 12;

        // A 4-page buffer, so a gap of up to 3 empty pages is still crossed
        const int MemorySizeBits = 14;

        // 4 pages per segment, so a small log spans several segments
        const int SegmentSizeBits = 14;

        /// <summary>The log's base path: segment n is the file logPath.n</summary>
        string logPath;

        /// <summary>Writes the commit-metadata files; tests list and read them through it</summary>
        DeviceLogCommitCheckpointManager manager;

        /// <summary>
        /// Wraps <see cref="manager"/>, so that a test can wait until a commit's metadata is written. The logs commit through it.
        /// </summary>
        NotifyingLogCommitManager commits;

        /// <summary>How a test log's device clears the segments above the tail's segment</summary>
        internal enum ClearingDevice
        {
            /// <summary>Removes them: a file device that deletes their files</summary>
            Removes,

            /// <summary>Removes them, or empties the files it holds open: NativeStorageDevice</summary>
            RemovesOrEmpties,

            /// <summary>Cannot remove them, so clearing zeroes the data it finds above the tail instead</summary>
            CannotRemove,
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            logPath = Path.Join(TestUtils.MethodTestDir, "c.log");
            manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir), removeOutdated: false);
            commits = new NotifyingLogCommitManager(manager);
        }

        [TearDown]
        public void TearDown()
        {
            manager?.Dispose();
            manager = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Recover to commit 2 of 8 and clear above its tail, as part of recovery or afterwards with PrepareToAppend, on each kind
        /// of device. Only zeros remain at and above the tail, the commit-metadata files of commits 3 to 8 are gone, and the next
        /// recovery, which would otherwise end at commit 8, ends at commit 2.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingAboveTheTailRemovesEverythingAfterIt([Values] bool clearWhileRecovering, [Values] ClearingDevice clearingDevice)
        {
            // Eight commits of 48 100-byte entries span three segments; commit 2 ends in segment 0
            var entry = CreateEntry(100, seed: 1);
            List<long> tails;
            using (var original = new TestLog(logPath, commits))
                tails = WriteCommits(original.Log, commitCount: 8, entriesPerCommit: 48, entry);
            Assert.That(tails[8], Is.GreaterThan(2L << SegmentSizeBits), "the log should span three segments");

            // Recover to commit 2, and clear above its tail
            using (var recovered = new TestLog(logPath, commits, clearingDevice: clearingDevice))
            {
                await recovered.Log.RecoverAsync(2, clearAboveTail: clearWhileRecovering).ConfigureAwait(false);
                if (!clearWhileRecovering)
                    recovered.Log.PrepareToAppend();
                ClassicAssert.AreEqual(tails[2], recovered.Log.TailAddress);
            }

            // Only zeros remain at or above the tail. A device that can remove segments leaves bytes only in the tail's segment;
            // one that cannot keeps the files above it, zeroed.
            AssertNoDataFrom(tails[2]);
            if (clearingDevice != ClearingDevice.CannotRemove)
                Assert.That(SegmentsHoldingBytes(), Is.EqualTo(new long[] { 0 }), "the segments above the tail's segment should be removed or emptied");

            // The commit-metadata files of later commits are removed, and the file for the tail is rewritten
            Assert.That(manager.ListCommits(), Is.EqualTo(new long[] { 2, 1 }));
            var tailInfo = new TsavoriteLogRecoveryInfo();
            tailInfo.Initialize(manager.GetCommitMetadata(2));
            ClassicAssert.AreEqual(tails[2], tailInfo.UntilAddress);

            // The next recovery ends at commit 2's tail, with the 96 entries of commits 1 and 2
            using (var reopened = new TestLog(logPath, commits))
            {
                await reopened.Log.RecoverAsync().ConfigureAwait(false);
                ClassicAssert.AreEqual(tails[2], reopened.Log.TailAddress);
                Assert.That(ReadEntries(reopened.Log), Has.Count.EqualTo(96));
            }
        }

        /// <summary>
        /// Recovering to an earlier commit without clearing leaves the segment files and the commit-metadata files unchanged, so a
        /// later recovery still reaches the last commit
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task RecoveringWithoutClearingLeavesTheDeviceUnchanged()
        {
            // Write eight commits, and save the contents of the segment files and the list of commit-metadata files
            var entry = CreateEntry(100, seed: 2);
            List<long> tails;
            using (var original = new TestLog(logPath, commits))
                tails = WriteCommits(original.Log, commitCount: 8, entriesPerCommit: 48, entry);
            var segmentsBefore = ReadSegments();
            var commitsBefore = manager.ListCommits().ToArray();

            // Recover to commit 2, without clearing
            using (var recovered = new TestLog(logPath, commits))
            {
                await recovered.Log.RecoverAsync(2).ConfigureAwait(false);
                ClassicAssert.AreEqual(tails[2], recovered.Log.TailAddress);
            }

            Assert.That(ReadSegments(), Is.EqualTo(segmentsBefore));
            Assert.That(manager.ListCommits(), Is.EqualTo(commitsBefore));

            // The later commits are still there to recover
            using var reopened = new TestLog(logPath, commits);
            await reopened.Log.RecoverAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual(tails[8], reopened.Log.TailAddress);
        }

        /// <summary>
        /// PrepareToAppend clears from the tail at which the log was positioned, so once anything is appended there, it would erase
        /// that: it throws instead
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task PrepareToAppendThrowsOnceTheLogHasBeenAppendedTo()
        {
            var entry = CreateEntry(100, seed: 3);
            using (var original = new TestLog(logPath, commits))
                _ = WriteCommits(original.Log, commitCount: 2, entriesPerCommit: 10, entry);

            // Recover without clearing, and append before calling PrepareToAppend
            using var recovered = new TestLog(logPath, commits);
            await recovered.Log.RecoverAsync().ConfigureAwait(false);
            _ = recovered.Log.Enqueue(entry);
            var ex = Assert.Throws<TsavoriteException>(recovered.Log.PrepareToAppend);
            StringAssert.Contains("before anything is appended", ex.Message);
        }

        /// <summary>
        /// Once the log has been cleared since it was positioned, PrepareToAppend does nothing, even after an append, which would
        /// make it throw on a log not yet cleared
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task PrepareToAppendDoesNothingOnceTheLogHasBeenCleared()
        {
            var entry = CreateEntry(100, seed: 4);
            using (var original = new TestLog(logPath, commits))
                _ = WriteCommits(original.Log, commitCount: 2, entriesPerCommit: 10, entry);

            // Recovery clears above the tail, so the append that follows does not make PrepareToAppend throw
            using var recovered = new TestLog(logPath, commits);
            await recovered.Log.RecoverAsync(-1, clearAboveTail: true).ConfigureAwait(false);
            _ = recovered.Log.Enqueue(entry);
            Assert.DoesNotThrow(recovered.Log.PrepareToAppend);
        }

        /// <summary>
        /// A read-only log cannot be cleared: recovering it with clearAboveTail throws, and so does PrepareToAppend after it is
        /// recovered read-only
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingIsRejectedForAReadOnlyLog()
        {
            var entry = CreateEntry(100, seed: 5);
            using (var original = new TestLog(logPath, commits))
                _ = WriteCommits(original.Log, commitCount: 2, entriesPerCommit: 10, entry);

            using var readOnly = new TestLog(logPath, commits, readOnly: true);
            _ = Assert.ThrowsAsync<TsavoriteException>(async () => await readOnly.Log.RecoverAsync(-1, clearAboveTail: true).ConfigureAwait(false));

            // A read-only recovery succeeds, but the log still cannot be prepared for appending
            await readOnly.Log.RecoverReadOnlyAsync().ConfigureAwait(false);
            _ = Assert.Throws<TsavoriteException>(readOnly.Log.PrepareToAppend);
        }

        /// <summary>
        /// A recovery that fails on a device error leaves the log empty, while the device may still hold its commits, so
        /// PrepareToAppend throws rather than erase them. Reset discards the log deliberately, after which PrepareToAppend clears
        /// the device.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task PrepareToAppendIsRejectedUntilAFailedRecoveryIsResolved()
        {
            var entry = CreateEntry(100, seed: 6);
            List<long> tails;
            using (var original = new TestLog(logPath, commits))
                tails = WriteCommits(original.Log, commitCount: 3, entriesPerCommit: 10, entry);

            // The page after commit 3's, which the recovery scan reads to look for later commits
            var pageSize = 1L << PageSizeBits;
            var firstUnwrittenPage = (tails[3] | (pageSize - 1)) + 1;

            var failing = new ErrorCodeOnReadDevice(new RandomAccessLocalStorageDevice(logPath, disableFileBuffering: false, osReadBuffering: true));
            using var recovered = new TestLog(logPath, commits, device: failing);

            // ERROR_IO_DEVICE: a genuine failure, so recovery cannot tell where the log ends
            failing.ReadErrorFromAddress = firstUnwrittenPage;
            failing.ReadErrorCode = 1117;
            _ = Assert.ThrowsAsync<TsavoriteIOException>(async () => await recovered.Log.RecoverAsync().ConfigureAwait(false));

            // Clearing above the empty log that failed recovery leaves would discard the log
            var ex = Assert.Throws<TsavoriteException>(recovered.Log.PrepareToAppend);
            StringAssert.Contains("Recovery did not determine", ex.Message);

            // Resetting the log discards it deliberately, after which it can be appended to. Reads must succeed again, since
            // clearing reads the device to find its data, and the device is closed before its files are checked.
            failing.ReadErrorCode = 0;
            recovered.Log.Reset();
            recovered.Log.PrepareToAppend();
            recovered.Dispose();
            AssertNoDataFrom(recovered.Log.BeginAddress);
        }

        /// <summary>
        /// Initialize restarts the log at the end of commit 1 and its commit numbering at 0, as replica sync does. The earlier
        /// history's commit-metadata files outnumber the new commits, so PrepareToAppend must remove them along with that
        /// history's data, and recovery then finds only what was appended after the restart.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task PrepareToAppendAfterInitializeIgnoresTheEarlierHistory()
        {
            var entry = CreateEntry(100, seed: 7);
            var replacement = CreateEntry(64, seed: 8);
            long replacementTail;
            List<long> tails;
            using (var original = new TestLog(logPath, commits))
            {
                // Four commits, then a restart at the end of commit 1, which leaves the device unchanged
                tails = WriteCommits(original.Log, commitCount: 4, entriesPerCommit: 40, entry);
                original.Log.Initialize(tails[1], tails[1], lastCommitNum: 0);
                ClassicAssert.AreEqual(tails[1], original.Log.TailAddress);

                // The files of commits 1 to 4 are removed, and one numbered 0 is written for the restart position
                original.Log.PrepareToAppend();
                Assert.That(manager.ListCommits(), Is.EqualTo(new long[] { 0 }));

                // The first commit after the restart is numbered 1, below the earlier history's 2 to 4
                ClassicAssert.AreEqual(tails[1], original.Log.Enqueue(replacement));
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out replacementTail, out var replacementCommitNum, spinWait: true));
                ClassicAssert.AreEqual(1, replacementCommitNum);
                commits.WaitForMetadata(replacementCommitNum, replacementTail);
            }

            AssertNoDataFrom(replacementTail);

            // Recovery starts from the restart position and finds only the replacement
            using var recovered = new TestLog(logPath, commits);
            await recovered.Log.RecoverAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual(replacementTail, recovered.Log.TailAddress);
            ClassicAssert.AreEqual(tails[1], recovered.Log.BeginAddress);
            Assert.That(ReadEntries(recovered.Log), Is.EqualTo([replacement]));
        }

        /// <summary>
        /// A crash can leave pages unwritten between written ones, but fewer of them than the buffer holds. With the longest such
        /// gap inside the tail's segment and data on both sides, clearing must cross the gap and zero the data beyond it, whether
        /// or not the device removes the segments above the tail's.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingFindsDataPastAGapShorterThanTheBuffer([Values] ClearingDevice clearingDevice)
        {
            // Segments of 8 pages, so the tail's segment holds the gap and data on both sides of it
            const int segmentSizeBits = 15;
            var entry = CreateEntry(100, seed: 9);
            List<long> tails;
            using (var original = new TestLog(logPath, commits, segmentSizeBits: segmentSizeBits))
                tails = WriteCommits(original.Log, commitCount: 8, entriesPerCommit: 48, entry);

            // Zero a gap one page shorter than the 4-page buffer, starting two pages after the tail's page, so that data lies on
            // both sides of it within the tail's segment
            var tailPage = tails[1] >> PageSizeBits;
            var gapPages = (1 << (MemorySizeBits - PageSizeBits)) - 1;
            var firstPageAfterGap = tailPage + 2 + gapPages;
            const int pageToSegmentBits = segmentSizeBits - PageSizeBits;
            Assert.That(firstPageAfterGap >> pageToSegmentBits, Is.EqualTo(tailPage >> pageToSegmentBits), "the gap and the data after it should be in the tail's segment");
            Assert.That(tails[8] >> PageSizeBits, Is.GreaterThan(firstPageAfterGap), "there should be data after the gap");
            for (var page = tailPage + 2; page < firstPageAfterGap; page++)
                ZeroRange(page << PageSizeBits, 1 << PageSizeBits, segmentSizeBits);

            // Recover to commit 1 and clear above its tail: nothing may remain at or above it, including the data after the gap
            using (var recovered = new TestLog(logPath, commits, segmentSizeBits: segmentSizeBits, clearingDevice: clearingDevice))
                await recovered.Log.RecoverAsync(1, clearAboveTail: true).ConfigureAwait(false);

            AssertNoDataFrom(tails[1], segmentSizeBits);
        }

        /// <summary>
        /// A replica's log can hold old segments far above the tail that a full sync sets, with nothing in between: its lower
        /// segments were truncated, and Initialize positions the log below the rest. No search from the tail can tell where those
        /// segments are, so the device removes every segment it holds above the tail's. A device that cannot remove segments is
        /// not tested, since clearing cannot find such segments on it.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingRemovesSegmentsBeyondALongGap([Values(ClearingDevice.Removes, ClearingDevice.RemovesOrEmpties)] ClearingDevice clearingDevice)
        {
            // Twenty commits span at least six segments
            var entry = CreateEntry(100, seed: 14);
            var replacement = CreateEntry(64, seed: 15);
            List<long> tails;
            using (var original = new TestLog(logPath, commits))
                tails = WriteCommits(original.Log, commitCount: 20, entriesPerCommit: 48, entry);
            var lastSegment = tails[20] >> SegmentSizeBits;
            Assert.That(lastSegment, Is.GreaterThanOrEqualTo(5), "the log should span at least six segments");

            // Keep only the last two segments, as truncation would
            for (var segment = 0L; segment < lastSegment - 1; segment++)
                File.Delete(SegmentPath(segment));

            long replacementTail;
            using (var replica = new TestLog(logPath, commits, clearingDevice: clearingDevice))
            {
                // Restart the log at address 0, far below the remaining segments, as a full sync can, and clear above it
                replica.Log.Initialize(beginAddress: 0, committedUntilAddress: 0, lastCommitNum: 0);
                replica.Log.PrepareToAppend();

                // Initialize resets the device, which leaves NativeStorageDevice holding no segment above the tail's open, so
                // every device deletes the old segments' files
                Assert.That(Segments().Select(segment => segment.Segment), Has.None.GreaterThan(0), "the old segments' files should be deleted");

                // Append and commit after the restart
                _ = replica.Log.Enqueue(replacement);
                ClassicAssert.IsTrue(replica.Log.CommitStrongly(out replacementTail, out var commitNum, spinWait: true));
                commits.WaitForMetadata(commitNum, replacementTail);
            }

            // Recovery finds only what was appended after the restart
            using var recovered = new TestLog(logPath, commits);
            await recovered.Log.RecoverAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual(replacementTail, recovered.Log.TailAddress);
            Assert.That(ReadEntries(recovered.Log), Is.EqualTo([replacement]));
        }

        /// <summary>
        /// A torn write can lose a page's first sector and keep later ones. Clearing probes each page by its first sector, so this
        /// checks that it still reads such a page in full when it is the last page that holds data, and so no later page reveals
        /// that the data continues. It uses 8 KB pages, several to an IO chunk, and 2 MB pages, each spanning two IO chunks.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingFindsAPageWhoseFirstSectorIsEmpty([Values(13, 21)] int pageSizeBits)
        {
            // A 2-page buffer and 4-page segments. Commit 1 is a single entry on page 0, and commit 2 adds 16 more, each an eighth
            // of a page, so it ends two pages later.
            var pageSize = 1L << pageSizeBits;
            var entry = CreateEntry((int)(pageSize / 8), seed: 10);
            long commit1Tail, lastTail;
            int sectorSize;
            using (var original = new TestLog(logPath, commits, pageSizeBits, memorySizeBits: pageSizeBits + 1, segmentSizeBits: pageSizeBits + 2))
            {
                sectorSize = (int)original.Device.SectorSize;
                _ = original.Log.Enqueue(entry);
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out commit1Tail, out var commitNum, spinWait: true));
                for (var i = 0; i < 16; i++)
                    _ = original.Log.Enqueue(entry);
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out lastTail, out commitNum, spinWait: true));
                commits.WaitForMetadata(commitNum, lastTail);
            }
            Assert.That(sectorSize, Is.LessThan(pageSize), "a page must have sectors after its first");

            // The last full page keeps only the sectors after its first, and the page after it is lost entirely
            var lastFullPage = (lastTail >> pageSizeBits) - 1;
            Assert.That(lastFullPage, Is.GreaterThan(commit1Tail >> pageSizeBits));
            ZeroRange(lastFullPage << pageSizeBits, sectorSize, pageSizeBits + 2);
            ZeroRange((lastFullPage + 1) << pageSizeBits, (int)pageSize, pageSizeBits + 2);

            // Recover to commit 1 and clear above its tail: the data on the last full page must be found and zeroed
            using (var recovered = new TestLog(logPath, commits, pageSizeBits, memorySizeBits: pageSizeBits + 1, segmentSizeBits: pageSizeBits + 2))
                await recovered.Log.RecoverAsync(1, clearAboveTail: true).ConfigureAwait(false);

            AssertNoDataFrom(commit1Tail, pageSizeBits + 2);
        }

        /// <summary>
        /// Clearing reads past the end of the log to find where its data ends, and on a file device, reading a segment creates its
        /// file. With the tail on the first of its segment's four pages, the three empty pages after it are fewer than the buffer
        /// holds, so a search that did not stop at the end of the tail's segment would read on into the next and create it.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingACleanLogLeavesNoSegmentsAboveTheTail()
        {
            // Two small commits leave the tail on page 0, the first of segment 0's four pages
            var entry = CreateEntry(100, seed: 12);
            using (var original = new TestLog(logPath, commits))
                _ = WriteCommits(original.Log, commitCount: 2, entriesPerCommit: 10, entry);
            Assert.That(Segments().Select(segment => segment.Segment), Is.EqualTo(new long[] { 0 }));

            // After recovering and clearing, segment 0 is still the only segment file
            using (var recovered = new TestLog(logPath, commits))
                await recovered.Log.RecoverAsync(-1, clearAboveTail: true).ConfigureAwait(false);

            Assert.That(Segments().Select(segment => segment.Segment), Is.EqualTo(new long[] { 0 }));
        }

        /// <summary>
        /// If clearing is interrupted, what remains above the tail must be the lower part of what was there, so each change to the
        /// device must be below the one before, ending with the sector that holds the tail. Removing the segments above the tail's
        /// counts as one change; a device that cannot remove them has them zeroed instead.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingChangesTheDeviceFromTheHighestAddressDown([Values] bool canRemoveSegments)
        {
            // 64 KB pages, a 2-page buffer, and 4 MB segments, so that zeroing the tail's segment takes several 1 MB writes
            const int pageSizeBits = 16, memorySizeBits = 17, segmentSizeBits = 22;

            // Commit 1 is one 8 KB entry, and commit 2 adds 1280 more, 10 MB, so the log spans three segments
            var entry = CreateEntry(8 << 10, seed: 13);
            long commit1Tail, lastTail;
            using (var original = new TestLog(logPath, commits, pageSizeBits, memorySizeBits, segmentSizeBits))
            {
                _ = original.Log.Enqueue(entry);
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out commit1Tail, out var commitNum, spinWait: true));
                for (var i = 0; i < 1280; i++)
                    _ = original.Log.Enqueue(entry);
                ClassicAssert.IsTrue(original.Log.CommitStrongly(out lastTail, out commitNum, spinWait: true));
                commits.WaitForMetadata(commitNum, lastTail);
            }
            Assert.That(lastTail, Is.GreaterThan(2L << segmentSizeBits), "the log should span three segments");

            // Recover to commit 1 and clear above its tail, recording each write and segment removal in order
            var device = new RecordingDevice(new RandomAccessLocalStorageDevice(logPath, disableFileBuffering: false, osReadBuffering: true), canRemoveSegments);
            using (var recovered = new TestLog(logPath, commits, pageSizeBits, memorySizeBits, segmentSizeBits, device: device))
                await recovered.Log.RecoverAsync(1, clearAboveTail: true).ConfigureAwait(false);

            // Several writes zero the tail's segment, each change ends at or below the start of the one before, and the last
            // rewrites the sector that holds the tail
            var changes = device.Changes.ToArray();
            Assert.That(changes.Count(change => change.End <= 1L << segmentSizeBits), Is.GreaterThan(2), "zeroing the tail's segment should take several writes");
            for (var i = 1; i < changes.Length; i++)
                Assert.That(changes[i].End, Is.LessThanOrEqualTo(changes[i - 1].Start), $"change {i} should be below the one before");
            Assert.That(changes[^1].Start, Is.EqualTo(commit1Tail & ~(device.SectorSize - 1L)), "the sector that holds the tail should be changed last");
            AssertNoDataFrom(commit1Tail, segmentSizeBits);
        }

        /// <summary>
        /// LocalMemoryDevice reports reading a segment it never wrote as not found, rather than as the end of a file. Both the
        /// recovery scan of an empty log and clearing above its tail read such segments, and must treat them as empty.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public async Task ClearingTreatsSegmentsThatDoNotExistAsEmpty()
        {
            var entry = CreateEntry(100, seed: 11);
            using var device = new LocalMemoryDevice(capacity: 1L << 20, segmentSize: 1L << SegmentSizeBits, parallelism: 1);
            using var log = new TsavoriteLog(TestLog.CreateSettings(device, commits, PageSizeBits, MemorySizeBits, SegmentSizeBits));

            // Recovering the empty log and clearing above its tail find nothing, and leave the log empty
            await log.RecoverAsync(-1, clearAboveTail: true).ConfigureAwait(false);
            ClassicAssert.IsNull(log.RecoveredCookie);
            ClassicAssert.AreEqual(log.BeginAddress, log.TailAddress);

            // The log can then be appended to and committed
            ClassicAssert.AreEqual(log.BeginAddress, log.Enqueue(entry));
            ClassicAssert.IsTrue(log.CommitStrongly(out _, out _, spinWait: true));
        }

        /// <summary>
        /// Append <paramref name="commitCount"/> commits of <paramref name="entriesPerCommit"/> entries each, and wait for their
        /// commit-metadata files
        /// </summary>
        /// <returns>The tail of each commit, indexed by commit number</returns>
        List<long> WriteCommits(TsavoriteLog log, int commitCount, int entriesPerCommit, byte[] entry)
        {
            List<long> tails = [0];
            long commitNum = 0, tail = 0;
            for (var commit = 0; commit < commitCount; commit++)
            {
                for (var i = 0; i < entriesPerCommit; i++)
                    _ = log.Enqueue(entry);
                ClassicAssert.IsTrue(log.CommitStrongly(out tail, out commitNum, spinWait: true));
                ClassicAssert.AreEqual(tails.Count, commitNum);
                tails.Add(tail);
            }
            commits.WaitForMetadata(commitNum, tail);
            return tails;
        }

        /// <summary>An entry of <paramref name="length"/> random bytes, the same for the same <paramref name="seed"/></summary>
        static byte[] CreateEntry(int length, int seed)
        {
            var entry = new byte[length];
            new Random(seed).NextBytes(entry);
            return entry;
        }

        /// <summary>The entries from the log's begin address to its tail, in order</summary>
        static List<byte[]> ReadEntries(TsavoriteLog log)
        {
            List<byte[]> entries = [];
            using var iterator = log.Scan(log.BeginAddress, log.TailAddress, recover: false);
            while (iterator.GetNext(out var entry, out var length, out _))
                entries.Add(entry.AsSpan(0, length).ToArray());
            return entries;
        }

        /// <summary>The file that holds segment <paramref name="segment"/> of the log</summary>
        string SegmentPath(long segment) => $"{logPath}.{segment}";

        /// <summary>The log's segment files with their segment numbers, in ascending order of segment</summary>
        IEnumerable<(long Segment, string Path)> Segments()
            => Directory.GetFiles(TestUtils.MethodTestDir, Path.GetFileName(logPath) + ".*")
                .Select(path => (long.Parse(Path.GetExtension(path).AsSpan(1)), path))
                .OrderBy(segment => segment.Item1);

        /// <summary>The contents of each segment file, by segment number</summary>
        Dictionary<long, byte[]> ReadSegments() => Segments().ToDictionary(segment => segment.Segment, segment => File.ReadAllBytes(segment.Path));

        /// <summary>The segments whose files hold any bytes; NativeStorageDevice empties, rather than deletes, the files it holds open</summary>
        IEnumerable<long> SegmentsHoldingBytes() => Segments().Where(segment => new FileInfo(segment.Path).Length > 0).Select(segment => segment.Segment);

        /// <summary>Assert that the device holds nothing but zeros at and above <paramref name="address"/></summary>
        void AssertNoDataFrom(long address, int segmentSizeBits = SegmentSizeBits)
        {
            foreach (var (segment, path) in Segments())
            {
                var bytes = File.ReadAllBytes(path);
                var from = (int)Math.Clamp(address - (segment << segmentSizeBits), 0, bytes.Length);
                var data = bytes.AsSpan(from).IndexOfAnyExcept((byte)0);
                Assert.That(data, Is.EqualTo(-1), $"segment {segment} holds data at address {(segment << segmentSizeBits) + from + data}, at or above {address}");
            }
        }

        /// <summary>
        /// Overwrite <paramref name="length"/> bytes at log address <paramref name="address"/> with zeros, directly in the segment
        /// file, as if a crash had left them unwritten. The log must be closed, and the range must lie within one segment of the
        /// size that <paramref name="segmentSizeBits"/> gives.
        /// </summary>
        void ZeroRange(long address, int length, int segmentSizeBits)
        {
            using var file = new FileStream(SegmentPath(address >> segmentSizeBits), FileMode.Open, FileAccess.Write);
            file.Position = address & ((1L << segmentSizeBits) - 1);
            file.Write(new byte[length]);
        }

        /// <summary>
        /// A fast-commit log with per-entry checksums over a device that keeps its files: the one given, or else one of the kind
        /// <see cref="ClearingDevice"/> names. Its sizes default to the fixture's, and it is not recovered on construction.
        /// Disposing it disposes the log, then the device, which the log does not own.
        /// </summary>
        sealed class TestLog : IDisposable
        {
            internal readonly IDevice Device;
            internal readonly TsavoriteLog Log;

            /// <summary>Whether this has been disposed; a test can dispose it before the end of its using block</summary>
            bool disposed;

            /// <summary>
            /// Create a log over the segment files at <paramref name="logPath"/>, through <paramref name="device"/> if one is given,
            /// and otherwise through a new device of the kind <paramref name="clearingDevice"/> names
            /// </summary>
            internal TestLog(string logPath, ILogCommitManager commits, int pageSizeBits = PageSizeBits, int memorySizeBits = MemorySizeBits,
                int segmentSizeBits = SegmentSizeBits, bool readOnly = false, IDevice device = null, ClearingDevice clearingDevice = ClearingDevice.Removes)
            {
                Device = device ?? clearingDevice switch
                {
                    ClearingDevice.RemovesOrEmpties => new NativeStorageDevice(logPath, deleteOnClose: false, disableFileBuffering: false),
                    ClearingDevice.CannotRemove => new RecordingDevice(CreateFileDevice(logPath), canRemoveSegments: false),
                    _ => CreateFileDevice(logPath),
                };
                var settings = CreateSettings(Device, commits, pageSizeBits, memorySizeBits, segmentSizeBits);
                settings.ReadOnlyMode = readOnly;
                Log = new TsavoriteLog(settings);
            }

            /// <summary>A file device that keeps its segment files when disposed, so a later log can recover them</summary>
            static IDevice CreateFileDevice(string logPath) => new RandomAccessLocalStorageDevice(logPath, disableFileBuffering: false, osReadBuffering: true);

            /// <summary>Settings for a fast-commit log with per-entry checksums, which is not recovered on construction</summary>
            internal static TsavoriteLogSettings CreateSettings(IDevice device, ILogCommitManager commits, int pageSizeBits, int memorySizeBits, int segmentSizeBits)
                => new()
                {
                    LogDevice = device,
                    LogCommitManager = commits,
                    LogChecksum = LogChecksumType.PerEntry,
                    FastCommitMode = true,
                    TryRecoverLatest = false,
                    PageSizeBits = pageSizeBits,
                    MemorySizeBits = memorySizeBits,
                    SegmentSizeBits = segmentSizeBits,
                };

            public void Dispose()
            {
                if (disposed)
                    return;
                disposed = true;
                Log.Dispose();
                Device.Dispose();
            }
        }

        /// <summary>
        /// A device that forwards to another, and records in order the address range of each write and of each removal of
        /// segments. Unless <c>canRemoveSegments</c> is set, it reports that it cannot remove the segments above a segment, as a
        /// device without that support does, so clearing zeroes them instead.
        /// </summary>
        /// <param name="underlying">The device to forward to; disposed with this one</param>
        /// <param name="canRemoveSegments">Whether to forward <see cref="IDevice.TryRemoveSegmentsAbove"/></param>
        sealed class RecordingDevice(IDevice underlying, bool canRemoveSegments = true) : StorageDeviceBase(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            /// <summary>
            /// The address range [Start, End) of each change, in the order the device received them. A removal of the segments
            /// above a segment ends at long.MaxValue.
            /// </summary>
            internal readonly ConcurrentQueue<(long Start, long End)> Changes = new();

            /// <summary>Initialize this device, whose segment size the recorded ranges use, and the one it forwards to</summary>
            public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
            {
                base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
                underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            }

            /// <summary>Record the removal and forward it, or report that this device cannot remove segments</summary>
            public override bool TryRemoveSegmentsAbove(int segment, out int removed)
            {
                removed = 0;
                if (!canRemoveSegments)
                    return false;
                Changes.Enqueue(((segment + 1) * SegmentSize, long.MaxValue));
                return underlying.TryRemoveSegmentsAbove(segment, out removed);
            }

            /// <summary>Record the removal of one segment, and forward it</summary>
            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            {
                Changes.Enqueue((segment * SegmentSize, (segment + 1) * SegmentSize));
                underlying.RemoveSegmentAsync(segment, callback, result);
            }

            /// <summary>Record the write's address range, and forward it</summary>
            public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
            {
                var start = segmentId * SegmentSize + (long)destinationAddress;
                Changes.Enqueue((start, start + numBytesToWrite));
                underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
            }

            public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

            public override void Dispose() => underlying.Dispose();
        }
    }
}