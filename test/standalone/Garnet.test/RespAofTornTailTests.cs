// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Recovery tests for the tail of the AOF. A "torn tail" - an ungraceful shutdown that leaves the final commit
    /// record half-written - must be reported, and recovery must not crash (FailOnRecoveryError is set so a bad
    /// recovery surfaces as a startup exception) and must replay every committed operation up to the last valid
    /// commit. An intact tail must recover without reporting anything. What the AOF holds above the recovered tail
    /// must be cleared before anything is appended, unless clearing is turned off.
    /// </summary>
    [TestFixture]
    public class RespAofTornTailTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void TornTailCommitRecordRecovery()
        {
            const int numKeys = 1000;

            // 1. Start a server with AOF on, WaitForCommit (ack == durable), and FailOnRecoveryError
            //    so a bad recovery surfaces as a crash instead of being silently swallowed.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitWait: true, lowMemory: true, failOnRecoveryError: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < numKeys; i++)
                    db.StringSet("key" + i, "value" + i); // each SET is durably committed + acked
            }

            // 2. Shut down WITHOUT deleting files.
            server.Dispose(false);
            server = null;

            // 3. Simulate the torn tail on disk.
            SimulateTornTailWrite(TestUtils.MethodTestDir);

            // 4. Recover. With the fix, recovery stops at the last VALID commit and must not throw
            //    even though FailOnRecoveryError is set. (Without the fix this throws on replay.)
            var recoveryLog = new LogCapture();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitWait: true, lowMemory: true, failOnRecoveryError: true,
                logTo: recoveryLog);
            Assert.DoesNotThrow(() => server.Start(),
                "recovery must not crash on a torn-tail commit record");
            Assert.That(AofWarnings(recoveryLog.ToString()), Has.Some.Contains("torn"),
                "the dropped torn-tail commit record must be reported");

            // 5. All committed operations up to the last valid commit must be replayed. The single
            //    torn-tail operation (key{numKeys-1}) may be dropped, so assert through numKeys-2.
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < numKeys - 1; i++)
                    ClassicAssert.AreEqual("value" + i, db.StringGet("key" + i).ToString(),
                        $"committed key{i} must be recovered from the AOF");

                // The AOF must remain writable after recovering from a torn tail.
                for (int i = numKeys; i < numKeys * 2; i++)
                    db.StringSet("key" + i, "value" + i);
            }

            // 6. Kill and recover again: the newly written data must itself be durably recoverable.
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitWait: true, lowMemory: true, failOnRecoveryError: true);
            Assert.DoesNotThrow(() => server.Start(),
                "second recovery (after post-recovery writes) must not crash");

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < numKeys - 1; i++)
                    ClassicAssert.AreEqual("value" + i, db.StringGet("key" + i).ToString(),
                        $"originally committed key{i} must survive re-recovery");
                for (int i = numKeys; i < numKeys * 2; i++)
                    ClassicAssert.AreEqual("value" + i, db.StringGet("key" + i).ToString(),
                        $"post-recovery key{i} must be recovered from the AOF");
            }
        }

        /// <summary>
        /// Recovery reads to the end of the written AOF to find its latest commit. Reaching that end is how recovery
        /// completes, whether the AOF is empty (a first start with recovery enabled) or holds committed data.
        /// </summary>
        [Test]
        public void IntactAofRecoveryReportsNoWarnings()
        {
            const int numKeys = 1000;
            var log = new LogCapture();

            // A first start with recovery enabled recovers an empty AOF
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitWait: true, lowMemory: true, logTo: log);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < numKeys; i++)
                    db.StringSet("key" + i, "value" + i);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitWait: true, lowMemory: true, failOnRecoveryError: true, logTo: log);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < numKeys; i++)
                    ClassicAssert.AreEqual("value" + i, db.StringGet("key" + i).ToString(), $"key{i} must be recovered from the AOF");
            }

            Assert.That(AofWarnings(log.ToString()), Is.Empty, "recovering an intact AOF must not report warnings or errors");
        }

        /// <summary>
        /// Recovery that stops short of the end of the AOF leaves intact commits above the recovered tail. Here a corrupted
        /// record stops it, and a segment file above the tail's segment stands for a history that a replica sync replaced.
        /// With clearing on, both are erased before anything is appended. With clearing off, the records appended at the
        /// tail are laid out exactly as the old ones were and end in a page's padding, while the next page still holds old
        /// records. The next recovery then passes the padding into the old commits and adopts them, restoring keys that the
        /// first recovery did not. The test asserts that too, which shows that it reaches them.
        /// </summary>
        [Test]
        public void RecoveryClearsAofAboveTail([Values] bool clearAboveTail)
        {
            const int numKeys = 400;
            const string aofPageSize = "8k";
            const int aofPageBytes = 8 << 10;

            // Each SET is committed before the next one is issued (a commit per operation, and commitWait), so the AOF holds a
            // SET record and then its commit record for each key. Keys and values all have the same length, so all SET records
            // are the same size, as are all commit records. Small AOF pages spread them over several pages.
            server = CreateServer(tryRecover: false);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < numKeys; i++)
                    db.StringSet(Key(i), Value(i));
            }
            server.Dispose(false);
            server = null;

            var aofDir = Path.Combine(TestUtils.MethodTestDir, "AOF");
            var tailSegment = Path.Combine(aofDir, "aof.log.0");
            var staleSegment = Path.Combine(aofDir, "aof.log.5");
            var records = ReadRecords(tailSegment, aofPageBytes);

            // Zero the length of the first record on page 1, a SET record, so that recovery ends the log there. Its tail is then
            // at the start of page 0's padding, so the new keys' records start on page 1 too, at the same offsets as the old.
            var firstLostKey = records.Find(r => r.Offset == aofPageBytes).Key;
            Assert.That(firstLostKey, Is.GreaterThan(0), "page 1 must start with a SET record");
            ZeroLength(tailSegment, aofPageBytes);

            // The new keys' records end with the commit record before the first later page that starts with a SET record.
            // Padding follows that commit record, since the SET record did not fit on its page, so recovery passes from it to
            // the old records on the next page. Old records later on the same page as the tail would not be reached: a flush
            // writes whole sectors, which zeroes the rest of the tail's sector, and recovery stops at data after zeros on a page.
            var firstStaleKey = records.Find(r => r.Offset > aofPageBytes && r.Offset % aofPageBytes == 0 && r.Key >= 0).Key;
            Assert.That(firstStaleKey, Is.GreaterThan(firstLostKey).And.LessThan(numKeys - 1), "a later page must start with a SET record that old records follow");
            var numNewKeys = firstStaleKey - firstLostKey;

            // A segment file far above the tail's, standing for a history that a replica sync replaced
            File.WriteAllBytes(staleSegment, Enumerable.Repeat((byte)0x5A, 4096).ToArray());

            // First recovery: the keys before the corrupted record come back. The new keys' records are then appended at the
            // recovered tail, where the lost keys' records were.
            server = CreateServer(tryRecover: true);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < numKeys; i++)
                    ClassicAssert.AreEqual(i < firstLostKey ? Value(i) : null, (string)db.StringGet(Key(i)), $"first recovery, {Key(i)}");
                for (var i = 0; i < numNewKeys; i++)
                    db.StringSet(NewKey(i), Value(i));
            }
            server.Dispose(false);
            server = null;

            ClassicAssert.AreEqual(!clearAboveTail, HoldsData(staleSegment), "the segment file above the tail's segment must be removed or emptied only when clearing");
            ClassicAssert.AreEqual(!clearAboveTail, FileContains(tailSegment, Key(numKeys - 1)), "the records above the tail must be zeroed only when clearing");

            // Second recovery: the new keys come back, and without clearing, so do the old keys from the first stale one on
            server = CreateServer(tryRecover: true);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < numNewKeys; i++)
                    ClassicAssert.AreEqual(Value(i), (string)db.StringGet(NewKey(i)), $"second recovery, {NewKey(i)}");
                for (var i = 0; i < numKeys; i++)
                {
                    var restored = i < firstLostKey || (!clearAboveTail && i >= firstStaleKey);
                    ClassicAssert.AreEqual(restored ? Value(i) : null, (string)db.StringGet(Key(i)), $"second recovery, {Key(i)}");
                }
            }

            GarnetServer CreateServer(bool tryRecover)
                => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: tryRecover, enableAOF: true, commitWait: true, lowMemory: true,
                    aofPageSize: aofPageSize, aofMemorySize: "32k", failOnRecoveryError: true, aofReplayClearAboveTail: clearAboveTail);

            static string Key(int i) => $"key{i:D4}";
            static string NewKey(int i) => $"new{i:D4}";
            static string Value(int i) => $"value{i:D4}";
        }

        /// <summary>
        /// The warning, error, and critical entries the AOF logger wrote to a <see cref="NUnitLoggerProvider"/> log,
        /// each from its category through its message.
        /// </summary>
        private static string[] AofWarnings(string log)
            => [.. Regex.Matches(log, @"\((?:warn|errr|crit)\)\] \|[^|]*\| <GarnetLog \[aof\]>.*?\^.*?\^", RegexOptions.Singleline).Select(m => m.Value)];

        /// <summary>
        /// Collects log output, serializing reads with the writes that server threads may still be making.
        /// </summary>
        private sealed class LogCapture : TextWriter
        {
            private readonly StringBuilder text = new();

            /// <inheritdoc/>
            public override Encoding Encoding => Encoding.Unicode;

            /// <inheritdoc/>
            public override void Write(char value)
            {
                lock (text)
                {
                    _ = text.Append(value);
                }
            }

            /// <inheritdoc/>
            public override void Write(string value)
            {
                lock (text)
                {
                    _ = text.Append(value);
                }
            }

            /// <inheritdoc/>
            public override string ToString()
            {
                lock (text)
                {
                    return text.ToString();
                }
            }
        }

        /// <summary>
        /// The records in AOF segment 0, in order: each one's offset in the segment, and for a SET record the index of its key,
        /// formatted "key" and four digits, or -1 for a commit record
        /// </summary>
        private static List<(long Offset, int Key)> ReadRecords(string segmentFile, int pageBytes)
        {
            const int headerSize = 4;   // LogChecksumType.None => 4-byte length-only record header
            var data = File.ReadAllBytes(segmentFile);
            var keyPrefix = Encoding.ASCII.GetBytes("key");
            var records = new List<(long Offset, int Key)>();
            for (long offset = Tsavorite.core.LogAddress.FirstValidAddress; offset + headerSize <= data.Length;)
            {
                // Commit records have negative lengths. A zero length is padding to the end of the page, or at a page start,
                // the end of the written data.
                var length = BitConverter.ToInt32(data, (int)offset);
                if (length == 0)
                {
                    if (offset % pageBytes == 0)
                        break;
                    offset = (offset / pageBytes + 1) * pageBytes;
                    continue;
                }

                var payload = data.AsSpan((int)offset + headerSize, Math.Abs(length));
                var key = -1;
                if (length > 0)
                {
                    var keyStart = payload.IndexOf(keyPrefix) + keyPrefix.Length;
                    key = int.Parse(Encoding.ASCII.GetString(payload.Slice(keyStart, 4)));
                }
                records.Add((offset, key));
                offset += headerSize + ((payload.Length + 3) & ~3);
            }
            return records;
        }

        /// <summary>
        /// Zero the length field of the record at <paramref name="offset"/> in <paramref name="segmentFile"/>
        /// </summary>
        private static void ZeroLength(string segmentFile, long offset)
        {
            using var fs = new FileStream(segmentFile, FileMode.Open, FileAccess.Write, FileShare.None);
            fs.Position = offset;
            fs.Write(new byte[4]);
        }

        /// <summary>
        /// Whether <paramref name="path"/> holds the ASCII bytes of <paramref name="text"/>
        /// </summary>
        private static bool FileContains(string path, string text)
            => File.ReadAllBytes(path).AsSpan().IndexOf(Encoding.ASCII.GetBytes(text)) >= 0;

        /// <summary>
        /// Whether the file at <paramref name="path"/> exists and is not empty. Clearing deletes a segment file above the tail,
        /// or empties it if the device holds it open, as <see cref="Tsavorite.core.NativeStorageDevice"/> can.
        /// </summary>
        private static bool HoldsData(string path)
            => File.Exists(path) && new FileInfo(path).Length > 0;

        /// <summary>
        /// Corrupts the AOF to look like an ungraceful shutdown mid commit-record write:
        /// (a) deletes commit-metadata files so recovery must forward-scan the in-log commit records
        ///     and deterministically reach the torn tail record. Metadata writes are throttled and
        ///     coalesced, so which commit gets a file is timing-dependent; if the tail commit had one,
        ///     its UntilAddress would already cover the torn record and recovery would start the scan
        ///     past it -- never exercising the bug. This also models throttled fast commit, where the
        ///     real torn tail commit had no metadata file.
        /// (b) truncates the final commit record in the last segment, dropping its trailing bytes
        ///     (CommitNum + cookieLength) exactly like the real incident, so its checksum fails.
        /// </summary>
        private static void SimulateTornTailWrite(string checkpointDir)
        {
            const int headerSize = 4;   // LogChecksumType.None => 4-byte length-only record header
            const int tornBytes = 12;   // drop trailing CommitNum(8) + cookieLength(4) of the record
            var aofDir = Path.Combine(checkpointDir, "AOF");

            // (a) Force the forward-scan recovery path.
            var commitDir = Path.Combine(aofDir, "log-commits");
            if (Directory.Exists(commitDir))
                foreach (var f in Directory.GetFiles(commitDir))
                    File.Delete(f);

            // Find the highest-numbered AOF segment (where the tail lives).
            var lastSeg = Directory.GetFiles(aofDir, "aof.log.*")
                .OrderBy(f => int.Parse(f[(f.LastIndexOf('.') + 1)..]))
                .Last();
            int segNum = int.Parse(lastSeg[(lastSeg.LastIndexOf('.') + 1)..]);

            // Walk records to find the last commit record (negative length header). Segment 0 starts
            // at LogAddress.FirstValidAddress; higher segments start at their file offset 0.
            byte[] data = File.ReadAllBytes(lastSeg);
            long offset = segNum == 0 ? Tsavorite.core.LogAddress.FirstValidAddress : 0;
            long lastCommitOffset = -1;
            int lastCommitPayload = 0;
            while (offset + headerSize <= data.Length)
            {
                int len = System.BitConverter.ToInt32(data, (int)offset);
                if (len == 0) break; // zero padding / end of written data
                int payload = len < 0 ? -len : len;
                long recSize = headerSize + ((payload + 3) & ~3);
                if (offset + recSize > data.Length) break;
                if (len < 0) { lastCommitOffset = offset; lastCommitPayload = payload; }
                offset += recSize;
            }

            ClassicAssert.Greater(lastCommitOffset, -1L, "expected at least one commit record in the AOF");

            long recordSize = headerSize + ((lastCommitPayload + 3) & ~3);
            long truncateTo = lastCommitOffset + recordSize - tornBytes;
            using var fs = new FileStream(lastSeg, FileMode.Open, FileAccess.Write, FileShare.None);
            fs.SetLength(truncateTo);
        }
    }
}