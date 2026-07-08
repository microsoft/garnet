// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Recovery test for a "torn tail" AOF: an ungraceful shutdown that leaves the final commit
    /// record half-written. Recovery must not crash (FailOnRecoveryError is set so a bad recovery
    /// surfaces as a startup exception) and must replay every committed operation up to the last
    /// valid commit.
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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitWait: true, lowMemory: true, failOnRecoveryError: true);
            Assert.DoesNotThrow(() => server.Start(),
                "recovery must not crash on a torn-tail commit record");

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