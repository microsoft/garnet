// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for the AOF-replay reassembly bookkeeping in <see cref="RangeIndexManager"/>
    /// </summary>
    [TestFixture]
    public class RangeIndexStreamReplayTests : TestBase
    {
        private string testDir;

        [SetUp]
        public void Setup()
        {
            testDir = Path.Combine(TestUtils.MethodTestDir, "ri-stream-replay-test");
            if (Directory.Exists(testDir))
                Directory.Delete(testDir, recursive: true);
            Directory.CreateDirectory(testDir);
        }

        [TearDown]
        public void TearDown()
        {
            if (Directory.Exists(testDir))
                Directory.Delete(testDir, recursive: true);
            TestUtils.OnTearDown();
        }

        private static byte[] MakeStub()
        {
            var stub = new byte[RangeIndexManager.IndexSizeBytes];
            for (var i = 0; i < stub.Length; i++)
                stub[i] = (byte)(0xC0 + i);
            return stub;
        }

        private static byte[] RandomBytes(int n)
        {
            var b = new byte[n];
            new Random(1234).NextBytes(b);
            return b;
        }

        /// <summary>Frame a full stream into (chunk, isFirst, isLast) tuples via the production
        /// <see cref="RangeIndexMigrationReader"/> (over an in-memory stream), mirroring ReplicateRangeIndexStream.</summary>
        private static List<(byte[] Chunk, bool IsFirst, bool IsLast)> BuildStreamChunks(byte[] key, byte[] stub, byte[] fileData, int chunkSize)
        {
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);
            using var reader = new RangeIndexMigrationReader(serializer, new MemoryStream(fileData), tempFilePath: null);
            var result = new List<(byte[], bool, bool)>();
            var dest = new byte[chunkSize];
            var isFirst = true;

            while (!reader.IsComplete)
            {
                var written = reader.ReadNextChunk(dest);
                result.Add((dest.AsSpan(0, written).ToArray(), isFirst, reader.IsComplete));
                isFirst = false;
            }

            return result;
        }

        /// <summary>Assert exactly one reassembly-activity entry was logged with the given reason, and return it.</summary>
        private static CapturingLogger.Entry AssertSingleReassemblyReason(CapturingLogger capture, string reason)
        {
            var matches = capture.EntriesWithField("reason", reason);
            ClassicAssert.AreEqual(1, matches.Count, $"expected exactly one reassembly-activity log with reason={reason}");
            return matches[0];
        }

        [Test]
        public void PartialStreamIsPendingThenCleanedUp()
        {
            var capture = new CapturingLogger();
            using var mgr = new RangeIndexManager(testDir, logger: capture);
            var key = Encoding.UTF8.GetBytes("k1");
            var chunks = BuildStreamChunks(key, MakeStub(), RandomBytes(8192), chunkSize: 512);
            ClassicAssert.Greater(chunks.Count, 2, "test needs a multi-chunk stream");

            // Feed only the first chunk: reassembly is now in progress.
            mgr.ProcessStreamChunk(session: null, key, chunks[0].Chunk, chunks[0].IsFirst, chunks[0].IsLast);
            ClassicAssert.AreEqual(1, mgr.PendingStreamReassemblyCount);

            // End-of-replay cleanup drops the incomplete reassembly (no leak).
            mgr.DisposeIncompleteStreamReassembly();
            ClassicAssert.AreEqual(0, mgr.PendingStreamReassemblyCount);

            // The activity confirms WHY the reassembly ended and that it had received exactly the one chunk.
            var end = AssertSingleReassemblyReason(capture, "CleanupIncomplete");
            ClassicAssert.AreEqual("1", end.Field("chunkCount"));
        }

        [Test]
        public void RetryFirstChunkResetsStalePartialReassembly()
        {
            var capture = new CapturingLogger();
            using var mgr = new RangeIndexManager(testDir, logger: capture);
            var key = Encoding.UTF8.GetBytes("k1");
            var chunks = BuildStreamChunks(key, MakeStub(), RandomBytes(8192), chunkSize: 512);
            ClassicAssert.Greater(chunks.Count, 3, "test needs a multi-chunk stream");

            // Attempt 1 fails after the first chunk, leaving a partial reassembly for this key.
            mgr.ProcessStreamChunk(session: null, key, chunks[0].Chunk, isFirst: true, isLast: false);
            ClassicAssert.AreEqual(1, mgr.PendingStreamReassemblyCount);

            // Attempt 2 (retry) replays the same stream. Its first chunk (isFirst) must reset the stale
            // partial so the retry's chunks reassemble cleanly. Feed every chunk except the trailer so
            // we stay incomplete (avoids the publish path). If the reset did NOT happen, the retry's
            // header bytes would be fed into the stale mid-file deserializer and corrupt it (checksum
            // failure -> the reassembly would be dropped, leaving count 0).
            for (var i = 0; i < chunks.Count - 1; i++)
                mgr.ProcessStreamChunk(session: null, key, chunks[i].Chunk, isFirst: i == 0, isLast: false);

            ClassicAssert.AreEqual(1, mgr.PendingStreamReassemblyCount, "retry stream should reassemble cleanly after the first chunk reset the stale partial");

            // The reset is directly observable: the retry's first chunk superseded attempt 1's partial,
            // which had received exactly one chunk.
            var superseded = AssertSingleReassemblyReason(capture, "NewStreamReceived");
            ClassicAssert.AreEqual("1", superseded.Field("chunkCount"));

            mgr.DisposeIncompleteStreamReassembly();
            ClassicAssert.AreEqual(0, mgr.PendingStreamReassemblyCount);

            // The clean retry reassembly received every chunk except the withheld trailer.
            var end = AssertSingleReassemblyReason(capture, "CleanupIncomplete");
            ClassicAssert.AreEqual((chunks.Count - 1).ToString(), end.Field("chunkCount"));
        }

        [Test]
        public void MalformedChunkIsDropped()
        {
            var capture = new CapturingLogger();
            using var mgr = new RangeIndexManager(testDir, logger: capture);
            var key = Encoding.UTF8.GetBytes("k1");

            // A first chunk that begins with a non-positive key length is rejected by the deserializer.
            // Replay must throw after removing the failed reassembly (not left pending).
            var malformed = new byte[16]; // leading 4 bytes = 0 => invalid key length
            ClassicAssert.Throws<GarnetException>(() => mgr.ProcessStreamChunk(session: null, key, malformed, isFirst: true, isLast: false));
            ClassicAssert.AreEqual(0, mgr.PendingStreamReassemblyCount);

            var end = AssertSingleReassemblyReason(capture, "ChunkProcessingError");
            ClassicAssert.AreEqual("1", end.Field("chunkCount"));
        }

        [Test]
        public void FinalFlagOnIncompleteStreamDropsReassembly()
        {
            var capture = new CapturingLogger();
            using var mgr = new RangeIndexManager(testDir, logger: capture);
            var key = Encoding.UTF8.GetBytes("k1");
            var chunks = BuildStreamChunks(key, MakeStub(), RandomBytes(8192), chunkSize: 512);
            ClassicAssert.Greater(chunks.Count, 2, "test needs a multi-chunk stream");

            // Feed the first chunk flagged as the final one while the stream is still incomplete:
            // this is a malformed/truncated stream and must throw after being dropped.
            ClassicAssert.Throws<GarnetException>(() => mgr.ProcessStreamChunk(session: null, key, chunks[0].Chunk, isFirst: true, isLast: true));
            ClassicAssert.AreEqual(0, mgr.PendingStreamReassemblyCount);

            // The activity pins the exact drop reason: a final flag on a still-incomplete stream.
            var end = AssertSingleReassemblyReason(capture, "FinalChunkButDeserializerIncomplete");
            ClassicAssert.AreEqual("1", end.Field("chunkCount"));
        }

        [Test]
        public void PublishFailureOnCompleteStreamThrows()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var capture = new CapturingLogger();
            using var mgr = new RangeIndexManager(testDir, logger: capture);
            var key = Encoding.UTF8.GetBytes("k1");
            var chunks = BuildStreamChunks(key, MakeStub(), RandomBytes(8192), chunkSize: 512);
            ClassicAssert.Greater(chunks.Count, 2, "test needs a multi-chunk stream");

            // Feed every chunk except the trailer: the stream is fully buffered but not yet complete.
            for (var i = 0; i < chunks.Count - 1; i++)
                mgr.ProcessStreamChunk(session: null, key, chunks[i].Chunk, chunks[i].IsFirst, chunks[i].IsLast);

            ClassicAssert.AreEqual(1, mgr.PendingStreamReassemblyCount);

            // Force PublishMigratedIndex to be treated as failed so the final chunk completes the stream
            // but the publish is rejected. Replay must throw and drop the reassembly.
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Replay_Force_Publish_Failure);
            try
            {
                var last = chunks[^1];
                ClassicAssert.Throws<GarnetException>(() => mgr.ProcessStreamChunk(session: null, key, last.Chunk, last.IsFirst, last.IsLast));
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Replay_Force_Publish_Failure);
            }

            ClassicAssert.AreEqual(0, mgr.PendingStreamReassemblyCount);

            // The activity distinguishes a rejected publish from a clean completion.
            var end = AssertSingleReassemblyReason(capture, "PublishFailed");
            ClassicAssert.AreEqual(chunks.Count.ToString(), end.Field("chunkCount"));
        }
    }
}