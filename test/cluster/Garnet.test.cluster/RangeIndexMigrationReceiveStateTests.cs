// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
// The receive-side ProcessRecord takes a ref StringBasicContext; declare the alias locally so the
// test can pass a default (it is only dereferenced on stream completion, which these tests avoid).
using StringBasicContext = Tsavorite.core.BasicContext<
    Garnet.common.FixedSpanByteKey,
    Garnet.server.StringInput,
    Garnet.server.StringOutput,
    long, Garnet.server.MainSessionFunctions,
    Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
    Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>>;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Component tests for <see cref="RangeIndexMigrationReceiveState"/> focused on the
    /// dispose-vs-in-flight-<c>ProcessRecord</c> race that <see cref="Garnet.common.CooperativeDisposeGuard"/>
    /// guards. A receiving node may dispose its <c>ClusterSession</c> (on one networking thread)
    /// while a migration chunk is still being processed (on another, e.g. the FastMigrate
    /// <c>Task.Run</c> path) — the disposal must never run concurrently with, nor abort, an
    /// in-flight <c>ProcessRecord</c>; cleanup is deferred to the worker's exit.
    /// </summary>
    [TestFixture, NonParallelizable]
    internal class RangeIndexMigrationReceiveStateTests
    {
        string testDir;

        [SetUp]
        public void Setup()
        {
            testDir = Path.Combine(Path.GetTempPath(), "ri-recv-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(testDir);
        }

        [TearDown]
        public void TearDown()
        {
            try { if (Directory.Exists(testDir)) Directory.Delete(testDir, recursive: true); } catch { }
        }

        /// <summary>
        /// Builds a partial RI migration stream that drives the deserializer into the
        /// ReceivingFileData state (which opens a temp snapshot file) but never completes:
        /// <c>[4B keyLen][key][8B fileSize][partial file bytes]</c> with fileSize ≫ bytes sent.
        /// </summary>
        private static byte[] BuildPartialChunk()
        {
            var key = Encoding.UTF8.GetBytes("rikey");
            var chunk = new byte[sizeof(int) + key.Length + sizeof(long) + 10];
            var o = 0;
            BinaryPrimitives.WriteInt32LittleEndian(chunk.AsSpan(o), key.Length);
            o += sizeof(int);
            key.CopyTo(chunk.AsSpan(o));
            o += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(chunk.AsSpan(o), 1000); // far more than the 10 file bytes provided
            // The trailing 10 bytes are file data (already zero-initialized).
            return chunk;
        }

        private int CountTempFiles()
        {
            var dir = Path.Combine(testDir, "migration-tmp");
            return Directory.Exists(dir) ? Directory.GetFiles(dir, "*.bftree").Length : 0;
        }

        [Test]
        [Category("CLUSTER")]
        public void ProcessRecordAfterDispose_Throws()
        {
            var manager = new RangeIndexManager(testDir);
            var state = new RangeIndexMigrationReceiveState(manager);

            state.Dispose();

            StringBasicContext ctx = default;
            ClassicAssert.IsFalse(state.IsReceiving);
            Assert.Throws<ObjectDisposedException>(() => state.ProcessRecord(BuildPartialChunk(), null, ref ctx, replaceOption: false));
        }

        [Test]
        [Category("CLUSTER")]
        public void DisposeDuringProcessRecord_DefersCleanupToWorker()
        {
            ClusterTestUtils.IgnoreIfExceptionInjectionDisabled();

            var manager = new RangeIndexManager(testDir);
            var state = new RangeIndexMigrationReceiveState(manager);
            var chunk = BuildPartialChunk();

            // Arm the pause: ProcessRecord parks (with the dispose guard held) after processing the chunk.
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_Receive_Pause_In_ProcessRecord);

            Exception workerEx = null;
            var worker = new Thread(() =>
            {
                StringBasicContext ctx = default;
                try { _ = state.ProcessRecord(chunk, null, ref ctx, replaceOption: false); }
                catch (Exception e) { workerEx = e; }
            });

            try
            {
                worker.Start();

                // Wait until the worker has created the deserializer, processed the chunk (which
                // opens the temp snapshot file), and parked at the injection point. We wait on the
                // temp file rather than CurrentChunkCount: the chunk count is bumped (OnChunkReceived)
                // just BEFORE ProcessChunk creates the file, so keying off the count races that gap.
                // ProcessChunk runs immediately before the pause, so the file's appearance proves the
                // worker is parked in-flight.
                var deadline = DateTime.UtcNow.AddSeconds(10);
                while (CountTempFiles() == 0 && DateTime.UtcNow < deadline)
                    Thread.Yield();

                ClassicAssert.AreEqual(1, state.CurrentChunkCount, "worker did not reach the pause point");
                ClassicAssert.IsTrue(state.IsReceiving, "deserializer should be in-flight");
                ClassicAssert.AreEqual(1, CountTempFiles(), "temp snapshot file should exist while in-flight");

                // Settle so the worker is provably inside WaitOnClear (the statement right after the chunk).
                Thread.Sleep(100);

                // Dispose races the in-flight ProcessRecord — the guard must defer cleanup to the worker.
                state.Dispose();
            }
            finally
            {
                // Release the pause so the worker runs its finally (the deferred cleanup path).
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_Receive_Pause_In_ProcessRecord);
            }

            ClassicAssert.IsTrue(worker.Join(TimeSpan.FromSeconds(10)), "worker did not finish");
            ClassicAssert.IsNull(workerEx, $"ProcessRecord must not throw on a deferred dispose; got: {workerEx}");

            // The worker's finally observed the deferred disposal and performed cleanup exactly once.
            ClassicAssert.IsFalse(state.IsReceiving, "Reset should have cleared the deserializer");
            ClassicAssert.AreEqual(0, CountTempFiles(), "deferred cleanup should have deleted the temp snapshot file");

            // After disposal, any further ProcessRecord must throw.
            StringBasicContext ctx2 = default;
            Assert.Throws<ObjectDisposedException>(() => state.ProcessRecord(chunk, null, ref ctx2, replaceOption: false));
        }
    }
}