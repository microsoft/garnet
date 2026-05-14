// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Regression test for a race between <c>VectorManager</c>'s background
    /// cleanup-task scan iterator (over the main string keyspace) and a
    /// concurrent <c>storeWrapper.Reset()</c> (e.g. triggered by a replica
    /// re-attach). The race originally manifested as an AVE in
    /// <c>SpanByteScanIterator.GetNext</c> dereferencing a freed page.
    ///
    /// Production paths (cluster re-attach) wrap Reset with
    /// <c>VectorManager.PauseCleanupAsync</c> / <c>ResumeCleanup</c> to
    /// serialize the cleanup task (iterator + post-iterate RMWs) against
    /// allocator teardown — Reset is only safe for concurrent SCAN iteration,
    /// not for the cleanup task's RMWs on metadata records that follow the
    /// iteration. This test mirrors that production pattern: add a vector set
    /// on a single Garnet server, drop it (queues a cleanup scan), then hammer
    /// <c>Pause + Reset + Resume</c> while the cleanup task runs.
    /// </summary>
    [TestFixture]
    public class VectorCleanupVsResetRaceTests : TestBase
    {
        private global::Garnet.GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, enableVectorSetPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            try { server.Dispose(); } catch { }
            TestUtils.OnTearDown();
        }

        // The storeWrapper field is private on GarnetServer; everything below it
        // (DefaultDatabase, VectorManager, Reset(int)) is public so we access them
        // directly through the StoreWrapper reference returned here.
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(global::Garnet.GarnetServer server);

        /// <summary>
        /// Reproducer: drop a vector set (queues full-keyspace cleanup) and concurrently
        /// hammer Pause+Reset+Resume — the production pattern used by cluster re-attach
        /// (ReplicaDisklessSync / ReplicaDiskbasedSync). Without the Pause, Reset's
        /// post-Phase-2 Initialize() would race with the cleanup task's RMWs on metadata
        /// records (ClearDeleteInProgress / UpdateContextMetadata) and AVE — Reset is
        /// only safe for concurrent SCAN iteration, not for arbitrary RMW/Read/Upsert.
        ///
        /// Verifies:
        ///   * Pause+Reset+Resume against an in-flight cleanup-task iteration does not AVE.
        ///   * cleanupGate correctly serializes cleanup-iteration with Reset.
        /// </summary>
        [Test]
        [Repeat(5)]
        public void DropVectorSetWhileResettingStore()
        {
            const int Vectors = 4_000;
            const string Key = nameof(DropVectorSetWhileResettingStore);

            ref var storeWrapper = ref GetStoreWrapper(server);
            ClassicAssert.IsNotNull(storeWrapper, "Could not access storeWrapper via UnsafeAccessor");

            var vectorManager = storeWrapper.DefaultDatabase.VectorManager;
            ClassicAssert.IsNotNull(vectorManager, "VectorManager not initialised — enableVectorSetPreview must be true");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // Populate the vector set so the cleanup-task scan has lots of records to iterate.
            var elem = new byte[4];
            var data = new byte[75];
            var rand = new Random(2026_05_01);
            for (var i = 0; i < Vectors; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(elem, i);
                rand.NextBytes(data);
                _ = db.Execute("VADD", [Key, "XB8", data, elem, "XPREQ8"]);
            }

            // Drop the vector set. This calls VectorManager.CleanupDroppedIndex which writes to
            // the cleanup channel; the background task then runs Iterate over the full keyspace
            // and a series of RMWs to clear the in-progress-deletes metadata.
            _ = db.KeyDelete(Key);

            // Race window: hammer Pause+Reset+Resume while the cleanup task iterates.
            // Pause acquires VectorManager's cleanupGate; the cleanup-iteration body holds
            // that gate from the start of the iterate through the post-iterate RMWs, so
            // Pause waits for any in-flight iteration to fully finish before Reset proceeds.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            int resets = 0;
            while (DateTime.UtcNow < deadline)
            {
                vectorManager.PauseCleanupAsync().GetAwaiter().GetResult();
                try
                {
                    try
                    {
                        storeWrapper.Reset();
                        resets++;
                    }
                    catch (Exception ex)
                    {
                        // Reset itself can throw if the store is in an unexpected state — that's OK
                        // for our purposes; we care about whether the cleanup iteration AVEs.
                        TestContext.Progress.WriteLine($"[reset] threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
                finally
                {
                    vectorManager.ResumeCleanup();
                }
                Thread.Sleep(1);
            }

            TestContext.Progress.WriteLine($"[DropVectorSetWhileResettingStore] resets={resets}");
            // If we reach here the cleanup task did not AVE the host while Reset was hammering.
        }
    }
}