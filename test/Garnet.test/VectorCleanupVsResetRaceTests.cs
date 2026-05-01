// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Reflection;
using System.Threading;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Regression test for a race between <c>VectorManager</c>'s background
    /// cleanup-task scan iterator (over the main string keyspace) and a
    /// concurrent <c>storeWrapper.Reset()</c> (e.g. triggered by a replica
    /// re-attach). The race manifested as an AVE in
    /// <c>SpanByteScanIterator.GetNext</c> dereferencing a freed page.
    ///
    /// The test reproduces the path with no cluster overhead: add a vector
    /// set on a single Garnet server, drop it (queues a cleanup scan), then
    /// hammer <c>storeWrapper.Reset()</c> while the scan runs.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class VectorCleanupVsResetRaceTests : AllureTestBase
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

        // Reflection helpers — the storeWrapper field is internal/private on GarnetServer.
        private static object GetStoreWrapper(global::Garnet.GarnetServer s)
        {
            var field = typeof(global::Garnet.GarnetServer).GetField("storeWrapper", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            return field?.GetValue(s);
        }

        private static void CallReset(object storeWrapper)
        {
            var method = storeWrapper.GetType().GetMethod("Reset", BindingFlags.Instance | BindingFlags.Public, null, [typeof(int)], null);
            ClassicAssert.IsNotNull(method, "Reset(int) method not found on storeWrapper via reflection — signature may have changed; update CallReset()");
            method.Invoke(storeWrapper, [0]);
        }

        /// <summary>
        /// Reproducer: drop a vector set (queues full-keyspace cleanup) and concurrently
        /// hammer storeWrapper.Reset() until the cleanup task either completes or the
        /// process AVE's.
        /// </summary>
        [Test]
        [Repeat(5)]
        public void DropVectorSetWhileResettingStore()
        {
            const int Vectors = 4_000;
            const string Key = nameof(DropVectorSetWhileResettingStore);

            var storeWrapper = GetStoreWrapper(server);
            ClassicAssert.IsNotNull(storeWrapper, "Could not access storeWrapper via reflection");

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
            // the cleanup channel; the background task then runs Iterate over the full keyspace.
            _ = db.KeyDelete(Key);

            // Race window: hammer storeWrapper.Reset() while the cleanup task iterates.
            // If a Reset can free a page that the iterator subsequently dereferences,
            // the test host AVEs.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            int resets = 0;
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    CallReset(storeWrapper);
                    resets++;
                }
                catch (TargetInvocationException tie) when (tie.InnerException is not null)
                {
                    // Reset itself can throw if the store is in an unexpected state — that's OK
                    // for our purposes; we care about whether the cleanup iteration AVEs.
                    TestContext.Progress.WriteLine($"[reset] threw: {tie.InnerException.GetType().Name}: {tie.InnerException.Message}");
                }
                Thread.Sleep(1);
            }

            TestContext.Progress.WriteLine($"[DropVectorSetWhileResettingStore] resets={resets}");
            // If we reach here the cleanup iterator did not AVE the host.
        }
    }
}