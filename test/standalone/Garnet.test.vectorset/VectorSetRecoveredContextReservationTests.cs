// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// A Vector Set's index record is written before the context metadata that reserves its context, and
    /// the metadata is only flushed once index creation succeeds. A checkpoint taken between the two
    /// captures a live index record whose context is not marked in use, so recovery has to restore that
    /// reservation from the index records themselves - otherwise the free-list hands the same context to
    /// the next Vector Set created and the two share a namespace.
    /// </summary>
    [TestFixture]
    public class VectorSetRecoveredContextReservationTests : TestBase
    {
        private global::Garnet.GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        private void StartServer(bool tryRecover)
        {
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                memorySize: "8m",
                pageSize: "16k",
                enableAOF: true,
                aofMemorySize: "2g",
                tryRecover: tryRecover,
                enableVectorSetPreview: true);
            server.Start();
        }

        [Test]
        public async Task RecoveredVectorSetDoesNotShareContextWithNewVectorSetAsync()
        {
            const string Recovered = "recovered-vs";
            const string Fresh = "fresh-vs";
            const int Elements = 500;
            const int Dim = 32;

            StartServer(tryRecover: false);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                for (var i = 0; i < Elements; i++)
                {
                    ClassicAssert.AreEqual(1, (int)db.Execute("VADD", BuildVaddArgs(Recovered, Dim, i, $"recovered_{i}")));
                }

#pragma warning disable CS0618 // ForegroundSave is obsolete but is what the recovery tests use
                redis.GetServers().Single().Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                var committed = await server.Store.WaitForCommitAsync();
                ClassicAssert.IsTrue(committed, "checkpoint commit did not complete");
            }

            server.Dispose(deleteDir: false);
            StartServer(tryRecover: true);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                ClassicAssert.AreEqual(Elements, (int)db.Execute("VCARD", [Recovered]),
                    "the recovered Vector Set lost elements during recovery");

                // Creating a Vector Set now must not be handed the recovered set's context. If it is, the two
                // share a namespace and writes to one are visible in - and destroy - the other.
                for (var i = 0; i < Elements; i++)
                {
                    ClassicAssert.AreEqual(1, (int)db.Execute("VADD", BuildVaddArgs(Fresh, Dim, i + Elements, $"fresh_{i}")));
                }

                ClassicAssert.AreEqual(Elements, (int)db.Execute("VCARD", [Fresh]),
                    "the newly created Vector Set did not receive all of its elements");

                ClassicAssert.AreEqual(Elements, (int)db.Execute("VCARD", [Recovered]),
                    "creating a new Vector Set after recovery changed the recovered Vector Set's cardinality, " +
                    "so the two were handed the same context");

                var simArgs = new List<object> { Recovered, "VALUES", $"{Dim}" };
                for (var d = 0; d < Dim; d++)
                {
                    simArgs.Add($"{((0 % 7) + 1) * (d + 1) % 13}");
                }
                simArgs.Add("COUNT");
                simArgs.Add($"{Elements}");

                var recoveredMembers = ((RedisResult[])db.Execute("VSIM", simArgs.ToArray()))
                    .Select(static r => (string)r)
                    .ToArray();

                CollectionAssert.IsNotEmpty(recoveredMembers);
                CollectionAssert.IsSubsetOf(
                    recoveredMembers,
                    Enumerable.Range(0, Elements).Select(static i => $"recovered_{i}").ToArray(),
                    "the recovered Vector Set returned elements belonging to the Vector Set created after recovery");
            }
        }

        /// <summary>
        /// A cluster node with AOF enabled reconciles twice while starting up, once from
        /// RecoverCheckpointAndAOFAsync and again from StoreWrapper, and the first pass consumes the
        /// recovered metadata. The second pass therefore rebuilds the reservations from the index records
        /// alone, and a context that is in use without an index record - a Vector Set that was deleted but
        /// whose data has not finished being cleaned up - must still be marked for cleanup rather than
        /// silently dropped, or it is handed to the next Vector Set while its old data is still present.
        /// </summary>
        [Test]
        public async Task ReconcileWithoutRecoveredMetadataStillMarksUnbackedContextsAsync()
        {
            const string Live = "live-vs";
            const int Elements = 50;
            const int Dim = 32;
            const int UnbackedContexts = 4;

            StartServer(tryRecover: false);

            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

            // Reservations with no index record of their own, which is what a Vector Set whose data is still
            // being cleaned up looks like
            vectorManager.AllocateTestContexts(UnbackedContexts);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                for (var i = 0; i < Elements; i++)
                {
                    ClassicAssert.AreEqual(1, (int)db.Execute("VADD", BuildVaddArgs(Live, Dim, i, $"element_{i}")));
                }
            }

            var unbacked = new List<ulong>();
            for (var context = VectorManager.ContextStep; context <= UnbackedContexts * VectorManager.ContextStep; context += VectorManager.ContextStep)
            {
                vectorManager.GetContextState(context, out var isInUse, out _, out _);
                if (isInUse)
                {
                    unbacked.Add(context);
                }
            }

            ClassicAssert.AreEqual(UnbackedContexts, unbacked.Count, "expected every allocated context to be reserved");

            // Hold cleanup so the reconcile's decision stays observable instead of being processed away
            await vectorManager.PauseCleanupAsync();
            try
            {
                // recoveredMetadata is empty here, exactly as it is on the second reconcile of a startup
                vectorManager.ReconcileRecoveredState();

                foreach (var context in unbacked)
                {
                    vectorManager.GetContextState(context, out var isInUse, out var isCleaningUp, out _);

                    ClassicAssert.IsTrue(
                        isInUse && isCleaningUp,
                        $"context {context} was dropped rather than marked for cleanup, so it can be reused while its data is still present");
                }
            }
            finally
            {
                vectorManager.ResumeCleanup();
            }
        }

        private static object[] BuildVaddArgs(string key, int dim, int seed, string element)
        {
            var args = new List<object> { key, "VALUES", $"{dim}" };
            for (var d = 0; d < dim; d++)
            {
                args.Add($"{((seed % 7) + 1) * (d + 1) % 13}");
            }
            args.Add(element);

            return args.ToArray();
        }
    }
}