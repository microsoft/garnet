// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
#if DEBUG
using System.Threading.Tasks;
using Garnet.common;
#endif
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A node migrating a slot serves a key of it only while the key is still there, which it learns by looking the key
    /// up. Once the store has evicted records to its storage tier, that lookup may read the device, so the command is
    /// verified on an executor thread instead of the thread that received it.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterMigratingKeyOffloadTests : TestBase
    {
        const string Present = "{t}present";
        const string Absent = "{t}absent";
        const string Value = "value";

        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        /// <summary>
        /// Commands on a key of a slot being migrated are moved to an executor thread when the key may have to be looked
        /// up on the device, and only then. Either way, a key still here is served and a key that is not is redirected.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void AKeyBeingMigratedIsLookedUpOnAnExecutorThreadOnlyWhenItMayBeOnTheDevice([Values] bool evicted)
        {
            var source = SetUpMigratingSlot();
            var store = context.nodes[source].Provider.StoreWrapper.store;
            if (evicted)
            {
                store.Log.FlushAndEvict(wait: true);
                ClassicAssert.Greater(store.Log.HeadAddress, store.Log.BeginAddress, "Nothing was evicted.");
            }
            else
            {
                ClassicAssert.AreEqual(store.Log.BeginAddress, store.Log.HeadAddress, "Records were evicted.");
            }

            using var client = new RawRespSession(endpoint: context.clusterTestUtils.GetEndPoint(source));
            var executor = context.nodes[source].Provider.StoreWrapper.offloadExecutor;
            var posts = executor.Posts;

            ClassicAssert.AreEqual($"${Value.Length}\r\n{Value}\r\n", client.Execute("GET", Present));
            StringAssert.StartsWith("-ASK ", client.Execute("GET", Absent));

            if (evicted)
                ClassicAssert.AreEqual(posts + 2, executor.Posts, "A key being migrated was looked up on the thread that received the command.");
            else
                ClassicAssert.AreEqual(posts, executor.Posts, "A key being migrated that is in memory was looked up on an executor thread.");
        }

#if DEBUG
        /// <summary>
        /// A transaction, an EXEC or a custom transaction, is moved to an executor thread before it locks anything if the
        /// store has evicted records by the time it is dispatched. One dispatched before that, whose key being migrated
        /// may have been evicted by the time the transaction verifies it, is verified again on an executor thread, with
        /// nothing run, and an EXEC's commands still queued.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ATransactionThatFindsItsKeyBeingMigratedMayBeOnTheDeviceIsVerifiedAgainOnAnExecutorThread([Values] bool custom)
        {
            const ExceptionInjectionType Pause = ExceptionInjectionType.Cluster_Txn_Pause_Before_Slot_Verify;
            var bound = TimeSpan.FromSeconds(20);

            var source = SetUpMigratingSlot();
            var store = context.nodes[source].Provider.StoreWrapper.store;
            var executor = context.nodes[source].Provider.StoreWrapper.offloadExecutor;
            ClassicAssert.AreEqual(store.Log.BeginAddress, store.Log.HeadAddress, "Records were evicted, so the transaction is moved when it is dispatched.");
            if (custom)
                _ = context.nodes[source].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);

            using var client = new RawRespSession(endpoint: context.clusterTestUtils.GetEndPoint(source));
            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                var posts = executor.Posts;
                if (custom)
                    client.Send(BulkRead.Name, "1", Present);
                else
                    client.SendPipeline(["MULTI"], ["GET", Present], ["EXEC"]);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(bound);
                ClassicAssert.AreEqual(posts, executor.Posts, "The transaction was moved when it was dispatched.");

                store.Log.FlushAndEvict(wait: true);
                ClassicAssert.Greater(store.Log.HeadAddress, store.Log.BeginAddress, "Nothing was evicted.");

                // Let the transaction verify its key; it arrives here again on an executor thread
                ExceptionInjectionHelper.EnableException(Pause);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(bound);
                ClassicAssert.AreEqual(posts + 1, executor.Posts, "The transaction did not move to an executor thread to verify its key again.");

                ExceptionInjectionHelper.EnableException(Pause);
                if (!custom)
                {
                    ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
                    ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
                }
                ClassicAssert.AreEqual($"*1\r\n${Value.Length}\r\n{Value}\r\n", client.ReadReply());
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
            }
        }
#endif

        /// <summary>
        /// Sets up two primaries, stores <see cref="Present"/> on the one that owns its slot, and starts migrating the slot
        /// to the other. Returns the index of the node migrating the slot.
        /// </summary>
        int SetUpMigratingSlot()
        {
            context.CreateInstances(2, disableObjects: true, clusterConfigFlushFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(2, 0, logger: context.logger);

            var slot = context.clusterTestUtils.HashSlot(Present);
            var source = context.clusterTestUtils.GetSourceNodeIndexFromSlot((ushort)slot, context.logger);
            var target = 1 - source;

            using (var client = new RawRespSession(endpoint: context.clusterTestUtils.GetEndPoint(source)))
                ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", Present, Value));

            var targetId = context.clusterTestUtils.GetNodeIdFromNode(target, context.logger);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.SetSlot(source, slot, "MIGRATING", targetId, context.logger));
            return source;
        }
    }
}