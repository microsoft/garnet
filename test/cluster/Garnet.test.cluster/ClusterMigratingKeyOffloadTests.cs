// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
#if DEBUG
using System.Threading.Tasks;
#endif
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A node migrating a slot serves a key of it only while the key is still there, which it learns by looking the key
    /// up. Once the store has evicted records to its storage tier, that lookup may read the device, so a command that
    /// reaches the node on a socket event thread is verified on the thread pool instead.
    /// </summary>
    /// <remarks>
    /// The nodes run socket completions inline, and each client lets the server's receive for its connection be
    /// outstanding before the command under test, so that the command reaches the node on an event thread.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterMigratingKeyOffloadTests : TestBase
    {
        const string Present = "{t}present";
        const string Absent = "{t}absent";
        const string Value = "value";
        const int SettleTime = 50;

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
        /// Commands on a key of a slot being migrated leave the event thread when the key may have to be looked up on the
        /// device, and only then. Either way, a key still here is served and a key that is not is redirected.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void AKeyBeingMigratedLeavesAnEventThreadOnlyWhenItMayBeOnTheDevice([Values] bool evicted)
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
            Settle(client);
            var offloads = Offloads(source);

            ClassicAssert.AreEqual($"${Value.Length}\r\n{Value}\r\n", client.Execute("GET", Present));
            Thread.Sleep(SettleTime);
            StringAssert.StartsWith("-ASK ", client.Execute("GET", Absent));

            if (evicted)
                ClassicAssert.AreEqual(offloads + 2, Offloads(source), "A key being migrated was looked up on the event thread that received the command.");
            else
                ClassicAssert.AreEqual(offloads, Offloads(source), "A key being migrated that is in memory was looked up on the thread pool.");
        }

#if DEBUG
        /// <summary>
        /// A transaction, an EXEC or a custom transaction, whose key being migrated may have been evicted by the time the
        /// transaction verifies it, is verified again on the thread pool, with nothing run, and an EXEC's commands still
        /// queued.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ATransactionThatFindsItsKeyBeingMigratedMayBeOnTheDeviceIsVerifiedAgainOnThePool([Values] bool custom)
        {
            const ExceptionInjectionType Pause = ExceptionInjectionType.Cluster_Txn_Pause_Before_Slot_Verify;
            var bound = TimeSpan.FromSeconds(20);

            var source = SetUpMigratingSlot();
            var store = context.nodes[source].Provider.StoreWrapper.store;
            ClassicAssert.AreEqual(store.Log.BeginAddress, store.Log.HeadAddress, "Records were evicted, so the transaction is moved when it is verified first.");
            if (custom)
                _ = context.nodes[source].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);

            using var client = new RawRespSession(endpoint: context.clusterTestUtils.GetEndPoint(source));
            Settle(client);
            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                var offloads = Offloads(source);
                if (custom)
                    client.Send(BulkRead.Name, "1", Present);
                else
                    client.SendPipeline(["MULTI"], ["GET", Present], ["EXEC"]);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(bound);
                ClassicAssert.AreEqual(offloads, Offloads(source), "The transaction was moved before it verified its key.");

                store.Log.FlushAndEvict(wait: true);
                ClassicAssert.Greater(store.Log.HeadAddress, store.Log.BeginAddress, "Nothing was evicted.");

                // Let the transaction verify its key; it arrives here again on the thread pool
                ExceptionInjectionHelper.EnableException(Pause);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(bound);
                ClassicAssert.AreEqual(offloads + 1, Offloads(source), "The transaction did not move to the thread pool to verify its key again.");

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
            if (!SocketCompletions.InlineCompletionsSupported)
                Assert.Ignore("Sockets cannot run their completions inline on this platform.");

            context.CreateInstances(2, disableObjects: true, clusterConfigFlushFrequencyMs: -1,
                configureNode: (_, options) => options.NetworkInlineCompletions = true);
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

        /// <summary>Commands <paramref name="node"/> has moved to the thread pool because they would wait on a thread that must not.</summary>
        long Offloads(int node) => Interlocked.Read(ref context.nodes[node].Provider.StoreWrapper.offloads);

        /// <summary>
        /// Waits out <paramref name="client"/>'s last reply, so the server has a receive outstanding for its connection and
        /// the next request reaches the server through a socket event thread.
        /// </summary>
        static void Settle(RawRespSession client)
        {
            ClassicAssert.AreEqual("+PONG\r\n", client.Execute("PING"));
            Thread.Sleep(SettleTime);
        }
    }
}