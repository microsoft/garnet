// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.cluster;
#if DEBUG
using Garnet.common;
#endif
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Commands a primary sends a replica to sync it that wait: an attach to a diskless sync is held for a window so
    /// that other replicas can join it, and recovering from the checkpoint the primary sent takes as long as the store
    /// is large. The session that receives one parks, so the thread that delivered the command goes back to the
    /// network instead of waiting along with it.
    /// </summary>
    /// <remarks>
    /// The session is driven directly, off the test thread: a command that holds the thread delivering it never
    /// returns to say so, and the bound on each call turns that into a failure.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicaSyncSuspensionTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(20);

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
        /// A replica that attaches to a diskless sync is held for a window so that others can join it. The session that
        /// received the attach waits out the window parked instead of holding the thread that delivered it, and the
        /// window does not outlast the node.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task TheWindowOfADisklessAttachParksTheSessionAndEndsWithTheNode()
        {
            // Far longer than the test waits for anything, so that only the shutdown below ends it
            context.CreateInstances(1, enableAOF: true, enableDisklessSync: true, replicaDisklessSyncDelay: 120, disableObjects: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var address = AofAddress.Create(1, 64);
            var attach = new SyncMetadata(fullSync: true, Garnet.cluster.NodeRole.REPLICA, new string('a', 40), new string('b', 40),
                currentStoreVersion: 0, address, address, address, checkpointEntry: null);
            var input = Frame(Ascii("CLUSTER"), Ascii("attach_sync"), attach.ToByteArray());

            var sender = new ControlledSender(1024);
            var session = NewSession(sender);
            var sessionDisposed = false;
            try
            {
                var call = Task.Run(() => Consume(session, input));
                await WithinBoundAsync(call, "the attach to return to the receive loop, which a window held on the thread that delivered it never does");
                var consume = await call;

                ClassicAssert.IsFalse(consume.IsCompleted, "The attach waits out its window");
                ClassicAssert.IsEmpty(sender.Sent, "Nothing is answered while the window is open");

                // The client going away does not end the window. The node shutting down does.
                session.Dispose();
                sessionDisposed = true;
                await WithinBoundAsync(Task.Run(() => context.ShutdownNode(0)), "the node to shut down");

                var released = consume.AsTask();
                _ = await Task.WhenAny(released, Task.Delay(Bound));
                ClassicAssert.IsTrue(released.IsCompleted, "The shutdown ends the window, which releases the session that was parked on it");

                try
                {
                    _ = await released;
                }
                catch (ObjectDisposedException)
                {
                    // A session that was torn down while it was parked is released this way
                }
            }
            finally
            {
                if (!sessionDisposed)
                    session.Dispose();
                GC.KeepAlive(input);
            }
        }

#if DEBUG
        /// <summary>
        /// Recovering a replica from the checkpoint its primary sent takes as long as the store is large. The session
        /// that received <c>begin_replica_recover</c> parks while the recovery runs elsewhere, and answers with the
        /// offset the recovery reached once it is done.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task TheRecoveryFromAReceivedCheckpointParksTheSessionThatAskedForIt()
        {
            context.CreateInstances(1, enableAOF: true, disableObjects: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var replication = ((ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider).replicationManager;

            // What a replica holds while its primary syncs it, and what the recovery ends
            ClassicAssert.IsTrue(replication.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false));

            var address = AofAddress.Create(1, 64);
            var input = Frame(
                Ascii("CLUSTER"), Ascii("begin_replica_recover"),
                Ascii("0"), Ascii("0"), Ascii(new string('c', 40)),
                new CheckpointEntry(1).ToByteArray(), address.Span.ToArray(), address.Span.ToArray());

            var sender = new ControlledSender(1024);
            var session = NewSession(sender);

            // Holds the recovery where it starts, which stands in for the stretches of it that do not yield
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Pause_Replica_Recovery);
            try
            {
                var call = Task.Run(() => Consume(session, input));
                await WithinBoundAsync(call, "the command to return to the receive loop, which a recovery run on the thread that delivered it never does");
                var consume = await call;

                ClassicAssert.IsFalse(consume.IsCompleted, "The command waits for the recovery");
                ClassicAssert.IsEmpty(sender.Sent, "Nothing is answered until the recovery is done");

                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Pause_Replica_Recovery);
                var finished = consume.AsTask();
                await WithinBoundAsync(finished, "the recovery to finish once it is let go");

                ClassicAssert.AreEqual(input.Length, await finished);
                var offset = address.ToString();
                ClassicAssert.AreEqual($"${offset.Length}\r\n{offset}\r\n", sender.Sent, "The recovery answers with the offset it reached");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Pause_Replica_Recovery);
                session.Dispose();
                GC.KeepAlive(input);
                if (replication.IsRecovering)
                    replication.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }
        }
#endif

        RespServerSession NewSession(ControlledSender sender)
            => (RespServerSession)context.nodes[0].Provider.GetSession(WireFormat.ASCII, sender);

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        static async Task WithinBoundAsync(Task task, string what)
        {
            try
            {
                await task.WaitAsync(Bound);
            }
            catch (TimeoutException)
            {
                Assert.Fail($"Timed out after {Bound.TotalSeconds:0}s waiting for {what}.");
            }
        }

        static byte[] Ascii(string text) => Encoding.ASCII.GetBytes(text);

        // A RESP array of bulk strings, in memory that stays put for as long as the session reads from it
        static byte[] Frame(params byte[][] arguments)
        {
            using var stream = new MemoryStream();
            stream.Write(Ascii($"*{arguments.Length}\r\n"));
            foreach (var argument in arguments)
            {
                stream.Write(Ascii($"${argument.Length}\r\n"));
                stream.Write(argument);
                stream.Write(Ascii("\r\n"));
            }

            var frame = GC.AllocateArray<byte>((int)stream.Length, pinned: true);
            stream.GetBuffer().AsSpan(0, frame.Length).CopyTo(frame);
            return frame;
        }
    }
}