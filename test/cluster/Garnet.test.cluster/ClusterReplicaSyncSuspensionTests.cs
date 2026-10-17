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

        /// <summary>
        /// Making a node a primary first stops its replica-only background tasks, which takes as long as the slowest of
        /// them takes to observe its cancellation. The session that received <c>REPLICAOF NO ONE</c> waits for them
        /// parked.
        /// </summary>
        /// <remarks>
        /// The configuration is never written to its file, so the command is not moved to an executor thread and runs
        /// on the thread that delivered it.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public async Task ReplicaOfNoOneWaitsParkedForTheReplicaTasksToStop()
        {
            context.CreateInstances(1, enableAOF: true, disableObjects: true, clusterConfigFlushFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            // A replica-only task that does not stop until it is let go
            var taskManager = context.nodes[0].Provider.StoreWrapper.TaskManager;
            await taskManager.CancelAsync(TaskType.VectorReplicationReplayTask);
            var stop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ClassicAssert.IsTrue(taskManager.RegisterAndRun(TaskType.VectorReplicationReplayTask, _ => stop.Task));

            var input = Frame(Ascii("REPLICAOF"), Ascii("NO"), Ascii("ONE"));
            var sender = new ControlledSender(1024);
            var session = NewSession(sender);
            try
            {
                var call = Task.Run(() => Consume(session, input));
                await WithinBoundAsync(call, "the command to return to the receive loop, which a wait for the replica tasks on the thread that delivered it never does");
                var consume = await call;

                ClassicAssert.IsFalse(consume.IsCompleted, "The command waits for the replica tasks to stop");
                ClassicAssert.IsEmpty(sender.Sent, "Nothing is answered until they have");

                stop.SetResult();
                var finished = consume.AsTask();
                await WithinBoundAsync(finished, "the command to finish once the task stops");

                ClassicAssert.AreEqual(input.Length, await finished);
                ClassicAssert.AreEqual("+OK\r\n", sender.Sent);
            }
            finally
            {
                _ = stop.TrySetResult();
                session.Dispose();
                GC.KeepAlive(input);
            }
        }

        /// <summary>
        /// A replica that replays its primary's stream synchronously applies each chunk on the thread that delivered it,
        /// where a record waits for the Vector Set replays queued before it, which the streams of other sublogs can queue
        /// at any time. With Vector Sets enabled the chunks run on an executor thread; without them, and with nothing to
        /// read from the device, they run in place.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ASynchronouslyReplayedStreamRunsOnAnExecutorThreadOnlyWhenVectorSetsMayHoldItUp([Values] bool vectorSets)
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, clusterConfigFlushFrequencyMs: -1, enableVectorSetPreview: vectorSets);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);

            var executor = context.nodes[1].Provider.StoreWrapper.offloadExecutor;
            var posts = executor.Posts;

            context.kvPairs = [];
            context.SimplePopulateDB(disableObjects: true, keyLength: 16, kvpairCount: 64, primaryIndex: 0);

            // Waits for the replica by reading from it, which runs in place, unlike the INFO that offsets are read with
            foreach (var (key, value) in context.kvPairs)
            {
                var keyBytes = Encoding.ASCII.GetBytes(key);
                string read;
                while ((read = context.clusterTestUtils.GetKey(1, keyBytes, out _, out _, out var state, logger: context.logger)) is null
                    || state != ResponseState.OK || int.Parse(read) != value)
                {
                    ClusterTestUtils.BackOff(cancellationToken: context.cts.Token, msg: $"waiting for {key} on the replica");
                }
            }

            if (vectorSets)
                ClassicAssert.Greater(executor.Posts, posts, "The replica applied its stream in place, where a record waits for queued Vector Set replays.");
            else
                ClassicAssert.AreEqual(posts, executor.Posts, "The replica moved a stream that waits on nothing to an executor thread.");
        }

        /// <summary>
        /// A replica that truncates its AOF fast resets a sublog, on the device, before it enqueues a chunk the primary
        /// shipped from an address it skipped ahead to. Such a chunk runs on an executor thread; a chunk that follows on
        /// from the previous one runs in place.
        /// </summary>
        /// <remarks>
        /// Where a chunk runs is decided before the command is run, so the node need not be a replica to show it. Being
        /// none, it fails the command and closes the session either way.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public async Task AChunkThatSkipsAheadRunsOnAnExecutorThreadWhenTheAofIsTruncatedFast()
        {
            context.CreateInstances(1, enableAOF: true, FastAofTruncate: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, asyncReplay: true,
                disableObjects: true, clusterConfigFlushFrequencyMs: -1, enableVectorSetPreview: false);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var executor = context.nodes[0].Provider.StoreWrapper.offloadExecutor;
            var record = new byte[64];

            var posts = executor.Posts;
            await AppendAsync(previousAddress: 64, currentAddress: 64);
            ClassicAssert.AreEqual(posts, executor.Posts, "A chunk that follows on from the previous one was moved to an executor thread.");

            await AppendAsync(previousAddress: 64, currentAddress: 64 + record.Length + 8);
            ClassicAssert.AreEqual(posts + 1, executor.Posts, "A chunk that skips ahead did not run on an executor thread.");

            async Task AppendAsync(long previousAddress, long currentAddress)
            {
                var input = Frame(Ascii("CLUSTER"), Ascii("appendlog"), Ascii(new string('a', 40)), Ascii("0"),
                    Ascii(previousAddress.ToString()), Ascii(currentAddress.ToString()), Ascii((currentAddress + record.Length).ToString()), record);
                var session = NewSession(new ControlledSender(1024));
                try
                {
                    var consume = Task.Run(() => Consume(session, input).AsTask());
                    await WithinBoundAsync(consume, "the chunk to be run");
                }
                finally
                {
                    session.Dispose();
                    GC.KeepAlive(input);
                }
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

        /// <summary>
        /// A primary ends a diskless sync by attaching to the replica it synced, which then resets its AOF, records the
        /// primary's history and waits for its queued Vector Set replays before it answers. The session that received
        /// the attach parks while that runs elsewhere, and answers with the offset the replica resumes from.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task TheRecoveryThatEndsADisklessSyncParksTheSessionThatAskedForIt()
        {
            const ExceptionInjectionType pause = ExceptionInjectionType.Replication_Pause_Before_Diskless_Snapshot_Recovery;

            context.CreateInstances(1, enableAOF: true, disableObjects: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var replication = ((ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider).replicationManager;

            // What a replica holds while its primary syncs it, and what the recovery ends
            ClassicAssert.IsTrue(replication.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false));

            var address = AofAddress.Create(1, 64);
            var attach = new SyncMetadata(fullSync: true, Garnet.cluster.NodeRole.PRIMARY, new string('a', 40), new string('b', 40),
                currentStoreVersion: 0, address, address, address, checkpointEntry: null);
            var input = Frame(Ascii("CLUSTER"), Ascii("attach_sync"), attach.ToByteArray());

            var sender = new ControlledSender(1024);
            var session = NewSession(sender);

            // Holds the recovery where it starts, which stands in for the stretches of it that do not yield
            ExceptionInjectionHelper.EnableException(pause);
            try
            {
                var call = Task.Run(() => Consume(session, input));
                await WithinBoundAsync(call, "the attach to return to the receive loop, which a recovery run on the thread that delivered it never does");
                var consume = await call;

                await WithinBoundAsync(ExceptionInjectionHelper.WaitOnClearAsync(pause), "the recovery to start");
                ClassicAssert.IsFalse(consume.IsCompleted, "The attach waits for the recovery");
                ClassicAssert.IsEmpty(sender.Sent, "Nothing is answered until the recovery is done");

                // The recovery cleared the injection on arrival; setting it again lets the recovery go
                ExceptionInjectionHelper.EnableException(pause);
                var finished = consume.AsTask();
                await WithinBoundAsync(finished, "the recovery to finish once it is let go");

                ClassicAssert.AreEqual(input.Length, await finished);
                var offset = address.ToString();
                ClassicAssert.AreEqual($"${offset.Length}\r\n{offset}\r\n", sender.Sent, "The recovery answers with the offset the replica resumes from");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(pause);
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