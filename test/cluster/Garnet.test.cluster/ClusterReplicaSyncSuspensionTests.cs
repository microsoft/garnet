// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
#if DEBUG
using System.Diagnostics;
#endif
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.common;
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
        /// The configuration is never written to its file, so the command is not moved to the thread pool and runs on the
        /// thread that delivered it.
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
        /// where a record waits for the Vector Set replays queued before it. The chunk awaits those replays parked, so a
        /// chunk that reaches the replica on a socket event thread runs in place, whether or not it queues Vector Set
        /// replays and has records that wait for them, as long as there is nothing to read from the device.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ASynchronouslyReplayedStreamRunsOnTheEventThreadThatDeliveredIt([Values] bool vectorSets)
        {
            if (!SocketCompletions.InlineCompletionsSupported)
                Assert.Ignore("Sockets cannot run their completions inline on this platform.");

            context.CreateInstances(2, enableAOF: true, disableObjects: true, clusterConfigFlushFrequencyMs: -1, enableVectorSetPreview: vectorSets,
                configureNode: (i, options) => options.NetworkInlineCompletions = i == 1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);

            // Lets the replica's receive for the stream be outstanding, so the next chunk reaches it on an event thread
            Thread.Sleep(50);
            var offloads = Offloads(1);

            if (vectorSets)
            {
                // Queued on the replica ahead of the writes below, which wait for them
                using var primary = new RawRespSession(endpoint: context.endpoints[0]);
                ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "1", "2", "3", "a", "NOQUANT"));
                ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "4", "5", "6", "b", "NOQUANT"));
            }

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

            ClassicAssert.AreEqual(offloads, Offloads(1), "The replica moved a stream that waits on nothing but Vector Set replays to the thread pool.");

            if (vectorSets)
            {
                // Run on the thread pool, so read only once the stream has been accounted for
                using var replica = new RawRespSession(endpoint: context.endpoints[1]);
                ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
                ClassicAssert.AreEqual(":2\r\n", replica.Execute("VCARD", "vector"), "The writes were replayed before the adds queued ahead of them");
            }
        }

        /// <summary>
        /// A replica that truncates its AOF fast resets a sublog, on the device, before it enqueues a chunk the primary
        /// shipped from an address it skipped ahead to. Such a chunk, delivered on a thread that must not wait, moves to the
        /// thread pool; a chunk that follows on from the previous one runs in place.
        /// </summary>
        /// <remarks>
        /// Where a chunk runs is decided before the command is run, so the node need not be a replica to show it. Being
        /// none, it fails the command and closes the session either way. Each chunk is delivered from a thread outside the
        /// pool, as a socket event thread delivers it when completions run inline.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public async Task AChunkThatSkipsAheadLeavesAThreadThatMustNotWaitWhenTheAofIsTruncatedFast()
        {
            context.CreateInstances(1, enableAOF: true, FastAofTruncate: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, asyncReplay: true,
                disableObjects: true, clusterConfigFlushFrequencyMs: -1, enableVectorSetPreview: false);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var record = new byte[64];

            var offloads = Offloads(0);
            await AppendAsync(previousAddress: 64, currentAddress: 64);
            ClassicAssert.AreEqual(offloads, Offloads(0), "A chunk that follows on from the previous one was moved to the thread pool.");

            await AppendAsync(previousAddress: 64, currentAddress: 64 + record.Length + 8);
            ClassicAssert.AreEqual(offloads + 1, Offloads(0), "A chunk that skips ahead was run on the thread that delivered it.");

            async Task AppendAsync(long previousAddress, long currentAddress)
            {
                var input = Frame(Ascii("CLUSTER"), Ascii("appendlog"), Ascii(new string('a', 40)), Ascii("0"),
                    Ascii(previousAddress.ToString()), Ascii(currentAddress.ToString()), Ascii((currentAddress + record.Length).ToString()), record);
                var session = NewSession(new ControlledSender(1024));
                try
                {
                    var call = OffThePool(() => Consume(session, input));
                    await WithinBoundAsync(call, "the chunk to return to the receive loop");
                    await WithinBoundAsync((await call).AsTask(), "the chunk to be run");
                }
                finally
                {
                    session.Dispose();
                    GC.KeepAlive(input);
                }
            }
        }

        /// <summary>
        /// A primary ends a diskless sync by attaching to the replica it synced, which then records the primary's history
        /// in its configuration file before it answers. A device that writes on the thread pool completes that write there,
        /// so the attach, which runs on the pool, awaits the write instead of waiting for it, and finishes with a single
        /// free pool thread.
        /// </summary>
        /// <remarks>
        /// The attach is delivered from a thread outside the pool, as a socket event thread delivers it when completions
        /// run inline, and moves to the pool from there.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public void TheRecoveryThatEndsADisklessSyncFinishesWithOneFreePoolThread()
        {
            context.CreateInstances(1, enableAOF: true, disableObjects: true, deviceType: Tsavorite.core.DeviceType.RandomAccess);
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
            try
            {
                // Waited on from the test thread, which is outside the pool, so that the cap holds for the test alone
                using (ThreadPoolCap.ApplyFree(1))
                {
                    var call = OffThePool(() => Consume(session, input));
                    ClassicAssert.IsTrue(call.Wait(Bound), "The attach did not return to the receive loop");

                    var finished = call.Result.AsTask();
                    ClassicAssert.IsTrue(finished.Wait(Bound), "The attach did not finish with one free pool thread, which it holds while the configuration is written on the pool");
                    ClassicAssert.AreEqual(input.Length, finished.Result);
                }

                var offset = address.ToString();
                ClassicAssert.AreEqual($"${offset.Length}\r\n{offset}\r\n", sender.Sent, "The recovery answers with the offset the replica resumes from");
            }
            finally
            {
                session.Dispose();
                GC.KeepAlive(input);
                if (replication.IsRecovering)
                    replication.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
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
        /// A replica that recovers from the checkpoint its primary sent then replays its own AOF over it. Replay holds the
        /// thread replaying for as long as the AOF is long, and replaying a record can wait there on work the thread pool
        /// runs. Replay runs on threads of its own, whether the AOF is a single log, several sublogs, or a log replayed by
        /// several tasks, so the node answers its clients from a single free pool thread meanwhile.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void TheAofReplayOfAReplicaRecoveryHoldsNoPoolThread([Values(1, 2)] int sublogCount, [Values(1, 2)] int replayTaskCount)
        {
            const ExceptionInjectionType hold = ExceptionInjectionType.AofRecovery_Hold_Replay_Thread;
            const int keyCount = 64;

            context.CreateInstances(1, tryRecover: true, enableAOF: true, disableObjects: true, sublogCount: sublogCount, replayTaskCount: replayTaskCount);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            using (var writer = new RawRespSession(endpoint: context.endpoints[0]))
            {
                for (var i = 0; i < keyCount; i++)
                    ClassicAssert.AreEqual("+OK\r\n", writer.Execute("SET", $"key{i}", $"value{i}"));
                ClassicAssert.AreEqual("+AOF file committed\r\n", writer.Execute("COMMITAOF"));
            }

            // Replays only an AOF recovered from its commits, which is what records the sequence number that bounds the
            // replay of several sublogs
            context.RestartNode(0);

            using var client = new RawRespSession(endpoint: context.endpoints[0]);
            var storeWrapper = context.nodes[0].Provider.StoreWrapper;
            var replication = ((ClusterProvider)storeWrapper.clusterProvider).replicationManager;
            var beginAddress = storeWrapper.appendOnlyFile.Log.BeginAddress;
            var tailAddress = storeWrapper.appendOnlyFile.Log.TailAddress;

            // What a replica holds while its primary syncs it, and what the recovery ends
            ClassicAssert.IsTrue(replication.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false));

            // Recovers nothing from the checkpoint, and replays the whole AOF over the store
            var input = Frame(
                Ascii("CLUSTER"), Ascii("begin_replica_recover"),
                Ascii("0"), Ascii("1"), Ascii(new string('c', 40)),
                new CheckpointEntry(sublogCount).ToByteArray(), beginAddress.Span.ToArray(), tailAddress.Span.ToArray());

            var sender = new ControlledSender(1024);
            var session = NewSession(sender);
            ExceptionInjectionHelper.EnableException(hold);
            try
            {
                ValueTask<int> consume;

                // Waited on from the test thread, which is outside the pool, so that the cap holds for the test alone
                using (ThreadPoolCap.ApplyFree(1))
                {
                    var call = OffThePool(() => Consume(session, input));
                    ClassicAssert.IsTrue(call.Wait(Bound), "The command did not return to the receive loop");
                    consume = call.Result;

                    var clock = Stopwatch.StartNew();
                    while (ExceptionInjectionHelper.WaitingOnClear(hold) == 0)
                    {
                        ClassicAssert.Less(clock.Elapsed, Bound, "The replay did not reach a record");
                        Thread.Sleep(10);
                    }

                    client.Send("PING");
                    ClassicAssert.IsTrue(client.WaitForReply(Bound), "The node did not answer PING with one free pool thread, which the replay holds");
                    ClassicAssert.AreEqual("+PONG\r\n", client.ReadReply());
                    ClassicAssert.IsFalse(consume.IsCompleted, "The command waits for the replay");
                }

                ExceptionInjectionHelper.DisableException(hold);
                var finished = consume.AsTask();
                ClassicAssert.IsTrue(finished.Wait(Bound), "The recovery did not finish once the replay was let go");
                ClassicAssert.AreEqual(input.Length, finished.Result);
                StringAssert.StartsWith("$", sender.Sent, "The recovery answers with the offset it reached");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(hold);
                session.Dispose();
                GC.KeepAlive(input);
                if (replication.IsRecovering)
                    replication.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }

            for (var i = 0; i < keyCount; i++)
                ClassicAssert.AreEqual($"${$"value{i}".Length}\r\nvalue{i}\r\n", client.Execute("GET", $"key{i}"));
        }

        /// <summary>
        /// A primary ends a diskless sync by attaching to the replica it synced, which then resets its AOF, records the
        /// primary's history and waits for its queued Vector Set replays before it answers. The session that received
        /// the attach on a thread that must not wait parks while that runs on the thread pool, and answers with the offset
        /// the replica resumes from.
        /// </summary>
        /// <remarks>
        /// The attach is delivered from a thread outside the pool, as a socket event thread delivers it when completions
        /// run inline.
        /// </remarks>
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
                var call = OffThePool(() => Consume(session, input));
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

        /// <summary>
        /// The attach that ends a diskless sync waits for the Vector Set replays the replica has queued before it answers.
        /// Those replays run on the thread pool, so the attach, which runs on the pool, waits for them parked, and the
        /// replica answers its clients from a single free pool thread meanwhile.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void TheRecoveryThatEndsADisklessSyncWaitsForQueuedVectorSetReplaysParked()
        {
            const ExceptionInjectionType hold = ExceptionInjectionType.VectorSet_Hold_Replication_Replay;

            context.CreateInstances(2, enableAOF: true, clusterConfigFlushFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "1", "2", "3", "a", "NOQUANT"));
            ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
            while (replica.Execute("VCARD", "vector") != ":1\r\n")
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token, msg: "waiting for the first add on the replica");

            var replication = ((ClusterProvider)context.nodes[1].Provider.StoreWrapper.clusterProvider).replicationManager;
            var address = AofAddress.Create(1, 64);
            var attach = new SyncMetadata(fullSync: false, Garnet.cluster.NodeRole.PRIMARY, new string('a', 40), new string('b', 40),
                currentStoreVersion: 0, address, address, address, checkpointEntry: null);
            var input = Frame(Ascii("CLUSTER"), Ascii("attach_sync"), attach.ToByteArray());

            var sender = new ControlledSender(1024);
            var session = NewSession(sender, node: 1);
            ExceptionInjectionHelper.EnableException(hold);
            try
            {
                // Queued on the replica and held there
                ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "4", "5", "6", "b", "NOQUANT"));
                while (ExceptionInjectionHelper.WaitingOnClear(hold) == 0)
                    ClusterTestUtils.BackOff(cancellationToken: context.cts.Token, msg: "waiting for the replica to queue the add");

                // What a replica holds while its primary syncs it, and what the recovery ends
                ClassicAssert.IsTrue(replication.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false));

                ValueTask<int> consume;

                // Waited on from the test thread, which is outside the pool, so that the cap holds for the test alone
                using (ThreadPoolCap.ApplyFree(1))
                {
                    var call = OffThePool(() => Consume(session, input));
                    ClassicAssert.IsTrue(call.Wait(Bound), "The attach did not return to the receive loop");
                    consume = call.Result;
                    ClassicAssert.IsFalse(consume.IsCompleted, "The attach waits for the queued add");

                    // Nothing neutral shows the attach reaching the wait, which follows a configuration write, so the
                    // replica is kept busy for long enough
                    var clock = Stopwatch.StartNew();
                    do
                    {
                        replica.Send("PING");
                        ClassicAssert.IsTrue(replica.WaitForReply(Bound), "The replica did not answer PING with one free pool thread, which the attach holds while it waits for the queued add");
                        ClassicAssert.AreEqual("+PONG\r\n", replica.ReadReply());
                        Thread.Sleep(10);
                    }
                    while (clock.Elapsed < TimeSpan.FromSeconds(1));

                    ClassicAssert.IsEmpty(sender.Sent, "Nothing is answered until the add has been applied");
                }

                ExceptionInjectionHelper.DisableException(hold);
                var finished = consume.AsTask();
                ClassicAssert.IsTrue(finished.Wait(Bound), "The attach did not finish once the add was let go");
                ClassicAssert.AreEqual(input.Length, finished.Result);
                StringAssert.StartsWith("$", sender.Sent, "The recovery answers with the offset the replica resumes from");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(hold);
                session.Dispose();
                GC.KeepAlive(input);
                if (replication.IsRecovering)
                    replication.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }
        }

        /// <summary>
        /// A chunk that skips ahead resets the sublog and then waits for the Vector Set replays the replica has queued
        /// before it advertises the address skipped to. Those replays run on the thread pool, so the chunk, which runs on
        /// the pool, waits for them parked, and the replica answers its clients from a single free pool thread meanwhile.
        /// </summary>
        /// <remarks>
        /// The chunk is crafted, and sent on a session of its own, which the replica runs as it does its primary's. The
        /// session has not set up the replay, so the replica is set to replay in the background, which it hands the chunk
        /// to after the reset; failing there once the replays are let go is no part of what is shown.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public void AChunkThatSkipsAheadWaitsForQueuedVectorSetReplaysParked()
        {
            const ExceptionInjectionType hold = ExceptionInjectionType.VectorSet_Hold_Replication_Replay;

            context.CreateInstances(2, enableAOF: true, FastAofTruncate: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, asyncReplay: true,
                clusterConfigFlushFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "1", "2", "3", "a", "NOQUANT"));
            ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
            while (replica.Execute("VCARD", "vector") != ":1\r\n")
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token, msg: "waiting for the first add on the replica");

            var session = NewSession(new ControlledSender(1024), node: 1);
            byte[] input = null;
            ExceptionInjectionHelper.EnableException(hold);
            try
            {
                // Queued on the replica and held there
                ClassicAssert.AreEqual(":1\r\n", primary.Execute("VADD", "vector", "VALUES", "3", "4", "5", "6", "b", "NOQUANT"));
                while (ExceptionInjectionHelper.WaitingOnClear(hold) == 0)
                    ClusterTestUtils.BackOff(cancellationToken: context.cts.Token, msg: "waiting for the replica to queue the add");

                var tail = context.nodes[1].Provider.StoreWrapper.appendOnlyFile.Log.GetTailAddress(0);
                var record = new byte[64];
                input = Frame(Ascii("CLUSTER"), Ascii("appendlog"), Ascii(context.clusterTestUtils.GetNodeIdFromNode(0, context.logger)), Ascii("0"),
                    Ascii(tail.ToString()), Ascii((tail + record.Length).ToString()), Ascii((tail + 2 * record.Length).ToString()), record);

                ValueTask<int> consume;

                // Waited on from the test thread, which is outside the pool, so that the cap holds for the test alone
                using (ThreadPoolCap.ApplyFree(1))
                {
                    var call = OffThePool(() => Consume(session, input));
                    ClassicAssert.IsTrue(call.Wait(Bound), "The chunk did not return to the receive loop");
                    consume = call.Result;
                    ClassicAssert.IsFalse(consume.IsCompleted, "The chunk waits for the queued add");

                    replica.Send("PING");
                    ClassicAssert.IsTrue(replica.WaitForReply(Bound), "The replica did not answer PING with one free pool thread, which the chunk holds while it waits for the queued add");
                    ClassicAssert.AreEqual("+PONG\r\n", replica.ReadReply());
                }

                ExceptionInjectionHelper.DisableException(hold);
                ClassicAssert.IsTrue(Task.WhenAny(consume.AsTask()).Wait(Bound), "The chunk did not go on once the add was let go");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(hold);
                session.Dispose();
                GC.KeepAlive(input);
            }
        }
#endif

        /// <summary>Commands <paramref name="node"/> has moved to the thread pool because they would wait on a thread that must not.</summary>
        long Offloads(int node) => Interlocked.Read(ref context.nodes[node].Provider.StoreWrapper.offloads);

        /// <summary>Runs <paramref name="call"/> on a thread outside the pool.</summary>
        static Task<T> OffThePool<T>(Func<T> call)
        {
            var result = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            new Thread(() =>
            {
                try
                {
                    result.SetResult(call());
                }
                catch (Exception e)
                {
                    result.SetException(e);
                }
            })
            { IsBackground = true }.Start();
            return result.Task;
        }

        RespServerSession NewSession(ControlledSender sender, int node = 0)
            => (RespServerSession)context.nodes[node].Provider.GetSession(WireFormat.ASCII, sender);

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