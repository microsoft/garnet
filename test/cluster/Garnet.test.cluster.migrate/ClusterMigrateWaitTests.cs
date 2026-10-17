// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Net;
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
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A request for a key that a slot migration is moving waits for the migration to move on, and holds no thread
    /// while it does.
    /// </summary>
    /// <remarks>
    /// While a batch of keys is being transmitted a write to one of them has to wait: the key must not change after
    /// it has been copied, and cannot be written once it is gone. The session parks on the migration and verifies the
    /// request again each time the migration moves a step.
    /// <para>
    /// Each test holds the migration at the start of a batch's transmission, with the
    /// <see cref="ExceptionInjectionType.Migration_Slot_Keys_Transmitting"/> injection point, so that the requests
    /// under test arrive while the batch is in flight. Only keys in that batch are held, so the keys the tests use are
    /// stored before the migration starts.
    /// </para>
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterMigrateWaitTests : TestBase
    {
        const int TestTimeout = 100_000;
        const int SourceNode = 0;
        const int TargetNode = 1;

        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string Queued = "+QUEUED\r\n";
        const string Key = "{wait}key";
        const string StoredValue = "v0";

        /// <summary>The bulk string reply to a GET of <see cref="Key"/> before anything has changed it.</summary>
        const string StoredValueReply = "$2\r\nv0\r\n";

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
        /// Writers waiting on a migration hold no pool thread. The pool is smaller than the number of writers, so a
        /// writer that held a thread while it waited would leave no thread to serve a read of the same key. Each writer
        /// is redirected once the key has moved, and the request behind it runs after that, in order.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void WritersWaitingOnAMigrationHoldNoPoolThreads(CancellationToken cancellationToken)
        {
            const int WriterCount = 16;
            const int CappedWorkers = 4;

            var slot = CreateClusterHolding(Key);
            var source = context.endpoints[SourceNode];

            var writers = new RawRespSession[WriterCount];
            using var probe = new RawRespSession(endpoint: source);
            try
            {
                for (var i = 0; i < writers.Length; i++)
                    writers[i] = new RawRespSession(endpoint: source);

                // Connected and used before the pool is capped, so the cap is not measured against connecting
                ClassicAssert.AreEqual(Pong, probe.Execute("PING"));
                foreach (var writer in writers)
                    ClassicAssert.AreEqual(Pong, writer.Execute("PING"));

                using var pause = MigrationPause.Start(context, slot, cancellationToken);

                using (ThreadPoolCap.Apply(CappedWorkers))
                {
                    string[] set = ["SET", Key, "v1"];
                    string[] ping = ["PING"];

                    // The first writer also has a request ahead of the one that waits
                    writers[0].SendPipeline(ping, set, ping);
                    for (var i = 1; i < writers.Length; i++)
                        writers[i].SendPipeline(set, ping);

                    // Reading the migrating key is allowed, and is served although every writer is waiting
                    probe.Send("GET", Key);
                    ClassicAssert.AreEqual(StoredValueReply, ReadReplyWithin(probe, TimeSpan.FromSeconds(10), "GET of the migrating key"),
                        $"A pool of {CappedWorkers} threads could not serve a read while {WriterCount} writers waited on the migration");

                    // What came before the waiting request is answered without waiting for the migration
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(writers[0], TimeSpan.FromSeconds(10), "PING ahead of a waiting SET"));

                    Thread.Sleep(300);
                    foreach (var writer in writers)
                        ClassicAssert.IsFalse(writer.WaitForReply(TimeSpan.Zero), "A write to a key being migrated was answered while the migration was held");
                }

                pause.Release();

                foreach (var writer in writers)
                {
                    AssertRedirectedToTarget(ReadReplyWithin(writer, TimeSpan.FromSeconds(30), "SET of a key that has moved"), slot);
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(writer, TimeSpan.FromSeconds(10), "PING behind a SET that waited"));
                }

                context.clusterTestUtils.WaitForMigrationCleanup(SourceNode, cancellationToken, context.logger);
                ClassicAssert.AreEqual(StoredValue, GetValue(TargetNode, Key), "The redirected writes must not have been applied");
            }
            finally
            {
                foreach (var writer in writers)
                    writer?.Dispose();
            }
        }

        /// <summary>
        /// Transactions waiting on a migration hold no pool thread either. The pool is smaller than the number of
        /// transactions, so one that held a thread while it waited would leave no thread to serve a read of the key.
        /// Each EXEC is verified again once the migration has moved the key, is redirected, and ends its transaction.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void ExecsWaitingOnAMigrationHoldNoPoolThreads([Values(false, true)] bool watch, CancellationToken cancellationToken)
        {
            const int TxnCount = 16;
            const int CappedWorkers = 4;

            var slot = CreateClusterHolding(Key);
            var source = context.endpoints[SourceNode];

            var txns = new RawRespSession[TxnCount];
            using var probe = new RawRespSession(endpoint: source);
            try
            {
                // Queuing does not verify slots, so the transactions are set up before the migration starts
                for (var i = 0; i < txns.Length; i++)
                {
                    txns[i] = new RawRespSession(endpoint: source);
                    if (watch)
                        ClassicAssert.AreEqual(Ok, txns[i].Execute("WATCH", Key));
                    ClassicAssert.AreEqual(Ok, txns[i].Execute("MULTI"));
                    ClassicAssert.AreEqual(Queued, txns[i].Execute("SET", Key, "v1"));
                }

                ClassicAssert.AreEqual(Pong, probe.Execute("PING"));

                using var pause = MigrationPause.Start(context, slot, cancellationToken);

                using (ThreadPoolCap.Apply(CappedWorkers))
                {
                    foreach (var txn in txns)
                        txn.Send("EXEC");

                    probe.Send("GET", Key);
                    ClassicAssert.AreEqual(StoredValueReply, ReadReplyWithin(probe, TimeSpan.FromSeconds(10), "GET of the migrating key"),
                        $"A pool of {CappedWorkers} threads could not serve a read while {TxnCount} transactions waited on the migration");

                    Thread.Sleep(300);
                    foreach (var txn in txns)
                        ClassicAssert.IsFalse(txn.WaitForReply(TimeSpan.Zero), "An EXEC of a key being migrated was answered while the migration was held");
                }

                pause.Release();

                foreach (var txn in txns)
                {
                    AssertRedirectedToTarget(ReadReplyWithin(txn, TimeSpan.FromSeconds(30), "EXEC of a key that has moved"), slot);

                    // A PING still inside the transaction would have been queued
                    ClassicAssert.AreEqual(Pong, txn.Execute("PING"));
                }

                context.clusterTestUtils.WaitForMigrationCleanup(SourceNode, cancellationToken, context.logger);
                ClassicAssert.AreEqual(StoredValue, GetValue(TargetNode, Key), "The redirected transactions must not have been applied");
            }
            finally
            {
                foreach (var txn in txns)
                    txn?.Dispose();
            }
        }

        /// <summary>
        /// Custom transactions waiting on a migration hold no pool thread either. The pool is smaller than the number of
        /// transactions, so one whose <c>Prepare</c> held a thread while it waited would leave no thread to serve a read of
        /// the key. Each transaction is run again from the start once the migration has moved the key, and is redirected.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void CustomTransactionsWaitingOnAMigrationHoldNoPoolThreads(CancellationToken cancellationToken)
        {
            const int TxnCount = 16;
            const int CappedWorkers = 4;
            const string Txn = "DELRMW";

            var slot = CreateClusterHolding(Key);
            var source = context.endpoints[SourceNode];
            _ = context.nodes[SourceNode].Register.NewTransactionProc(Txn, () => new ClusterDelRmw(), new RespCommandsInfo { Arity = 3 });

            var txns = new RawRespSession[TxnCount];
            using var probe = new RawRespSession(endpoint: source);
            try
            {
                for (var i = 0; i < txns.Length; i++)
                {
                    txns[i] = new RawRespSession(endpoint: source);
                    ClassicAssert.AreEqual(Pong, txns[i].Execute("PING"));
                }

                ClassicAssert.AreEqual(Pong, probe.Execute("PING"));

                using var pause = MigrationPause.Start(context, slot, cancellationToken);

                using (ThreadPoolCap.Apply(CappedWorkers))
                {
                    string[] txn = [Txn, Key, "5"];
                    string[] ping = ["PING"];

                    foreach (var session in txns)
                        session.SendPipeline(txn, ping);

                    probe.Send("GET", Key);
                    ClassicAssert.AreEqual(StoredValueReply, ReadReplyWithin(probe, TimeSpan.FromSeconds(10), "GET of the migrating key"),
                        $"A pool of {CappedWorkers} threads could not serve a read while {TxnCount} custom transactions waited on the migration");

                    Thread.Sleep(300);
                    foreach (var session in txns)
                        ClassicAssert.IsFalse(session.WaitForReply(TimeSpan.Zero), "A custom transaction on a key being migrated was answered while the migration was held");
                }

                pause.Release();

                foreach (var session in txns)
                {
                    AssertRedirectedToTarget(ReadReplyWithin(session, TimeSpan.FromSeconds(30), "Custom transaction on a key that has moved"), slot);
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(session, TimeSpan.FromSeconds(10), "PING behind a custom transaction that waited"));
                }

                context.clusterTestUtils.WaitForMigrationCleanup(SourceNode, cancellationToken, context.logger);
                ClassicAssert.AreEqual(StoredValue, GetValue(TargetNode, Key), "The redirected transactions must not have been applied");
            }
            finally
            {
                foreach (var session in txns)
                    session?.Dispose();
            }
        }

        /// <summary>How the <c>Prepare</c> of <see cref="FinalizeCountingTxn"/> ends.</summary>
        public enum PrepareOutcome
        {
            /// <summary>Returns true.</summary>
            Succeeds,

            /// <summary>Returns false.</summary>
            Fails,

            /// <summary>Throws.</summary>
            Throws,
        }

        /// <summary>
        /// A custom transaction that waits on a migration runs its <c>Finalize</c> once, when it is answered, however its
        /// <c>Prepare</c> ends. The runs that leave the transaction to be run again answer nothing, so they finalize
        /// nothing.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void ACustomTransactionWaitingOnAMigrationIsFinalizedOnce([Values] PrepareOutcome outcome, CancellationToken cancellationToken)
        {
            const string Txn = "FINALIZECOUNT";

            var slot = CreateClusterHolding(Key);
            var source = context.endpoints[SourceNode];
            var finalized = new StrongBox<int>();
            _ = context.nodes[SourceNode].Register.NewTransactionProc(Txn, () => new FinalizeCountingTxn(outcome, finalized), new RespCommandsInfo { Arity = 2 });

            using var session = new RawRespSession(endpoint: source);
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));

            using (var pause = MigrationPause.Start(context, slot, cancellationToken))
            {
                session.Send(Txn, Key);

                Thread.Sleep(300);
                ClassicAssert.IsFalse(session.WaitForReply(TimeSpan.Zero), "A custom transaction on a key being migrated was answered while the migration was held");
                ClassicAssert.AreEqual(0, Volatile.Read(ref finalized.Value), "A custom transaction left to be run again was finalized");

                pause.Release();

                var reply = ReadReplyWithin(session, TimeSpan.FromSeconds(30), "Custom transaction on a key that has moved");
                if (outcome == PrepareOutcome.Succeeds)
                    AssertRedirectedToTarget(reply, slot);
                else
                    ClassicAssert.AreEqual("-ERR Transaction failed.\r\n", reply);
            }

            ClassicAssert.AreEqual(1, Volatile.Read(ref finalized.Value), "A custom transaction that waited on a migration was not finalized exactly once");
        }

        /// <summary>
        /// Custom transaction on one key that counts the runs of its <c>Finalize</c>, and whose <c>Prepare</c> ends as
        /// <see cref="PrepareOutcome"/> says once it has added the key.
        /// </summary>
        sealed class FinalizeCountingTxn(PrepareOutcome outcome, StrongBox<int> finalized) : CustomTransactionProcedure
        {
            public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            {
                var offset = 0;
                AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
                return outcome switch
                {
                    PrepareOutcome.Succeeds => true,
                    PrepareOutcome.Fails => false,
                    _ => throw new InvalidOperationException("Prepare throws"),
                };
            }

            public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
                => WriteSimpleString(ref output, "OK");

            public override void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
                => _ = Interlocked.Increment(ref finalized.Value);
        }

        /// <summary>
        /// When a migration fails, the requests waiting on it are served by the node that still owns the keys. The
        /// failure never reaches the steps that clear a batch, so the migration task going away is what wakes them.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void RequestsWaitingOnAMigrationThatFailsAreServed(CancellationToken cancellationToken)
        {
            const string KeyA = "{fail}a";
            const string KeyB = "{fail}b";

            var slot = CreateClusterHolding(KeyA, KeyB);
            var source = context.endpoints[SourceNode];

            using var writer = new RawRespSession(endpoint: source);
            using var txn = new RawRespSession(endpoint: source);

            ClassicAssert.AreEqual(Ok, txn.Execute("MULTI"));
            ClassicAssert.AreEqual(Queued, txn.Execute("SET", KeyB, "v2"));

            using var pause = MigrationPause.Start(context, slot, cancellationToken);

            writer.Send("SET", KeyA, "v1");
            txn.Send("EXEC");

            Thread.Sleep(300);
            ClassicAssert.IsFalse(writer.WaitForReply(TimeSpan.Zero), "A write to a key being migrated was answered while the migration was held");
            ClassicAssert.IsFalse(txn.WaitForReply(TimeSpan.Zero), "An EXEC of a key being migrated was answered while the migration was held");

            // With the target gone the batch cannot be transmitted, so the migration fails
            context.ShutdownNode(TargetNode);

            pause.Release();

            ClassicAssert.AreEqual(Ok, ReadReplyWithin(writer, TimeSpan.FromSeconds(30), "SET waiting on a migration that failed"));
            ClassicAssert.AreEqual("*1\r\n+OK\r\n", ReadReplyWithin(txn, TimeSpan.FromSeconds(30), "EXEC waiting on a migration that failed"));

            context.clusterTestUtils.WaitForMigrationCleanup(SourceNode, cancellationToken, context.logger);
            ClassicAssert.AreEqual("v1", GetValue(SourceNode, KeyA));
            ClassicAssert.AreEqual("v2", GetValue(SourceNode, KeyB));
        }

        /// <summary>
        /// Forgetting the target of a migration ends the migration task and gives the slot back to the node, so the
        /// requests waiting on the migration are served by it. The migration is held throughout and takes no step, so
        /// the task going away is what wakes them.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void RequestsWaitingOnAMigrationAreServedWhenItsTargetIsForgotten(CancellationToken cancellationToken)
        {
            const string KeyA = "{forget}a";
            const string KeyB = "{forget}b";

            var slot = CreateClusterHolding(KeyA, KeyB);
            var source = context.endpoints[SourceNode];

            using var writer = new RawRespSession(endpoint: source);
            using var txn = new RawRespSession(endpoint: source);

            ClassicAssert.AreEqual(Ok, txn.Execute("MULTI"));
            ClassicAssert.AreEqual(Queued, txn.Execute("SET", KeyB, "v2"));

            using var pause = MigrationPause.Start(context, slot, cancellationToken);

            writer.Send("SET", KeyA, "v1");
            txn.Send("EXEC");

            Thread.Sleep(300);
            ClassicAssert.IsFalse(writer.WaitForReply(TimeSpan.Zero), "A write to a key being migrated was answered while the migration was held");
            ClassicAssert.IsFalse(txn.WaitForReply(TimeSpan.Zero), "An EXEC of a key being migrated was answered while the migration was held");

            var targetId = context.clusterTestUtils.ClusterMyId(TargetNode, context.logger);
            _ = context.clusterTestUtils.ClusterForget(SourceNode, targetId, 60, context.logger);

            ClassicAssert.AreEqual(Ok, ReadReplyWithin(writer, TimeSpan.FromSeconds(30), "SET waiting on a migration whose target was forgotten"));
            ClassicAssert.AreEqual("*1\r\n+OK\r\n", ReadReplyWithin(txn, TimeSpan.FromSeconds(30), "EXEC waiting on a migration whose target was forgotten"));

            pause.Release();

            ClassicAssert.AreEqual("v1", GetValue(SourceNode, KeyA));
            ClassicAssert.AreEqual("v2", GetValue(SourceNode, KeyB));
        }

        /// <summary>
        /// Vector Set writes to a slot that is migrating, or importing, wait for the slot to become stable again, and
        /// are parked on the cluster configuration while they do rather than each holding the thread it runs on. Once
        /// the slot is stable they are verified again and served, in order with what follows them.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void VectorSetWritesWaitingForAStableSlotAreParked([Values] bool importing, CancellationToken cancellationToken)
        {
            const int WriterCount = 8;
            const string VectorKey = "{vwait}vectors";

            var slot = CreateClusterHolding(VectorKey + "-unused");
            var node = importing ? TargetNode : SourceNode;
            var endpoint = context.endpoints[node];
            var peerId = context.clusterTestUtils.ClusterMyId(importing ? SourceNode : TargetNode, context.logger);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.SetSlot(node, slot, importing ? "IMPORTING" : "MIGRATING", peerId, context.logger));

            var progress = ((ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider).migrationManager.Progress;
            var writers = new RawRespSession[WriterCount];
            try
            {
                for (var i = 0; i < writers.Length; i++)
                {
                    writers[i] = new RawRespSession(endpoint: endpoint);
                    string[] vadd = ["VADD", VectorKey, "VALUES", "3", "1", "0", $"{i}", $"element-{i}", "NOQUANT"];
                    if (importing)
                        writers[i].SendPipeline(["ASKING"], vadd, ["PING"]);
                    else
                        writers[i].SendPipeline(vadd, ["PING"]);
                }

                if (importing)
                {
                    foreach (var writer in writers)
                        ClassicAssert.AreEqual(Ok, ReadReplyWithin(writer, TimeSpan.FromSeconds(10), "ASKING ahead of a waiting VADD"));
                }

                while (progress.WaiterCount < WriterCount)
                    ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: $"{progress.WaiterCount} of {WriterCount} Vector Set writes are parked on the slot");

                Thread.Sleep(300);
                foreach (var writer in writers)
                    ClassicAssert.IsFalse(writer.WaitForReply(TimeSpan.Zero), "A Vector Set write was answered while its slot was not stable");

                ClassicAssert.AreEqual("OK", context.clusterTestUtils.SetSlot(node, slot, "STABLE", "", context.logger));

                // The source owns the slot throughout, so the target redirects to it once it no longer imports the slot
                var sourcePort = ((IPEndPoint)context.endpoints[SourceNode]).Port;
                foreach (var writer in writers)
                {
                    var reply = ReadReplyWithin(writer, TimeSpan.FromSeconds(30), "VADD to a slot that became stable");
                    if (importing)
                        Assert.That(reply, Does.Match($@"^-MOVED {slot} \S+:{sourcePort}\r\n$"));
                    else
                        ClassicAssert.AreEqual(":1\r\n", reply);
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(writer, TimeSpan.FromSeconds(10), "PING behind a VADD that waited"));
                }

                ClassicAssert.AreEqual(0, progress.WaiterCount, "A Vector Set write stayed parked after its slot became stable");
            }
            finally
            {
                foreach (var writer in writers)
                    writer?.Dispose();
            }
        }

        /// <summary>
        /// A session that is torn down while a request of it waits on a migration stops waiting. The migration is held
        /// and takes no step, so nothing else would end the wait, and the request would stay parked on the receive
        /// buffer it was parsed from.
        /// </summary>
        /// <remarks>
        /// The session is driven directly rather than through a connection. A parked session has no receive posted,
        /// so closing its connection is not noticed until the wait ends, and disposing the server resumes the held
        /// migration before it disposes the sessions, which wakes the wait by itself.
        /// </remarks>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public async Task ASessionTornDownWhileWaitingOnAMigrationStopsWaiting(CancellationToken cancellationToken)
        {
            var slot = CreateClusterHolding(Key);
            using var pause = MigrationPause.Start(context, slot, cancellationToken);

            var request = Encoding.ASCII.GetBytes($"*3\r\n$3\r\nSET\r\n${Key.Length}\r\n{Key}\r\n$2\r\nv1\r\n");
            var input = GC.AllocateArray<byte>(request.Length, pinned: true);
            request.CopyTo(input, 0);

            var progress = ((ClusterProvider)context.nodes[SourceNode].Provider.StoreWrapper.clusterProvider).migrationManager.Progress;
            var sender = new ScratchBufferNetworkSender();
            var session = (RespServerSession)context.nodes[SourceNode].Provider.GetSession(WireFormat.ASCII, sender);
            ValueTask<int> consume;
            try
            {
                consume = Consume(session, input);

                ClassicAssert.IsFalse(consume.IsCompleted, "A write to a key being migrated was not held");
                ClassicAssert.AreEqual(0, sender.GetResponse().Length, "A write to a key being migrated was answered while the migration was held");
                ClassicAssert.AreEqual(1, progress.WaiterCount, "The held write is not registered with the migration");
            }
            finally
            {
                session.Dispose();
            }

            // The migration is held, and would otherwise keep the torn-down session until it moved on
            ClassicAssert.AreEqual(0, progress.WaiterCount, "A session that was torn down stayed registered with the migration");

            // Teardown can also run between the session arming its wait and registering it, and then finds nothing to
            // take off. The verification the write made is still current, since the migration has not moved on.
            ClassicAssert.IsTrue(session.MigrationWait.TryRegister(session.clusterSession), "The migration refused the registration, so nothing was tested");
            ClassicAssert.AreEqual(0, progress.WaiterCount, "A wait registered after teardown stayed registered with the migration");

            // Whether the teardown or the wake it causes reaches the session first decides whether the receive
            // completes or faults, and either ends the wait
            var consumed = consume.AsTask();
            var finished = await Task.WhenAny(consumed, Task.Delay(TimeSpan.FromSeconds(15), cancellationToken));
            ClassicAssert.AreSame(consumed, finished, "A session that was torn down kept waiting on the migration");
        }

        /// <summary>
        /// Creates a two node cluster with every slot on the first node, and stores the keys, which must share a slot,
        /// on it.
        /// </summary>
        /// <returns>The slot of the keys.</returns>
        int CreateClusterHolding(params string[] keys)
        {
            context.CreateInstances(2, disableObjects: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(SourceNode, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(SourceNode, SourceNode + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(TargetNode, TargetNode + 1, logger: context.logger);
            context.clusterTestUtils.Meet(SourceNode, TargetNode, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(TargetNode, SourceNode, logger: context.logger);
            // Slot state transitions are issued against the source, so the source has to know the target node id
            context.clusterTestUtils.WaitUntilNodeIsKnown(SourceNode, TargetNode, logger: context.logger);

            var server = context.clusterTestUtils.GetServer(SourceNode);
            foreach (var key in keys)
                ClassicAssert.AreEqual("OK", (string)server.Execute(0, "set", [key, StoredValue]));

            return HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(keys[0]));
        }

        string GetValue(int node, string key)
            => (string)context.clusterTestUtils.GetServer(node).Execute(0, "get", [key]);

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        void AssertRedirectedToTarget(string reply, int slot)
        {
            var targetPort = ((IPEndPoint)context.endpoints[TargetNode]).Port;
            Assert.That(reply, Does.Match($@"^-(ASK|MOVED) {slot} \S+:{targetPort}\r\n$"));
        }

        static string ReadReplyWithin(RawRespSession session, TimeSpan timeout, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(timeout), $"{what}: no reply within {timeout.TotalSeconds:F0}s");
            return session.ReadReply();
        }

        /// <summary>
        /// Holds the migration of a slot at the start of its first batch's transmission, which is when the batch's keys
        /// become unavailable to writers, until it is released.
        /// </summary>
        sealed class MigrationPause : IDisposable
        {
            const ExceptionInjectionType Point = ExceptionInjectionType.Migration_Slot_Keys_Transmitting;

            /// <summary>Starts migrating the slot to the target node, and returns once it is held.</summary>
            internal static MigrationPause Start(ClusterTestContext context, int slot, CancellationToken cancellationToken)
            {
                var pause = new MigrationPause();
                try
                {
                    ExceptionInjectionHelper.EnableException(Point);
                    context.clusterTestUtils.MigrateSlotsIndex(SourceNode, TargetNode, [slot], logger: context.logger);

                    // The migration clears the injection point when it arrives
                    while (ExceptionInjectionHelper.IsEnabled(Point))
                        ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for the migration to transmit its first batch");
                    return pause;
                }
                catch
                {
                    pause.Dispose();
                    throw;
                }
            }

            /// <summary>Lets the migration go on.</summary>
            internal void Release() => ExceptionInjectionHelper.EnableException(Point);

            /// <summary>
            /// Releasing leaves the injection point enabled, so it is cleared once the migration has finished with it.
            /// </summary>
            public void Dispose() => ExceptionInjectionHelper.DisableException(Point);
        }
    }
}
#endif