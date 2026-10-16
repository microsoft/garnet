// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

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

            var sender = new ScratchBufferNetworkSender();
            var session = (RespServerSession)context.nodes[SourceNode].Provider.GetSession(WireFormat.ASCII, sender);
            ValueTask<int> consume;
            try
            {
                consume = Consume(session, input);

                ClassicAssert.IsFalse(consume.IsCompleted, "A write to a key being migrated was not held");
                ClassicAssert.AreEqual(0, sender.GetResponse().Length, "A write to a key being migrated was answered while the migration was held");
            }
            finally
            {
                session.Dispose();
            }

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

        /// <summary>
        /// Limits the thread pool to a few worker threads for the life of the scope, so that a thread a request holds
        /// is a thread the others cannot use.
        /// </summary>
        sealed class ThreadPoolCap : IDisposable
        {
            readonly int minWorkers, minIo, maxWorkers, maxIo;
            bool restored;

            ThreadPoolCap(int minWorkers, int minIo, int maxWorkers, int maxIo)
                => (this.minWorkers, this.minIo, this.maxWorkers, this.maxIo) = (minWorkers, minIo, maxWorkers, maxIo);

            internal static ThreadPoolCap Apply(int workers)
            {
                ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
                ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
                var cap = new ThreadPoolCap(minWorkers, minIo, maxWorkers, maxIo);

                if (!ThreadPool.SetMinThreads(Math.Min(minWorkers, workers), minIo) || !ThreadPool.SetMaxThreads(workers, maxIo))
                {
                    cap.Dispose();
                    Assert.Ignore("Could not cap the thread pool, so a held thread cannot be made to matter.");
                }

                return cap;
            }

            public void Dispose()
            {
                if (restored)
                    return;
                restored = true;

                _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
            }
        }
    }
}
#endif
