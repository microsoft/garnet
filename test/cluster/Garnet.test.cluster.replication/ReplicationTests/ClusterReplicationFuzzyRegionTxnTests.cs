// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Replication of transactions that commit while the primary is taking a checkpoint.
    /// </summary>
    /// <remarks>
    /// A checkpoint writes a CheckpointStartCommit and a CheckpointEndCommit record into the AOF. The span between them
    /// is the <b>fuzzy region</b>: it carries records of both the old and the new store version, so a replica replaying
    /// the stream sets them aside, takes its own checkpoint at the end marker, and only then replays what it buffered.
    /// A transaction that commits inside that span takes a different path from an ordinary write — its operations are
    /// held in a transaction group, and the group, rather than the individual records, is what has to be buffered and
    /// then replayed — so it needs coverage of its own.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationFuzzyRegionTxnTests : TestBase
    {
        ClusterTestContext context;

        const int PrimaryIndex = 0;
        const int ReplicaIndex = 1;

        // One slot, so a multi-key transaction is valid in cluster mode.
        static readonly string[] TxnKeys = ["{fz}a", "{fz}b", "{fz}c"];

        /// <summary>
        /// How long the checkpoints wait for every writer to commit a transaction before giving up. Generous, because
        /// what it covers is a blocking connection setup on a machine that may have little CPU left for the client.
        /// </summary>
        static readonly TimeSpan WriterStartupTimeout = TimeSpan.FromSeconds(30);

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();

            // Raised above the default: the wait for the writers to start committing can take up to
            // WriterStartupTimeout, and the context's own deadline must not expire first and report a cancellation
            // in place of whatever actually went wrong.
            context.Setup([], testTimeoutSeconds: 120);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        [Test]
        [Category("REPLICATION")]
        public void TransactionCommittedDuringPrimaryCheckpointReachesReplica()
        {
            SetUpPrimaryAndReplica();

            var expected = RunTransactionsAcrossCheckpoints().ToString();

            // The primary is the reference for what should have been replicated.
            AssertKeys(PrimaryIndex, expected, "primary lost transaction increments");

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, context.logger);

            // The replica must have applied every one of those transactions, including those that committed inside a
            // checkpoint's fuzzy region and were therefore buffered for replay at the end marker.
            AssertKeys(ReplicaIndex, expected, "replica lost transaction increments");
        }

        void SetUpPrimaryAndReplica()
        {
            context.CreateInstances(2, disableObjects: false, enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Give the checkpoint some real work, so it does not complete before any transaction can overlap it.
            context.kvPairs = [];
            context.SimplePopulateDB(disableObjects: true, keyLength: 16, kvpairCount: 16384, primaryIndex: PrimaryIndex);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, context.logger);
        }

        void AssertKeys(int nodeIndex, string expected, string message)
        {
            foreach (var key in TxnKeys)
            {
                var actual = context.clusterTestUtils.GetKey(nodeIndex, Encoding.ASCII.GetBytes(key), out _, out _, out _);
                ClassicAssert.AreEqual(expected, actual, $"{message} for {key}");
            }
        }

        /// <summary>
        /// Commit transactions continuously while repeated checkpoints run on the primary, so that commits land between
        /// a CheckpointStartCommit and its CheckpointEndCommit. Returns the total number of increments applied to each
        /// key, which is what every key in <see cref="TxnKeys"/> must hold on both nodes afterwards.
        /// </summary>
        /// <remarks>
        /// The two markers are not written at the start and end of the checkpoint. The primary enqueues
        /// CheckpointStartCommit on entering <c>IN_PROGRESS</c> and CheckpointEndCommit on entering <c>WAIT_FLUSH</c>,
        /// and that span is precisely the wait for transactions of the previous version to drain — not the flush, which
        /// is most of the checkpoint's duration. So the region is very short unless a transaction is holding it open:
        /// with small transactions alone it was measured at about two per checkpoint, far too thin to test against,
        /// even though several hundred transactions committed during each checkpoint overall. One writer therefore
        /// issues <b>large</b> transactions, each of which keeps the drain waiting while the small transactions from
        /// the other writers commit inside it.
        /// <para>
        /// The checkpoints do not start until every writer has committed, because they are over in a few seconds — the
        /// loop is mostly <c>WaitUntilNextSecond</c> waiting out the one second resolution of LASTSAVE — whereas a
        /// writer first has to establish a connection, which blocks on async plumbing and can take far longer on a
        /// loaded machine. A writer still connecting when the last checkpoint finishes commits nothing at all, and the
        /// checkpoints it was supposed to overlap ran against an idle primary.
        /// </para>
        /// </remarks>
        long RunTransactionsAcrossCheckpoints(int checkpointCount = 6, int smallWriterCount = 3, int largeTxnOpsPerKey = 100)
        {
            var primaryServer = context.clusterTestUtils.GetServer(PrimaryIndex);
            var primaryEndpoint = context.clusterTestUtils.GetEndPoint(PrimaryIndex);
            var increments = new StrongBox<long>();

            using var stop = new CancellationTokenSource();
            var writers = new Task[smallWriterCount + 1];
            var committing = new Task[writers.Length];

            // Writer 0 issues the large transactions that hold the drain - and so the fuzzy region - open; the rest
            // issue the short transactions that must land inside it.
            for (var w = 0; w < writers.Length; w++)
            {
                var firstCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                committing[w] = firstCommit.Task;
                writers[w] = RunWriter(primaryEndpoint, stop.Token, w == 0 ? largeTxnOpsPerKey : 1, increments, firstCommit);
            }

            Exception failure = null;
            try
            {
                // A writer that cannot commit at all faults its own wait, so the reason surfaces here rather than as
                // an absent increment long afterwards.
                if (!Task.WaitAll(committing, WriterStartupTimeout))
                    Assert.Fail($"writers did not start committing transactions within {WriterStartupTimeout}");

                for (var i = 0; i < checkpointCount; i++)
                {
                    var lastSave = context.clusterTestUtils.LastSave(PrimaryIndex, logger: context.logger);
                    context.clusterTestUtils.WaitUntilNextSecond(PrimaryIndex, lastSave);
                    primaryServer.Save(SaveType.BackgroundSave);
                    context.clusterTestUtils.WaitCheckpoint(PrimaryIndex, lastSave, logger: context.logger);
                }
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            stop.Cancel();

            // Joined on every path, including a failing one. Each writer holds a multiplexer with
            // AbortOnConnectFail false, which once abandoned reconnects to whichever server next binds this port, and
            // every cluster fixture in this assembly binds the same ports.
            try
            {
                Task.WaitAll(writers);
            }
            catch (Exception ex)
            {
                failure ??= ex;
            }

            if (failure is not null)
                ExceptionDispatchInfo.Capture(failure).Throw();

            var total = Interlocked.Read(ref increments.Value);
            ClassicAssert.Greater(total, 0, "no transactions were committed");
            return total;
        }

        /// <summary>
        /// Commit transactions on a dedicated connection until cancelled, each incrementing every key in
        /// <see cref="TxnKeys"/> <paramref name="opsPerKey"/> times, and add what was applied to
        /// <paramref name="increments"/>. <paramref name="firstCommit"/> completes once the writer is committing, or
        /// carries the failure that stopped it from getting that far.
        /// </summary>
        /// <remarks>
        /// The connection is per writer rather than the shared one: StackExchange.Redis multiplexes, so two concurrent
        /// MULTI/EXEC blocks over one connection interleave on the wire and the server sees a transaction opened inside
        /// another, which it rejects.
        /// </remarks>
        static Task RunWriter(EndPoint endpoint, CancellationToken token, int opsPerKey, StrongBox<long> increments, TaskCompletionSource firstCommit)
            => Task.Run(() =>
            {
                try
                {
                    var config = new ConfigurationOptions
                    {
                        AbortOnConnectFail = false,
                        ConnectRetry = 5,
                        ConnectTimeout = 5000,
                        // A transaction of several hundred commands, issued while the primary is checkpointing, takes
                        // longer than the five second default allows on a loaded machine.
                        SyncTimeout = 30_000,
                        IncludeDetailInExceptions = true,
                    };
                    config.EndPoints.Add(endpoint);
                    using var redis = ConnectionMultiplexer.Connect(config);
                    var db = redis.GetDatabase(0);

                    while (!token.IsCancellationRequested)
                    {
                        var txn = db.CreateTransaction();
                        for (var i = 0; i < opsPerKey; i++)
                        {
                            foreach (var key in TxnKeys)
                                _ = txn.StringIncrementAsync(key, 1);
                        }
                        ClassicAssert.IsTrue(txn.Execute(), "transaction was not committed");
                        _ = Interlocked.Add(ref increments.Value, opsPerKey);
                        _ = firstCommit.TrySetResult();
                    }
                }
                catch (Exception ex)
                {
                    _ = firstCommit.TrySetException(ex);
                    throw;
                }
            });
    }
}