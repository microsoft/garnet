// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A slot migration ended from outside, by forgetting its target or shutting its node down, releases its
    /// connections only once the tasks using them are done with them.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterMigrateDisposeTests : TestBase
    {
        const int TestTimeout = 100_000;
        const int SourceNode = 0;
        const int TargetNode = 1;

        const string Key = "{wait}key";
        const string StoredValue = "v0";

        ClusterTestContext context;
        AssertionRecorder assertions;

        [SetUp]
        public void Setup()
        {
            // Ahead of the test host's listener, which turns a failed assertion into an exception the server may catch
            assertions = new AssertionRecorder();
            Trace.Listeners.Insert(0, assertions);

            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                context?.TearDown();
            }
            finally
            {
                Trace.Listeners.Remove(assertions);
            }
            ClassicAssert.AreEqual(0, assertions.Failures, "A debug assertion failed on the server");
        }

        /// <summary>
        /// Forgetting the target of a migration while a batch is being transmitted ends the migration, but leaves the
        /// connections to the tasks still using them: disposing one from another thread races the task's send through
        /// it, and can return the send buffer twice or let the task write into it after it is back in the pool. The
        /// last task to finish disposes them, and every buffer the migration rented is then back in its pool.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void AMigrationWhoseTargetIsForgottenMidBatchKeepsItsConnectionsUntilItIsDone(CancellationToken cancellationToken)
        {
            var slot = CreateClusterHolding(Key);
            var manager = ((ClusterProvider)context.nodes[SourceNode].Provider.StoreWrapper.clusterProvider).migrationManager;

            using var pause = MigrationPause.Start(context, slot, cancellationToken);

            var migration = MigrateSessions(MigrationTaskStore(manager))[slot];
            ClassicAssert.IsNotNull(migration, "The held migration is not in the task store");
            var clients = Array.ConvertAll(MigrateOperations(migration), static o => o.Client);

            var targetId = context.clusterTestUtils.ClusterMyId(TargetNode, context.logger);
            _ = context.clusterTestUtils.ClusterForget(SourceNode, targetId, 60, context.logger);

            ClassicAssert.IsNull(MigrateSessions(MigrationTaskStore(manager))[slot], "Forgetting the target left its migration in the task store");
            ClassicAssert.IsFalse(Array.Exists(clients, static c => c.Disposed), "Forgetting the target disposed a connection the held migration is still using");

            pause.Release();

            while (!Array.TrueForAll(clients, static c => c.Disposed) || manager.GetNetworkPool.LiveBytes != 0)
                ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: $"Waiting for the migration to dispose its connections ({manager.GetNetworkPool.GetStats()})");
        }

        /// <summary>
        /// A node shut down while a migration hands its slots over finishes shutting down, and the migration disposes
        /// its connections. Shutdown disposes the cluster manager before the migrations, so the migration cannot resume
        /// config merges when it goes on, and it still releases its connections, which shutdown waits for the buffers of.
        /// </summary>
        [Test, CancelAfter(TestTimeout)]
        [Category("CLUSTER")]
        public void ANodeShutDownWhileAMigrationHandsOverItsSlotsFinishesShuttingDown(CancellationToken cancellationToken)
        {
            const ExceptionInjectionType Hold = ExceptionInjectionType.Migration_Hold_Ownership_Transfer;

            var slot = CreateClusterHolding(Key);
            var store = MigrationTaskStore(((ClusterProvider)context.nodes[SourceNode].Provider.StoreWrapper.clusterProvider).migrationManager);

            ExceptionInjectionHelper.EnableException(Hold);
            try
            {
                context.clusterTestUtils.MigrateSlotsIndex(SourceNode, TargetNode, [slot], logger: context.logger);
                while (ExceptionInjectionHelper.WaitingOnClear(Hold) == 0)
                    ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for the migration to hand its slots over");

                var clients = Array.ConvertAll(MigrateOperations(MigrateSessions(store)[slot]), static o => o.Client);
                var shutdown = Task.Factory.StartNew(() => context.ShutdownNode(SourceNode), TaskCreationOptions.LongRunning);

                // The task store is disposed after the cluster manager, and removes the migration when it is
                while (MigrateSessions(store)[slot] != null)
                    ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for the shutdown to dispose the migration");

                ExceptionInjectionHelper.DisableException(Hold);
                ClassicAssert.IsTrue(shutdown.Wait(TimeSpan.FromSeconds(30), cancellationToken), "A node shut down while a migration handed its slots over did not finish shutting down");
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Array.TrueForAll(clients, static c => c.Disposed), TimeSpan.FromSeconds(30)), "A migration ended by its node shutting down left its connections undisposed");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Hold);
            }
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "migrationTaskStore")]
        static extern ref MigrateSessionTaskStore MigrationTaskStore(MigrationManager manager);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "sessions")]
        static extern ref MigrateSession[] MigrateSessions(MigrateSessionTaskStore store);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "migrateOperation")]
        static extern ref MigrateSession.MigrateOperation[] MigrateOperations(MigrateSession session);

        /// <summary>
        /// Creates a two node cluster with every slot on the first node, and stores the key on it.
        /// </summary>
        /// <returns>The slot of the key.</returns>
        int CreateClusterHolding(string key)
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
            ClassicAssert.AreEqual("OK", (string)server.Execute(0, "set", [key, StoredValue]));

            return HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key));
        }

        /// <summary>
        /// Counts failed debug assertions, which the server can otherwise swallow along with the exception the test
        /// host raises for them.
        /// </summary>
        sealed class AssertionRecorder : TraceListener
        {
            int failures;

            internal int Failures => Volatile.Read(ref failures);

            public override void Fail(string message, string detailMessage) => Interlocked.Increment(ref failures);

            public override void Write(string message) { }

            public override void WriteLine(string message) { }
        }

        /// <summary>
        /// Holds the migration of a slot at the start of its first batch's transmission, until it is released.
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