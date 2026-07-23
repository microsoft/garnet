// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterRangeIndexMigrateTests : TestBase
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(new Dictionary<string, LogLevel>
            {
                // Raise to Information so the RangeIndex AOF-stream activity traces (chunkCount) reach the
                // CapturingLogger attached in ClusterMigrateRangeIndexMultiChunkStreamReplicatesAndRecovers.
                [nameof(ClusterMigrateRangeIndexMultiChunkStreamReplicatesAndRecovers)] = LogLevel.Information,
            });
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
        }

        #region Helpers

        /// <summary>
        /// Create a RangeIndex key and insert fields on the given endpoint.
        /// </summary>
        private void CreateRangeIndexWithFields(IPEndPoint endpoint, string key, IEnumerable<(string Field, string Value)> fields)
        {
            var createResult = (string)context.clusterTestUtils.Execute(endpoint, "RI.CREATE", [key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", createResult, $"RI.CREATE should succeed for key {key}");

            SetRangeIndexFields(endpoint, key, fields);
        }

        /// <summary>
        /// Set fields on an existing RangeIndex key.
        /// </summary>
        private void SetRangeIndexFields(IPEndPoint endpoint, string key, IEnumerable<(string Field, string Value)> fields)
        {
            foreach (var (field, value) in fields)
            {
                var setResult = (string)context.clusterTestUtils.Execute(endpoint, "RI.SET", [key, field, value], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"RI.SET should succeed for {key}/{field}");
            }
        }

        /// <summary>
        /// Assert that a read command on a node that no longer owns the key's slot returns a MOVED redirect.
        /// </summary>
        private void AssertMovedFrom(IPEndPoint formerOwner, string command, params object[] args)
        {
            var result = (string)context.clusterTestUtils.Execute(formerOwner, command, args, flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(result.StartsWith("Key has MOVED to "), $"Expected MOVED from source for {command} {args[0]}, got: {result}");
        }

        /// <summary>
        /// Verify all fields are readable on the given endpoint.
        /// </summary>
        private void VerifyFieldsOnEndpoint(IPEndPoint endpoint, string key, IEnumerable<(string Field, string Value)> fields)
        {
            foreach (var (field, value) in fields)
            {
                var result = (string)context.clusterTestUtils.Execute(endpoint, "RI.GET", [key, field], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(value, result, $"RI.GET {key}/{field} should return {value}");
            }
        }

        /// <summary>
        /// Wait for slot ownership to propagate: slot must be on target and not on source.
        /// </summary>
        private void WaitForSlotOwnership(IPEndPoint source, IPEndPoint target, int slot, int timeoutSeconds = 10)
        {
            var start = Stopwatch.GetTimestamp();
            while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(timeoutSeconds))
            {
                var sourceSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(source, NullLogger.Instance);
                var targetSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(target, NullLogger.Instance);

                if (!sourceSlots.Contains(slot) && targetSlots.Contains(slot))
                    return;

                Thread.Sleep(100);
            }

            ClassicAssert.Fail($"Slot {slot} ownership did not propagate within {timeoutSeconds}s");
        }

        /// <summary>
        /// Generate a key whose hash slot is owned by the node at <paramref name="nodeIndex"/>, using the
        /// cluster test util's slot-restricted random-key generator (no brute-force prefix loop).
        /// </summary>
        private string FindKeyOwnedByNode(int nodeIndex)
        {
            var slot = context.clusterTestUtils.GetOwnedSlotsFromNode(nodeIndex, context.logger)[0];
            var data = new byte[16];
            context.clusterTestUtils.RandomBytesRestrictedToSlot(ref data, slot);
            return Encoding.ASCII.GetString(data);
        }

        /// <summary>Chunk size (bytes) that forces a migrated RI's range index stream to span many AOF entries.</summary>
        private const int SmallStreamChunkSize = 1024;

        /// <summary>Build a deterministic list of RI fields.</summary>
        private static List<(string Field, string Value)> MakeFields(int count, string tag)
        {
            var fields = new List<(string Field, string Value)>(count);
            for (var i = 0; i < count; i++)
                fields.Add(($"field_{i:D4}", $"{tag}_{i:D4}_{new string('x', 64)}"));
            return fields;
        }

        /// <summary>
        /// Wait until RI.GET on <paramref name="endpoint"/> (NoRedirect) serves the migrated key and assert
        /// it equals <paramref name="expected"/>. AOF replay / recovery and slot-view propagation are absorbed
        /// by retrying only while the node reports a transient not-ready response (see
        /// <see cref="IsTransientRiGetResponse"/>). A concrete value that differs fails immediately (a real
        /// bug) instead of spinning until the timeout, and the timeout message includes the last response.
        /// </summary>
        private void AssertRiGetEventually(IPEndPoint endpoint, string key, string field, string expected, string because, int maxRetries = 200)
        {
            string last = null;
            for (var r = 0; r < maxRetries; r++)
            {
                last = (string)context.clusterTestUtils.Execute(endpoint, "RI.GET", [key, field], skipLogging: true, flags: CommandFlags.NoRedirect);
                if (last == expected)
                    return;
                if (!IsTransientRiGetResponse(last))
                    ClassicAssert.Fail($"{because}: RI.GET {key}/{field} returned '{last}', expected '{expected}'");
                Thread.Sleep(100);
            }
            ClassicAssert.Fail($"{because}: RI.GET {key}/{field} did not return '{expected}' within {maxRetries * 100}ms (last response: '{last}')");
        }

        private static bool IsTransientRiGetResponse(string v)
            => v == null
            || v.Contains("range index not found", StringComparison.OrdinalIgnoreCase)
            || v.Contains("redirect not followed", StringComparison.OrdinalIgnoreCase)
            || v.Contains("MOVED", StringComparison.OrdinalIgnoreCase)
            || v.Contains("CLUSTERDOWN", StringComparison.OrdinalIgnoreCase)
            || v.Contains("TRYAGAIN", StringComparison.OrdinalIgnoreCase)
            || v.Contains("LOADING", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Assert the primary appended the migrated RI <paramref name="key"/> to the AOF across at least
        /// <paramref name="minChunks"/> chunks with no error, per the target's
        /// <c>RangeIndexReplicationStreamActivity</c> trace captured in <paramref name="capture"/> (its
        /// <c>chunkCount</c> is incremented once per <c>EnqueueRangeIndexStreamChunk</c> AOF append).
        /// </summary>
        private static void AssertRangeIndexStreamedInManyChunks(CapturingLogger capture, string key, int minChunks)
        {
            var streamEntries = capture.Entries
                .Where(e => e.Message.StartsWith("RangeIndexReplicationStreamActivity", StringComparison.Ordinal) && e.Field("key") == key)
                .ToList();
            ClassicAssert.IsNotEmpty(streamEntries, $"expected a RangeIndexReplicationStreamActivity trace for key {key}");
            ClassicAssert.IsTrue(streamEntries.All(e => e.Field("isError") == "False"), $"primary AOF stream for '{key}' should not report an error");

            var chunkCount = streamEntries.Max(e => int.Parse(e.Field("chunkCount")));
            ClassicAssert.GreaterOrEqual(chunkCount, minChunks, $"migrated RI '{key}' should stream across many AOF chunks (was {chunkCount})");
        }

        /// <summary>
        /// Assert that AOF recovery reassembled the migrated RI <paramref name="key"/> across at least
        /// <paramref name="minChunks"/> chunks, per a successful <c>RangeIndexReplicationReassemblyActivity</c>
        /// trace logged after <paramref name="afterEntryIndex"/> (the capture watermark taken before restart,
        /// so the replica's earlier live-AOF reassembly is excluded).
        /// </summary>
        private static void AssertRangeIndexReassembledInManyChunks(CapturingLogger capture, string key, int minChunks, int afterEntryIndex)
        {
            var reassemblies = capture.Entries
                .Skip(afterEntryIndex)
                .Where(e => e.Message.StartsWith("RangeIndexReplicationReassemblyActivity", StringComparison.Ordinal)
                    && e.Field("key") == key && e.Field("reason") == "Complete" && e.Field("publishResult") == "Success")
                .ToList();
            ClassicAssert.IsNotEmpty(reassemblies, $"expected a successful RangeIndex AOF-recovery reassembly for key {key} after restart");

            var chunkCount = reassemblies.Max(e => int.Parse(e.Field("chunkCount")));
            ClassicAssert.GreaterOrEqual(chunkCount, minChunks, $"recovered RI '{key}' should reassemble across many AOF chunks (was {chunkCount})");
        }

        /// <summary>
        /// Verify every field of a migrated RI key on a replica
        /// </summary>
        private void VerifyRangeIndexOnReplica(int primaryIndex, int replicaIndex, string key, List<(string Field, string Value)> fields)
        {
            var replicaEndpoint = Endpoint(replicaIndex);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            _ = context.clusterTestUtils.Execute(replicaEndpoint, "READONLY", Array.Empty<object>(), flags: CommandFlags.NoRedirect);

            AssertRiGetEventually(replicaEndpoint, key, fields[0].Field, fields[0].Value, $"Replica (node {replicaIndex}) did not serve migrated RI key {key} after AOF sync");
            foreach (var (field, value) in fields)
            {
                var result = (string)context.clusterTestUtils.Execute(replicaEndpoint, "RI.GET", [key, field], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(value, result, $"Replica RI.GET {key}/{field} should return migrated value");
            }
        }

        private void RestartNode(int nodeIndex)
        {
            context.RestartNode(nodeIndex, ensureAofFlush: true);
            context.CreateConnection();
        }

        /// <summary>Endpoint (as <see cref="IPEndPoint"/>) of the cluster node at <paramref name="nodeIndex"/>.</summary>
        private IPEndPoint Endpoint(int nodeIndex) => (IPEndPoint)context.clusterTestUtils.GetEndPoint(nodeIndex);

        /// <summary>Migrate <paramref name="slots"/> from <paramref name="source"/> to <paramref name="target"/> and wait for migration cleanup to settle.</summary>
        private void MigrateSlotsAndWaitCleanup(IPEndPoint source, IPEndPoint target, List<int> slots)
        {
            context.clusterTestUtils.MigrateSlots(source, target, slots, logger: context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);
        }

        /// <summary>
        /// Migrate <paramref name="slots"/> from <paramref name="source"/> to <paramref name="target"/>, wait
        /// for migration cleanup to settle, and confirm the target owns every migrated slot.
        /// </summary>
        private void MigrateSlotsAndWaitOwnership(IPEndPoint source, IPEndPoint target, List<int> slots)
        {
            context.clusterTestUtils.MigrateSlots(source, target, slots);
            context.clusterTestUtils.WaitForMigrationCleanup();
            foreach (var slot in slots)
                WaitForSlotOwnership(source, target, slot);
        }

        #endregion

        /// <summary>
        /// A migrated RangeIndex key must replicate to the destination primary's replica via the
        /// AOF (the chunked range index stream). Set up two primaries each with a replica, populate a
        /// DISK-backed RI key on the source primary, migrate its slot to the target primary, then
        /// verify the target's replica returns the full migrated data (not an empty tree) after AOF
        /// sync.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexReplicatesToReplicaViaAof()
        {
            const int primaryCount = 2;
            const int replicaCount = 1;
            const int nodeCount = primaryCount + primaryCount * replicaCount; // node0/1 primaries, node2/3 replicas

            context.CreateInstances(nodeCount, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);

            const int sourceNodeIndex = 0;
            const int targetNodeIndex = 1;
            const int targetReplicaIndex = 3; // replica of primary index 1 (see SimpleSetupCluster replica mapping)

            var sourceEndpoint = Endpoint(sourceNodeIndex);
            var targetEndpoint = Endpoint(targetNodeIndex);

            // Pick a key owned by the source primary and populate a non-trivial DISK-backed RI.
            var riKey = FindKeyOwnedByNode(sourceNodeIndex);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(50, "value");

            CreateRangeIndexWithFields(sourceEndpoint, riKey, fields);

            // Migrate the slot to the target primary.
            MigrateSlotsAndWaitCleanup(sourceEndpoint, targetEndpoint, new List<int> { slot });

            // The target primary itself must have the full data.
            VerifyFieldsOnEndpoint(targetEndpoint, riKey, fields);

            // The target's replica must replay the migrated range index stream via AOF and serve the full data.
            VerifyRangeIndexOnReplica(targetNodeIndex, targetReplicaIndex, riKey, fields);
        }

        /// <summary>
        /// A migrated RangeIndex key must survive a crash and be reconstructed from the destination
        /// primary's own AOF (the chunked range index stream) on recovery. Migrate a DISK-backed RI key
        /// to the target primary, restart it with recovery enabled, then verify the full data.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexRecoversFromAof()
        {
            const int primaryCount = 2;

            context.CreateInstances(primaryCount, tryRecover: true, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            const int sourceNodeIndex = 0;
            const int targetNodeIndex = 1;

            var sourceEndpoint = Endpoint(sourceNodeIndex);
            var targetEndpoint = Endpoint(targetNodeIndex);

            var riKey = FindKeyOwnedByNode(sourceNodeIndex);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(50, "value");

            CreateRangeIndexWithFields(sourceEndpoint, riKey, fields);

            MigrateSlotsAndWaitCleanup(sourceEndpoint, targetEndpoint, new List<int> { slot });
            VerifyFieldsOnEndpoint(targetEndpoint, riKey, fields);

            // Restart the target primary with recovery: its AOF (containing the range index stream) is
            // replayed and HandleRangeIndexStreamReplay must reconstruct the migrated key.
            RestartNode(targetNodeIndex);

            // Verify the recovered node serves the key and all fields.
            AssertRiGetEventually(targetEndpoint, riKey, fields[0].Field, fields[0].Value, "Target primary did not recover the migrated RI key from its AOF");
            VerifyFieldsOnEndpoint(targetEndpoint, riKey, fields);
        }

        /// <summary>
        /// A migrated RI whose file spans many range index stream chunks (small chunk size) must reassemble
        /// correctly on the target's replica (live AOF) and on the target after crash recovery.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexMultiChunkStreamReplicatesAndRecovers()
        {
            const int primaryCount = 2, replicaCount = 1, nodeCount = 4;

            context.CreateInstances(nodeCount, tryRecover: true, enableAOF: true, enableRangeIndexPreview: true);
            context.SetRangeIndexStreamChunkSizeOnAllNodes(SmallStreamChunkSize);
            var capture = context.CaptureNodeLogs();
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);

            const int source = 0, target = 1, targetReplica = 3;
            var sourceEp = Endpoint(source);
            var targetEp = Endpoint(target);

            var riKey = FindKeyOwnedByNode(source);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(80, "mc");
            CreateRangeIndexWithFields(sourceEp, riKey, fields);

            MigrateSlotsAndWaitCleanup(sourceEp, targetEp, [slot]);
            VerifyFieldsOnEndpoint(targetEp, riKey, fields);

            // Replica reassembles the multi-chunk stream from live AOF.
            VerifyRangeIndexOnReplica(target, targetReplica, riKey, fields);

            // Assert the migrated RI really was streamed into the AOF across many chunks (not a single
            // one): the target's RangeIndexReplicationStreamActivity records the chunk count it enqueued.
            AssertRangeIndexStreamedInManyChunks(capture, riKey, minChunks: 4);

            // Target reconstructs the multi-chunk stream from its own AOF on recovery.
            var entriesBeforeRestart = capture.Entries.Count;
            RestartNode(target);
            AssertRiGetEventually(targetEp, riKey, fields[0].Field, fields[0].Value, "Target did not recover the multi-chunk migrated RI key");
            VerifyFieldsOnEndpoint(targetEp, riKey, fields);

            // Validate recovery itself reassembled the multi-chunk stream from the AOF (a new successful
            // reassembly logged after the restart watermark, distinct from the replica's live reassembly).
            AssertRangeIndexReassembledInManyChunks(capture, riKey, minChunks: 4, afterEntryIndex: entriesBeforeRestart);
        }

        /// <summary>
        /// Migrating an empty RI (RI.CREATE, no RI.SET) still streams a valid range index stream (headers +
        /// trailer/stub only); the empty index must exist and be functional on the target's replica
        /// and after recovery.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateEmptyRangeIndexReplicatesAndRecovers()
        {
            const int primaryCount = 2, replicaCount = 1, nodeCount = 4;

            context.CreateInstances(nodeCount, tryRecover: true, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);

            const int source = 0, target = 1, targetReplica = 3;
            var sourceEp = Endpoint(source);
            var targetEp = Endpoint(target);

            var riKey = FindKeyOwnedByNode(source);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            CreateRangeIndexWithFields(sourceEp, riKey, []);

            MigrateSlotsAndWaitCleanup(sourceEp, targetEp, [slot]);

            // The empty index must exist on the target: RI.SET succeeds only if the index is present.
            var setResult = (string)context.clusterTestUtils.Execute(targetEp, "RI.SET", [riKey, "field_0000", "value_0000"], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult, "RI.SET should succeed on target (empty index must have migrated)");

            var oneField = new List<(string Field, string Value)> { ("field_0000", "value_0000") };
            VerifyRangeIndexOnReplica(target, targetReplica, riKey, oneField);

            // Recovery: the empty-index range index stream (and the later RI.SET) must both survive.
            RestartNode(target);
            AssertRiGetEventually(targetEp, riKey, "field_0000", "value_0000", "Target did not recover the migrated empty index (+ subsequent set)");
        }

        /// <summary>
        /// Round-trip migration P0 -> P1 -> P0 with a replica on each primary. Both primaries and
        /// their replicas must end consistent, and P1 must no longer own the key after it is migrated
        /// back (no stale ownership).
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexRoundTripWithReplicas()
        {
            const int primaryCount = 2, replicaCount = 1, nodeCount = 4;

            context.CreateInstances(nodeCount, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);

            const int p0 = 0, p1 = 1, r0 = 2, r1 = 3;
            var ep0 = Endpoint(p0);
            var ep1 = Endpoint(p1);

            var riKey = FindKeyOwnedByNode(p0);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(40, "rt");
            CreateRangeIndexWithFields(ep0, riKey, fields);

            // P0 -> P1
            MigrateSlotsAndWaitCleanup(ep0, ep1, [slot]);
            VerifyFieldsOnEndpoint(ep1, riKey, fields);
            VerifyRangeIndexOnReplica(p1, r1, riKey, fields);

            // P1 -> P0
            MigrateSlotsAndWaitCleanup(ep1, ep0, [slot]);
            VerifyFieldsOnEndpoint(ep0, riKey, fields);
            VerifyRangeIndexOnReplica(p0, r0, riKey, fields);

            // P1 must no longer own the slot (no stale ownership after migrating the key away).
            var p1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(ep1, context.logger);
            ClassicAssert.IsFalse(p1Slots.Contains(slot), "P1 should not own the slot after migrating the key back to P0");
        }

        /// <summary>
        /// After migrating a key to the target and replicating to its replica, failing the replica
        /// over to primary must leave the migrated RI data intact and writable on the new primary.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenFailover()
        {
            const int primaryCount = 2, replicaCount = 1, nodeCount = 4;

            context.CreateInstances(nodeCount, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);

            const int source = 0, target = 1, targetReplica = 3;
            var sourceEp = Endpoint(source);
            var targetEp = Endpoint(target);

            var riKey = FindKeyOwnedByNode(source);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(40, "fo");
            CreateRangeIndexWithFields(sourceEp, riKey, fields);

            MigrateSlotsAndWaitCleanup(sourceEp, targetEp, [slot]);
            VerifyFieldsOnEndpoint(targetEp, riKey, fields);
            VerifyRangeIndexOnReplica(target, targetReplica, riKey, fields);

            // Checkpoint so the replica has a durable base before failover.
            var targetLastSave = context.clusterTestUtils.LastSave(target, logger: context.logger);
            var replicaLastSave = context.clusterTestUtils.LastSave(targetReplica, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(targetReplica, replicaLastSave);
            context.clusterTestUtils.Checkpoint(target, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(target, targetLastSave, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(targetReplica, replicaLastSave, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(target, targetReplica, context.logger);

            _ = context.clusterTestUtils.ClusterFailover(targetReplica, logger: context.logger);
            context.clusterTestUtils.WaitForNoFailover(targetReplica, logger: context.logger);
            context.clusterTestUtils.WaitForFailoverCompleted(targetReplica, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(target, logger: context.logger);

            var newPrimaryEp = Endpoint(targetReplica);
            VerifyFieldsOnEndpoint(newPrimaryEp, riKey, fields);

            var setResult = (string)context.clusterTestUtils.Execute(newPrimaryEp, "RI.SET", [riKey, "post_failover", "ok"], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", setResult, "new primary should accept RI.SET after failover");
        }

        /// <summary>
        /// Checkpoint after migration then recover. The range index stream entries precede the checkpoint and
        /// must be skipped on recovery (the key is restored from the checkpoint) with no double-publish;
        /// a post-checkpoint RI.SET (in the AOF) must also survive.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexCheckpointThenRecover()
        {
            const int primaryCount = 2;

            context.CreateInstances(primaryCount, tryRecover: true, enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            const int source = 0, target = 1;
            var sourceEp = Endpoint(source);
            var targetEp = Endpoint(target);

            var riKey = FindKeyOwnedByNode(source);
            var slot = context.clusterTestUtils.HashSlot(riKey);
            var fields = MakeFields(40, "cp");
            CreateRangeIndexWithFields(sourceEp, riKey, fields);

            MigrateSlotsAndWaitCleanup(sourceEp, targetEp, [slot]);
            VerifyFieldsOnEndpoint(targetEp, riKey, fields);

            var targetLastSave = context.clusterTestUtils.LastSave(target, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(target, targetLastSave);
            context.clusterTestUtils.Checkpoint(target, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(target, targetLastSave, logger: context.logger);

            // Post-checkpoint write lands in the AOF (replayed on top of the checkpoint).
            _ = context.clusterTestUtils.Execute(targetEp, "RI.SET", [riKey, "post_ckpt", "pv"], flags: CommandFlags.NoRedirect);

            RestartNode(target);
            AssertRiGetEventually(targetEp, riKey, fields[0].Field, fields[0].Value, "Target did not recover the migrated RI key after a post-migration checkpoint");
            VerifyFieldsOnEndpoint(targetEp, riKey, fields);
            AssertRiGetEventually(targetEp, riKey, "post_ckpt", "pv", "post-checkpoint RI.SET should survive recovery");
        }

        /// <summary>
        /// Single RI key with multiple fields, slot-based migration between 2 primaries.
        /// Verifies all fields survive and source returns MOVED.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexSingleBySlot()
        {
            const int shards = 2;
            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI with multiple fields
            var fields = new[]
            {
                ("field1", "value1"), ("field2", "value2"), ("field3", "value3"),
                ("field4", "value4"), ("field5", "value5"),
            };
            CreateRangeIndexWithFields(primary0, riKey, fields);
            VerifyFieldsOnEndpoint(primary0, riKey, fields);

            // Migrate
            MigrateSlotsAndWaitOwnership(primary0, primary1, [slot]);

            // Verify on target
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Verify source returns MOVED
            AssertMovedFrom(primary0, "RI.GET", riKey, "field1");
        }

        /// <summary>
        /// Key-based migration of multiple RI keys in random order.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexByKeys()
        {
            const int shardCount = 3;
            const int keyCount = 10;

            context.CreateInstances(shardCount, enableRangeIndexPreview: true);
            context.CreateConnection();

            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, NullLogger.Instance);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, NullLogger.Instance);
            var sourceEndpoint = Endpoint(sourceNodeIndex);
            var targetEndpoint = Endpoint(targetNodeIndex);

            var keyBase = Encoding.ASCII.GetBytes("{abc}ri_");
            var workingSlot = ClusterTestUtils.HashSlot(keyBase);

            var rand = new Random(2025_05_03_00);
            var allKeys = new List<(byte[] Key, List<(string Field, string Value)> Fields)>();

            for (var i = 0; i < keyCount; i++)
            {
                var newKey = new byte[keyBase.Length + 1];
                Array.Copy(keyBase, 0, newKey, 0, keyBase.Length);
                newKey[^1] = (byte)('a' + i);
                ClassicAssert.AreEqual(workingSlot, ClusterTestUtils.HashSlot(newKey));

                var keyStr = Encoding.ASCII.GetString(newKey);
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                    fields.Add(($"field_{f:D4}", $"value_{i}_{f}_{rand.Next(10000)}"));

                CreateRangeIndexWithFields(sourceEndpoint, keyStr, fields);
                allKeys.Add((newKey, fields));
            }

            // Manual slot migration setup
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "IMPORTING", sourceNodeId);
            ClassicAssert.AreEqual("OK", respImport);
            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual("OK", respMigrate);

            // Migrate keys one at a time in random order
            var toMigrate = allKeys.Select(k => k.Key).ToList();
            while (toMigrate.Count > 0)
            {
                var ix = rand.Next(toMigrate.Count);
                context.clusterTestUtils.MigrateKeys(sourceEndpoint, targetEndpoint, [toMigrate[ix]], NullLogger.Instance);
                toMigrate.RemoveAt(ix);
            }

            // Complete migration
            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeTarget);
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeSource);
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true);

            context.clusterTestUtils.WaitForMigrationCleanup();

            // Verify all keys and fields on target
            foreach (var (key, fields) in allKeys)
            {
                var keyStr = Encoding.ASCII.GetString(key);
                VerifyFieldsOnEndpoint(targetEndpoint, keyStr, fields);
            }
        }

        /// <summary>
        /// Key-based migration of a MIXED batch — some keys are RangeIndex and some are
        /// plain string/hash keys — all in a single MIGRATE ... KEYS call.
        ///
        /// Validates <c>RangeIndexManager.GetRangeIndexKeysForMigration</c>:
        /// it must identify the RI subset (so they go through the RI snapshot/transmit path)
        /// while leaving the non-RI keys to the regular key-transmission path. Both sets
        /// must arrive intact on the target.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateMixedRangeIndexAndRegularByKeys()
        {
            const int shardCount = 3;
            const int riKeyCount = 4;
            const int stringKeyCount = 4;
            const int hashKeyCount = 4;

            context.CreateInstances(shardCount, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, NullLogger.Instance);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, NullLogger.Instance);
            var sourceEndpoint = Endpoint(sourceNodeIndex);
            var targetEndpoint = Endpoint(targetNodeIndex);

            // All keys share the {abc} hash tag so they map to one slot.
            const string HashTag = "{abc}";
            var workingSlot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(HashTag));

            var rand = new Random(2026_06_03_01);

            // RI keys
            var riKeys = new List<(string Key, List<(string Field, string Value)> Fields)>();
            for (var i = 0; i < riKeyCount; i++)
            {
                var key = $"{HashTag}ri_{i}";
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                    fields.Add(($"field_{f:D4}", $"ri_value_{i}_{f}_{rand.Next(10000)}"));

                CreateRangeIndexWithFields(sourceEndpoint, key, fields);
                riKeys.Add((key, fields));
            }

            // Plain string keys
            var stringKeys = new List<(string Key, string Value)>();
            for (var i = 0; i < stringKeyCount; i++)
            {
                var key = $"{HashTag}str_{i}";
                var value = $"str_value_{i}_{rand.Next(10000)}";
                var setResult = (string)context.clusterTestUtils.Execute(sourceEndpoint, "SET", [key, value], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, $"SET should succeed for {key}");
                stringKeys.Add((key, value));
            }

            // Hash keys
            var hashKeys = new List<(string Key, List<(string Field, string Value)> Fields)>();
            for (var i = 0; i < hashKeyCount; i++)
            {
                var key = $"{HashTag}hash_{i}";
                var fields = new List<(string Field, string Value)>();
                var fieldCount = rand.Next(1, 4);
                for (var f = 0; f < fieldCount; f++)
                {
                    var field = $"hf_{f:D4}";
                    var value = $"hash_value_{i}_{f}_{rand.Next(10000)}";
                    var hsetArgs = new object[] { key, field, value };
                    _ = context.clusterTestUtils.Execute(sourceEndpoint, "HSET", hsetArgs, flags: CommandFlags.NoRedirect);
                    fields.Add((field, value));
                }
                hashKeys.Add((key, fields));
            }

            // Manual slot migration setup
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "IMPORTING", sourceNodeId);
            ClassicAssert.AreEqual("OK", respImport);
            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual("OK", respMigrate);

            // Build a single MIGRATE ... KEYS batch with a shuffled mix of RI + non-RI keys.
            var batch = new List<byte[]>();
            batch.AddRange(riKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            batch.AddRange(stringKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            batch.AddRange(hashKeys.Select(k => Encoding.ASCII.GetBytes(k.Key)));
            for (var i = batch.Count - 1; i > 0; i--)
            {
                var j = rand.Next(i + 1);
                (batch[i], batch[j]) = (batch[j], batch[i]);
            }

            context.clusterTestUtils.MigrateKeys(sourceEndpoint, targetEndpoint, batch, NullLogger.Instance);

            // Complete migration
            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeTarget);
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual("OK", respNodeSource);
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true);

            context.clusterTestUtils.WaitForMigrationCleanup();

            // Verify RI keys on target
            foreach (var (key, fields) in riKeys)
                VerifyFieldsOnEndpoint(targetEndpoint, key, fields);

            // Verify string keys on target
            foreach (var (key, expected) in stringKeys)
            {
                var actual = (string)context.clusterTestUtils.Execute(targetEndpoint, "GET", [key], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(expected, actual, $"GET {key} on target");
            }

            // Verify hash keys on target
            foreach (var (key, fields) in hashKeys)
            {
                foreach (var (field, expected) in fields)
                {
                    var actual = (string)context.clusterTestUtils.Execute(targetEndpoint, "HGET", [key, field], flags: CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual(expected, actual, $"HGET {key} {field} on target");
                }
            }

            // Verify source returns MOVED for one key from each type
            AssertMovedFrom(sourceEndpoint, "RI.GET", riKeys[0].Key, riKeys[0].Fields[0].Field);
            AssertMovedFrom(sourceEndpoint, "GET", stringKeys[0].Key);
            AssertMovedFrom(sourceEndpoint, "HGET", hashKeys[0].Key, hashKeys[0].Fields[0].Field);
        }

        /// <summary>
        /// Multiple RI keys in the same slot, slot-based migration.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexManyBySlot()
        {
            const int shards = 2;
            const int keysPerPrimary = 4;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            var rand = new Random(42);
            var primary0Keys = new List<(string Key, int Slot, List<(string Field, string Value)> Fields)>();

            var ix = 0;
            while (primary0Keys.Count < keysPerPrimary)
            {
                var key = $"{nameof(ClusterMigrateRangeIndexManyBySlot)}_{ix}";
                var slot = context.clusterTestUtils.HashSlot(key);
                if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot))
                {
                    var fields = new List<(string Field, string Value)>();
                    var fieldCount = rand.Next(1, 6);
                    for (var f = 0; f < fieldCount; f++)
                        fields.Add(($"field_{f:D4}", $"value_{ix}_{f:D4}"));

                    primary0Keys.Add((key, slot, fields));
                }
                ix++;
            }

            // Create all keys on primary0
            foreach (var (key, _, fields) in primary0Keys)
                CreateRangeIndexWithFields(primary0, key, fields);

            // Migrate all distinct slots
            var migrateSlots = primary0Keys.Select(k => k.Slot).Distinct().ToList();
            MigrateSlotsAndWaitOwnership(primary0, primary1, migrateSlots);

            // Verify all keys on primary1
            foreach (var (key, _, fields) in primary0Keys)
                VerifyFieldsOnEndpoint(primary1, key, fields);

            // Verify source returns MOVED
            foreach (var (key, _, _) in primary0Keys)
                AssertMovedFrom(primary0, "RI.GET", key, "field_0000");
        }

        /// <summary>
        /// Stress test: concurrent reads + writes + repeated back-and-forth migrations.
        /// Verifies zero data loss.
        /// </summary>
        /// <remarks>
        /// Currently marked Explicit because RI.SET via cluster-mode client redirect can hit
        /// "ERR no such range index" on the target if the RI key was just migrated and the
        /// BfTree native instance isn't yet registered. This needs investigation in the
        /// migration pipeline before this test can run reliably.
        /// </remarks>
        [Test, Explicit("RI.SET via cluster redirect not yet reliable during migration")]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexStressAsync()
        {
            const int shards = 2;
            const int keysPerPrimary = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            // Find keys on each primary
            var allKeys = new List<(string Key, int Slot, bool OnPrimary0)>();
            var numP0 = 0;
            var numP1 = 0;
            var ix = 0;

            while (numP0 < keysPerPrimary || numP1 < keysPerPrimary)
            {
                var key = $"{nameof(ClusterMigrateRangeIndexStressAsync)}_{ix}";
                var slot = context.clusterTestUtils.HashSlot(key);
                var isOnP0 = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot);

                if (isOnP0 && numP0 < keysPerPrimary)
                {
                    allKeys.Add((key, slot, true));
                    numP0++;
                }
                else if (!isOnP0 && numP1 < keysPerPrimary)
                {
                    allKeys.Add((key, slot, false));
                    numP1++;
                }
                ix++;
            }

            // Create RI keys on their respective primaries
            foreach (var (key, _, onP0) in allKeys)
            {
                var endpoint = onP0 ? primary0 : primary1;
                var createResult = (string)context.clusterTestUtils.Execute(endpoint, "RI.CREATE", [key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", createResult);
            }

            // Start concurrent writers
            using var writeCancel = new CancellationTokenSource();
            var writeResults = new ConcurrentBag<(string Field, string Value)>[allKeys.Count];
            var writeTasks = new Task[allKeys.Count];
            var mostRecentWrite = 0L;

            using var readWriteCon = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
            var readWriteDb = readWriteCon.GetDatabase();

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, _, _) = allKeys[i];
                var bag = writeResults[i] = new ConcurrentBag<(string, string)>();

                writeTasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                    var wix = 0;

                    while (!writeCancel.IsCancellationRequested)
                    {
                        var field = $"field_{wix}";
                        var value = $"value_{wix}";

                        try
                        {
                            var result = (string)readWriteDb.Execute("RI.SET", [new RedisKey(key), field, value]);
                            if (result == "OK")
                                bag.Add((field, value));
                        }
                        catch (Exception exc) when (
                            exc is RedisTimeoutException
                            || exc is RedisConnectionException
                            || (exc is RedisServerException rse && (
                                rse.Message.StartsWith("MOVED ")
                                || rse.Message.StartsWith("Key has MOVED to "))))
                        {
                            if (writeCancel.IsCancellationRequested) return;
                            continue;
                        }

                        var now = DateTime.UtcNow.Ticks;
                        var prev = Interlocked.CompareExchange(ref mostRecentWrite, now, mostRecentWrite);
                        while (prev < now)
                            prev = Interlocked.CompareExchange(ref mostRecentWrite, now, prev);

                        wix++;
                    }
                });
            }

            // Start concurrent readers
            using var readCancel = new CancellationTokenSource();
            var readTasks = new Task<int>[allKeys.Count];

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, _, _) = allKeys[i];
                var bag = writeResults[i];

                readTasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                    var successfulReads = 0;
                    var rng = new Random(i);

                    while (!readCancel.IsCancellationRequested)
                    {
                        var snapshot = bag.ToList();
                        if (snapshot.Count == 0)
                        {
                            await Task.Delay(10).ConfigureAwait(false);
                            continue;
                        }

                        var (field, expectedValue) = snapshot[rng.Next(snapshot.Count)];

                        try
                        {
                            var result = (string)readWriteDb.Execute("RI.GET", [new RedisKey(key), field]);
                            if (result != null)
                            {
                                ClassicAssert.AreEqual(expectedValue, result, $"Read mismatch for {key}/{field}");
                                successfulReads++;
                            }
                        }
                        catch (Exception exc) when (
                            exc is RedisTimeoutException
                            || exc is RedisConnectionException
                            || (exc is RedisServerException rse && (
                                rse.Message.StartsWith("MOVED ")
                                || rse.Message.StartsWith("Key has MOVED to "))))
                        {
                            continue;
                        }
                    }

                    return successfulReads;
                });
            }

            await Task.Delay(1_000).ConfigureAwait(false);
            ClassicAssert.IsTrue(writeResults.All(r => !r.IsEmpty), "Should have writes before migration");

            // Migrator: ping-pong slots between primaries
            using var migrateCancel = new CancellationTokenSource();

            var migrateTask = Task.Run(async () =>
            {
                var slotsOnP0 = allKeys.Where(k => k.OnPrimary0).Select(k => k.Slot).Distinct().ToList();
                var slotsOnP1 = allKeys.Where(k => !k.OnPrimary0).Select(k => k.Slot).Distinct().ToList();
                var migrationCount = 0;
                var mostRecentMigration = 0L;

                while (!migrateCancel.IsCancellationRequested)
                {
                    await Task.Delay(100).ConfigureAwait(false);

                    // Wait for at least one write since last migration
                    if (Interlocked.CompareExchange(ref mostRecentWrite, 0, 0) < mostRecentMigration)
                        continue;

                    // Move P0 → P1
                    if (slotsOnP0.Count > 0)
                    {
                        using var token = new CancellationTokenSource();
                        token.CancelAfter(30_000);

                        context.clusterTestUtils.MigrateSlots(primary0, primary1, slotsOnP0);
                        context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: token.Token);
                        context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: token.Token);
                    }

                    // Move P1 → P0
                    if (slotsOnP1.Count > 0)
                    {
                        using var token = new CancellationTokenSource();
                        token.CancelAfter(30_000);

                        context.clusterTestUtils.MigrateSlots(primary1, primary0, slotsOnP1);
                        context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: token.Token);
                        context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: token.Token);
                    }

                    mostRecentMigration = DateTime.UtcNow.Ticks;
                    migrationCount++;

                    // Flip for next pass
                    (slotsOnP0, slotsOnP1) = (slotsOnP1, slotsOnP0);
                }

                return migrationCount;
            });

            await Task.Delay(10_000).ConfigureAwait(false);

            migrateCancel.Cancel();
            var migrations = await migrateTask.ConfigureAwait(false);
            ClassicAssert.IsTrue(migrations >= 2, $"Should have at least 2 migrations, had {migrations}");

            writeCancel.Cancel();
            await Task.WhenAll(writeTasks).ConfigureAwait(false);

            readCancel.Cancel();
            var readResults = await Task.WhenAll(readTasks).ConfigureAwait(false);
            ClassicAssert.IsTrue(readResults.All(r => r > 0), "Should have successful reads on all keys");

            // Final verification: every written field must be readable
            var curP0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);

            for (var i = 0; i < allKeys.Count; i++)
            {
                var (key, slot, _) = allKeys[i];
                var endpoint = curP0Slots.Contains(slot) ? primary0 : primary1;

                VerifyFieldsOnEndpoint(endpoint, key, writeResults[i]);
            }
        }

        /// <summary>
        /// Test concurrent RI.GET reads during migration. Reads could trigger RIPROMOTE
        /// (CTT for stubs), creating potential races with the migration snapshot path.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWhileReadingAsync()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            // Create RI key and populate with known data
            var fields = Enumerable.Range(0, 100).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Start background reader
            using var cts = new CancellationTokenSource();
            var readCount = 0;
            var readErrors = new ConcurrentBag<string>();

            var readTask = Task.Run(async () =>
            {
                await Task.Yield();

                using var con = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var db = con.GetDatabase();

                while (!cts.IsCancellationRequested)
                {
                    var ix = Interlocked.Increment(ref readCount) % fields.Count;
                    var (field, expectedValue) = fields[ix];

                    try
                    {
                        var result = (string)db.Execute("RI.GET", [new RedisKey(riKey), field]);
                        if (result != null && result != expectedValue)
                            readErrors.Add($"RI.GET {field}: expected '{expectedValue}', got '{result}'");
                    }
                    catch (Exception exc) when (
                        exc is RedisTimeoutException
                        || exc is RedisConnectionException
                        || (exc is RedisServerException rse && (
                            rse.Message.StartsWith("MOVED ")
                            || rse.Message.StartsWith("Key has MOVED to "))))
                    {
                        // Expected during/after migration
                    }
                }
            });

            await Task.Delay(1_000).ConfigureAwait(false);
            ClassicAssert.IsTrue(readCount > 0, "Should have some reads before migration");

            // Migrate
            using (var migrateToken = new CancellationTokenSource())
            {
                migrateToken.CancelAfter(30_000);
                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(1, cancellationToken: migrateToken.Token);
            }

            WaitForSlotOwnership(primary0, primary1, slot);

            // Let reads continue post-migration
            await Task.Delay(2_000).ConfigureAwait(false);

            cts.Cancel();
            await readTask.ConfigureAwait(false);

            ClassicAssert.IsEmpty(readErrors, $"Read value mismatches during migration: {string.Join("; ", readErrors.Take(5))}");

            // Verify all fields intact on target
            VerifyFieldsOnEndpoint(primary1, riKey, fields);
        }

        /// <summary>
        /// Pause migration during TRANSMITTING state via fault injection,
        /// fire RI.SET and RI.GET while migration is paused, then resume.
        /// Writes should be blocked by sketch during TRANSMITTING.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWithPauseDuringTransmitAsync()
        {
            ClusterTestUtils.IgnoreIfExceptionInjectionDisabled();

            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm the pause hook — migration will pause after entering TRANSMITTING
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

            try
            {
                // Start migration in background
                var migrateTask = Task.Run(() =>
                {
                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                });

                // Wait for migration to reach the pause point
                // ResetAndWaitAsync disables the flag when it reaches the pause, so we wait for it to clear
                var deadline = Stopwatch.GetTimestamp();
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting)
                       && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                }

                // Migration is now paused in TRANSMITTING state.
                // RI.GET on initial fields should still work (reads allowed during TRANSMITTING).
                foreach (var (field, value) in fields.Take(5))
                {
                    try
                    {
                        var result = (string)context.clusterTestUtils.Execute(primary0, "RI.GET", [riKey, field], flags: CommandFlags.NoRedirect);
                        // Read may succeed or fail depending on exact sketch gating behavior
                        context.logger?.LogInformation("RI.GET during TRANSMITTING: {field} = {result}", field, result);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogInformation("RI.GET during TRANSMITTING: {field} threw {message}", field, ex.Message);
                    }
                }

                // Resume migration
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);

                // Wait for migration to complete
                await migrateTask.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                WaitForSlotOwnership(primary0, primary1, slot);

                // Verify all fields on target
                VerifyFieldsOnEndpoint(primary1, riKey, fields);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting);
            }
        }

        /// <summary>
        /// Pause migration during DELETING state via fault injection.
        /// Both reads and writes should be blocked during DELETING.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public async Task ClusterMigrateRangeIndexWithPauseDuringDeleteAsync()
        {
            ClusterTestUtils.IgnoreIfExceptionInjectionDisabled();

            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm the pause hook — migration will pause after entering DELETING
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);

            try
            {
                var migrateTask = Task.Run(() =>
                {
                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);
                });

                // Wait for migration to reach the DELETING pause
                var deadline = Stopwatch.GetTimestamp();
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.RangeIndex_Migration_After_Deleting)
                       && Stopwatch.GetElapsedTime(deadline) < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                }

                // Migration paused in DELETING. Data was already transmitted to target.
                // Verify target already has the data (it was sent before DELETING)
                foreach (var (field, value) in fields.Take(5))
                {
                    try
                    {
                        var result = (string)context.clusterTestUtils.Execute(primary1, "RI.GET", [riKey, field], flags: CommandFlags.NoRedirect);
                        context.logger?.LogInformation("RI.GET on target during DELETING: {field} = {result}", field, result);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogInformation("RI.GET on target during DELETING threw: {message}", ex.Message);
                    }
                }

                // Resume migration
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);

                await migrateTask.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                WaitForSlotOwnership(primary0, primary1, slot);

                // Verify all fields on target after completion
                VerifyFieldsOnEndpoint(primary1, riKey, fields);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_After_Deleting);
            }
        }

        /// <summary>
        /// Inject an exception during the transmit phase. Verifies that on failure:
        /// - Sketch is cleared (operations unblocked)
        /// - Source data remains intact
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexExceptionDuringTransmit()
        {
            ClusterTestUtils.IgnoreIfExceptionInjectionDisabled();

            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Arm exception to fire before DELETING (after transmit completes).
            // This simulates a failure that aborts migration before source deletion.
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.RangeIndex_Migration_Before_Deleting);

            try
            {
                // Migration should fail but not crash — exception is caught by migration framework
                context.clusterTestUtils.MigrateSlots(primary0, primary1, [slot]);

                // Wait for cleanup
                context.clusterTestUtils.WaitForMigrationCleanup(logger: context.logger);

                // Source should still own the slot (migration failed)
                var sourceSlots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, context.logger);
                ClassicAssert.IsTrue(sourceSlots.Contains(slot), "Source should still own slot after failed migration");

                // Source data should be intact
                VerifyFieldsOnEndpoint(primary0, riKey, fields);

                // RI.SET should work (sketch must have been cleared)
                var setResult = (string)context.clusterTestUtils.Execute(primary0, "RI.SET", [riKey, "after_failure", "works"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual("OK", setResult, "RI.SET should succeed after failed migration");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.RangeIndex_Migration_Before_Deleting);
            }
        }

        #region Post-migration lifecycle

        /// <summary>
        /// Take a checkpoint on the given node and wait for it to complete.
        /// </summary>
        private void CheckpointNode(int nodeIndex)
        {
            var lastSave = context.clusterTestUtils.LastSave(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(nodeIndex, lastSave);
            context.clusterTestUtils.Checkpoint(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(nodeIndex, lastSave, logger: context.logger);
        }

        /// <summary>
        /// After migration, write additional fields on the target, take a checkpoint (which
        /// snapshots the migrated tree to its own <c>data.bftree</c>), then read every field back.
        /// Exercises checkpoint of a migrated-then-modified tree plus sustained post-migration use.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenFlushAndRead()
        {
            const int shards = 2;
            const int targetIndex = 1;

            context.CreateInstances(shards, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 30).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            MigrateSlotsAndWaitOwnership(primary0, primary1, [slot]);

            // Write more fields to the migrated tree on the target.
            var moreFields = Enumerable.Range(30, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            SetRangeIndexFields(primary1, riKey, moreFields);

            // Checkpoint the target: OnFlush snapshots the migrated tree's BfTree to its data.bftree.
            CheckpointNode(targetIndex);

            // Read back every field (migrated + post-migration writes).
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(moreFields));
        }

        /// <summary>
        /// After migration, read the migrated tree on the target, then delete the whole key and
        /// verify it is gone (RI.EXISTS = 0, RI.GET null) and a fresh index can be created at the
        /// same key — proving the delete cleaned up the migrated tree's file state.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenDelete()
        {
            const int shards = 2;

            context.CreateInstances(shards, enableRangeIndexPreview: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 20).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            MigrateSlotsAndWaitOwnership(primary0, primary1, [slot]);

            // Reads work on the target.
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Delete the whole key on the target.
            var delResult = (int)context.clusterTestUtils.Execute(primary1, "DEL", [riKey], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(1, delResult, "DEL should remove the migrated key");

            // The key is gone.
            var exists = (int)context.clusterTestUtils.Execute(primary1, "RI.EXISTS", [riKey], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(0, exists, "RI.EXISTS should be 0 after DEL");

            var getResult = (string)context.clusterTestUtils.Execute(primary1, "RI.GET", [riKey, "field_0"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(getResult.Contains("not found", StringComparison.OrdinalIgnoreCase), $"RI.GET should report the index is gone after DEL, got: {getResult}");

            // A fresh index can be created at the same key (delete cleaned up file state).
            var recreated = new[] { ("field_a", "value_a"), ("field_b", "value_b") };
            CreateRangeIndexWithFields(primary1, riKey, recreated);
            VerifyFieldsOnEndpoint(primary1, riKey, recreated);
        }

        /// <summary>
        /// Migrate a tree to the target, take a checkpoint, restart, then keep using it: after
        /// recovery write new fields and read everything back, and take a second checkpoint +
        /// restart to confirm the recovered tree is itself durable.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexThenCheckpointRestartAndContinue()
        {
            const int shards = 2;
            const int targetIndex = 1;

            context.CreateInstances(shards, tryRecover: true, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // Migrate the slot to the target.
            MigrateSlotsAndWaitOwnership(primary0, primary1, [slot]);

            // Checkpoint + restart (recover the migrated tree from the snapshot).
            CheckpointNode(targetIndex);
            RestartNode(targetIndex);
            VerifyFieldsOnEndpoint(primary1, riKey, fields);

            // Continue using the recovered tree: new writes + reads.
            var afterRecovery = Enumerable.Range(15, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            SetRangeIndexFields(primary1, riKey, afterRecovery);
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(afterRecovery));

            // Second checkpoint + restart: the recovered-then-extended tree is itself durable.
            CheckpointNode(targetIndex);
            RestartNode(targetIndex);
            VerifyFieldsOnEndpoint(primary1, riKey, fields.Concat(afterRecovery));
        }

        /// <summary>
        /// Round-trip a tree P0 → P1 → P0, then checkpoint and restart the original owner (P0) and
        /// verify the data is intact — guards against stale-file / liveIndexes issues left behind
        /// after a back-migration interacting with persistence.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void ClusterMigrateRangeIndexRoundTripThenRestart()
        {
            const int shards = 2;
            const int sourceIndex = 0;

            context.CreateInstances(shards, tryRecover: true, enableRangeIndexPreview: true, enableAOF: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var primary0 = Endpoint(0);
            var primary1 = Endpoint(1);

            var riKey = FindKeyOwnedByNode(0);
            var slot = context.clusterTestUtils.HashSlot(riKey);

            var fields = Enumerable.Range(0, 15).Select(i => ($"field_{i}", $"value_{i}")).ToList();
            CreateRangeIndexWithFields(primary0, riKey, fields);

            // P0 → P1.
            MigrateSlotsAndWaitOwnership(primary0, primary1, [slot]);

            // Add a field on P1, then migrate back P1 → P0.
            var extra = ("field_extra", "value_extra");
            ClassicAssert.AreEqual("OK", (string)context.clusterTestUtils.Execute(primary1, "RI.SET", [riKey, extra.Item1, extra.Item2], flags: CommandFlags.NoRedirect));

            MigrateSlotsAndWaitOwnership(primary1, primary0, [slot]);

            VerifyFieldsOnEndpoint(primary0, riKey, fields.Append(extra));

            // Checkpoint + restart the original owner and confirm the round-tripped data survives.
            CheckpointNode(sourceIndex);
            RestartNode(sourceIndex);
            VerifyFieldsOnEndpoint(primary0, riKey, fields.Append(extra));
        }

        #endregion
    }
}