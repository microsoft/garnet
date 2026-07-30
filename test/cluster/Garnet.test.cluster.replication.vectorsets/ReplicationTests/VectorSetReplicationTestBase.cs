// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Shared harness for Vector Set replication tests. Asserts both index ownership
    /// and element-level equality, since either can hold while the other is broken.
    /// </summary>
    public abstract class VectorSetReplicationTestBase : TestBase
    {
        /// <summary>Length in bytes of an XB8 vector.</summary>
        protected const int VectorDimensions = 64;

        /// <summary>
        /// Enough padding keys to force a re-attach down the full-sync path instead of incremental replay.
        /// </summary>
        protected const int FullSyncForcingKeys = 256;

        /// <summary>Diskless full-sync marker emitted by SyncMetadata.</summary>
        private const string DisklessFullSyncMarker = "recoverFullSync:True";

        /// <summary>Disk-based full-sync marker emitted when the primary ships a checkpoint.</summary>
        private const string DiskBasedFullSyncMarker = "Sending main store checkpoint";

        /// <summary>Captures node logs while still forwarding them to the test output.</summary>
        protected sealed class CaptureLogWriter(TextWriter passThrough) : TextWriter
        {
            private readonly StringBuilder buffer = new();

            public override Encoding Encoding => passThrough.Encoding;

            public override void Write(string value)
            {
                passThrough.Write(value);
                lock (buffer)
                {
                    _ = buffer.Append(value);
                }
            }

            public string Snapshot()
            {
                lock (buffer)
                {
                    return buffer.ToString();
                }
            }
        }

        protected ClusterTestContext context;
        protected CaptureLogWriter captureLogWriter;

        protected readonly int timeout = (int)TimeSpan.FromSeconds(15).TotalSeconds;
        protected readonly int testTimeout = (int)TimeSpan.FromSeconds(60).TotalSeconds;

        /// <summary>Elements written through PopulateVectorSet, used for source-vs-target checks.</summary>
        private readonly Dictionary<string, List<byte[]>> writtenElements = [];

        /// <summary>Trace logging is required to distinguish full sync from incremental replay.</summary>
        protected virtual LogLevel MonitorLogLevel => LogLevel.Trace;

        [SetUp]
        public virtual void Setup()
        {
            writtenElements.Clear();
            captureLogWriter = new(TestContext.Progress);

            context = new ClusterTestContext();
            context.logTextWriter = captureLogWriter;
            context.Setup(new Dictionary<string, LogLevel> { [TestContext.CurrentContext.Test.MethodName] = MonitorLogLevel }, testTimeoutSeconds: testTimeout);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        protected IPEndPoint Node(int index) => (IPEndPoint)context.endpoints[index];

        #region cluster formation

        /// <summary>
        /// Assigns all slots to primaryIndex and introduces the other nodes without attaching replicas.
        /// </summary>
        protected void FormCluster(int primaryIndex, params int[] otherIndexes)
        {
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);

            foreach (var other in otherIndexes)
            {
                context.clusterTestUtils.SetConfigEpoch(other, other + 1, logger: context.logger);
                context.clusterTestUtils.Meet(primaryIndex, other, logger: context.logger);
                context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, other, logger: context.logger);
            }
        }

        protected void Attach(int replicaIndex, int primaryIndex, bool waitForRecovery = false)
        {
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            if (waitForRecovery)
                context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);
        }

        /// <summary>Detaches and re-introduces a replica so the next attach starts from scratch.</summary>
        protected void ResetReplica(int replicaIndex, int primaryIndex)
        {
            _ = context.clusterTestUtils.ClusterReset(replicaIndex, soft: true, expiry: 1, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(replicaIndex, logger: context.logger);

            while (!context.clusterTestUtils.IsKnown(replicaIndex, primaryIndex, logger: context.logger))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                context.clusterTestUtils.Meet(replicaIndex, primaryIndex, logger: context.logger);
            }
        }

        /// <summary>Moves the primary far enough ahead that a re-attach cannot be incremental.</summary>
        protected void PushPrimaryAhead(int primaryIndex)
        {
            var primary = Node(primaryIndex);
            for (var i = 0; i < FullSyncForcingKeys; i++)
            {
                _ = context.clusterTestUtils.Execute(primary, "SET", [$"{{padding}}key{i}", new string('x', 64)], skipLogging: true);
            }
        }

        private int CountLogOccurrences(string marker)
        {
            var log = captureLogWriter.Snapshot();
            var count = 0;
            var at = 0;

            while ((at = log.IndexOf(marker, at, StringComparison.Ordinal)) >= 0)
            {
                count++;
                at += marker.Length;
            }

            return count;
        }

        protected int FullSyncCount() => CountLogOccurrences(DisklessFullSyncMarker);

        protected int DiskBasedFullSyncCount() => CountLogOccurrences(DiskBasedFullSyncMarker);

        /// <summary>Asserts the attach took diskless full sync, not incremental replay.</summary>
        protected void AssertTookFullSync(int since = 0)
        {
            ClassicAssert.Greater(
                FullSyncCount(),
                since,
                "no new diskless streaming full sync was observed, so this test is not exercising the scenario it claims to");
        }

        /// <summary>Disk-based counterpart of AssertTookFullSync.</summary>
        protected void AssertTookDiskBasedFullSync(int since = 0)
        {
            ClassicAssert.Greater(
                DiskBasedFullSyncCount(),
                since,
                "the primary never shipped a main store checkpoint, so no disk-based full sync took place and this test is not exercising the scenario it claims to");
        }

        /// <summary>
        /// Takes a checkpoint after waiting past LASTSAVE's one-second resolution, so
        /// WaitCheckpoint observes the new save.
        /// </summary>
        protected void TakeCheckpoint(int nodeIndex)
        {
            var lastSave = context.clusterTestUtils.LastSave(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(nodeIndex, lastSave, logger: context.logger);
            context.clusterTestUtils.Checkpoint(nodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(nodeIndex, lastSave, logger: context.logger);
        }

        protected void MakeReadable(int nodeIndex)
        {
            var ok = (string)context.clusterTestUtils.Execute(Node(nodeIndex), "READONLY", [], logger: context.logger);
            ClassicAssert.AreEqual("OK", ok);
        }

        /// <summary>
        /// Waits until nodeIndex serves key instead of returning MOVED after a slot migration.
        /// </summary>
        protected void WaitUntilServes(int nodeIndex, string key)
        {
            var endpoint = Node(nodeIndex);

            for (var attempt = 0; attempt < 200; attempt++)
            {
                MakeReadable(nodeIndex);
                WaitForVectorReplay(nodeIndex);

                var reply = context.clusterTestUtils.Execute(endpoint, "VINFO", [key], skipLogging: true);
                if (reply.Resp2Type == ResultType.Array)
                    return;

                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
            }

            Assert.Fail($"node {nodeIndex} never began serving '{key}'");
        }

        #endregion

        #region vector set operations

        /// <summary>
        /// Adds deterministic XB8 elements and records them so targets can be compared with the
        /// actual writes. XPREQ8 round-trips exactly through VEMB.
        /// </summary>
        protected void PopulateVectorSet(int nodeIndex, string key, int count, int seed)
        {
            var endpoint = Node(nodeIndex);
            var r = new Random(seed);

            if (!writtenElements.TryGetValue(key, out var elements))
            {
                writtenElements[key] = elements = [];
            }

            for (var i = 0; i < count; i++)
            {
                var vector = new byte[VectorDimensions];
                r.NextBytes(vector);

                var element = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(element, elements.Count);

                var added = (int)context.clusterTestUtils.Execute(endpoint, "VADD", [key, "XB8", vector, element, "XPREQ8"], skipLogging: true);
                ClassicAssert.AreEqual(1, added, $"VADD of element {i} into '{key}' should have inserted a new element");

                elements.Add(element);
            }
        }

        protected IReadOnlyList<byte[]> ElementsWrittenTo(string key) => writtenElements.TryGetValue(key, out var e) ? e : [];

        /// <summary>Reads VINFO size, the path that reaches NativeDiskANNMethods.card.</summary>
        protected long VectorSetSize(int nodeIndex, string key)
        {
            var reply = context.clusterTestUtils.Execute(Node(nodeIndex), "VINFO", [key], logger: context.logger);

            // Execute returns failures as bulk strings, so non-array replies mean the read failed.
            if (reply.Resp2Type != ResultType.Array)
                Assert.Fail($"VINFO on '{key}' at node {nodeIndex} did not return an array, got {reply.Resp2Type}: {reply}");

            var fields = (RedisValue[])reply;
            if (fields is null)
                Assert.Fail($"VINFO on '{key}' at node {nodeIndex} returned a nil array, so that node does not hold the Vector Set at all");

            for (var i = 0; i + 1 < fields.Length; i += 2)
            {
                if (((string)fields[i]).Equals("size", StringComparison.OrdinalIgnoreCase))
                    return (long)fields[i + 1];
            }

            Assert.Fail($"VINFO reply for '{key}' had no 'size' field: [{string.Join(", ", fields.Select(static f => (string)f))}]");
            return -1;
        }

        protected long VectorSetDimensions(int nodeIndex, string key)
        {
            var reply = context.clusterTestUtils.Execute(Node(nodeIndex), "VDIM", [key], logger: context.logger);
            if (reply.Resp2Type != ResultType.Integer)
                Assert.Fail($"VDIM on '{key}' at node {nodeIndex} did not return an integer, got {reply.Resp2Type}: {reply}");

            return (long)reply;
        }

        /// <summary>Returns the stored embedding for an element, or an empty array when absent.</summary>
        protected string[] ElementEmbedding(int nodeIndex, string key, byte[] element)
        {
            var reply = context.clusterTestUtils.Execute(Node(nodeIndex), "VEMB", [key, element], skipLogging: true);
            if (reply.Resp2Type != ResultType.Array)
                Assert.Fail($"VEMB on '{key}' at node {nodeIndex} did not return an array, got {reply.Resp2Type}: {reply}");

            return (string[])reply;
        }

        protected byte[][] Search(int nodeIndex, string key, byte[] query, int count)
        {
            var reply = context.clusterTestUtils.Execute(Node(nodeIndex), "VSIM", [key, "XB8", query, "COUNT", count.ToString()], skipLogging: true);
            if (reply.Resp2Type != ResultType.Array)
                Assert.Fail($"VSIM on '{key}' at node {nodeIndex} did not return an array, got {reply.Resp2Type}: {reply}");

            return (byte[][])reply;
        }

        #endregion

        #region assertions

        /// <summary>
        /// Compares cardinality, dimensions, embeddings, and VSIM behavior against the elements
        /// actually written; the embedding sweep catches correct counts with wrong vectors.
        /// </summary>
        protected void AssertVectorSetsMatch(int sourceIndex, int targetIndex, string key)
        {
            var expected = ElementsWrittenTo(key);
            ClassicAssert.Greater(expected.Count, 0, $"no elements were recorded for '{key}'; the test is asserting nothing");

            WaitForVectorReplay(sourceIndex);
            WaitForVectorReplay(targetIndex);

            var sourceSize = VectorSetSize(sourceIndex, key);
            var targetSize = VectorSetSize(targetIndex, key);

            ClassicAssert.AreEqual(expected.Count, sourceSize, $"node {sourceIndex} lost elements of '{key}'");
            ClassicAssert.AreEqual(sourceSize, targetSize, $"node {targetIndex} disagrees with node {sourceIndex} on the cardinality of '{key}'");

            ClassicAssert.AreEqual(
                VectorSetDimensions(sourceIndex, key),
                VectorSetDimensions(targetIndex, key),
                $"node {targetIndex} disagrees with node {sourceIndex} on the dimensionality of '{key}'");

            var missing = new List<int>();
            var mismatched = new List<int>();

            for (var i = 0; i < expected.Count; i++)
            {
                var element = expected[i];
                var targetEmbedding = ElementEmbedding(targetIndex, key, element);

                if (targetEmbedding.Length == 0)
                {
                    missing.Add(i);
                    continue;
                }

                var sourceEmbedding = ElementEmbedding(sourceIndex, key, element);
                if (!sourceEmbedding.SequenceEqual(targetEmbedding))
                    mismatched.Add(i);
            }

            ClassicAssert.IsEmpty(
                missing,
                $"{missing.Count} of {expected.Count} elements VADDed into '{key}' on node {sourceIndex} are missing on node {targetIndex} (first few: {string.Join(", ", missing.Take(10))})");

            ClassicAssert.IsEmpty(
                mismatched,
                $"{mismatched.Count} of {expected.Count} elements of '{key}' have a different embedding on node {targetIndex} than on node {sourceIndex} (first few: {string.Join(", ", mismatched.Take(10))})");

            AssertSearchesAgree(sourceIndex, targetIndex, key);
        }

        /// <summary>
        /// Verifies VSIM on the target without requiring identical tails: rebuilt approximate-NN
        /// graphs can legitimately return different tail orderings.
        /// </summary>
        protected void AssertSearchesAgree(int sourceIndex, int targetIndex, string key, int queries = 8, int count = 10)
        {
            var members = ElementsWrittenTo(key);
            var membership = new HashSet<string>(members.Select(Convert.ToHexString));

            // Exact queries must find the element itself regardless of graph construction.
            var r = new Random(20260729);
            for (var q = 0; q < queries; q++)
            {
                var element = members[r.Next(members.Count)];
                var embedding = ElementEmbedding(targetIndex, key, element);
                ClassicAssert.Greater(embedding.Length, 0, $"element {q} of '{key}' is missing on node {targetIndex}");

                var query = embedding.Select(static component => (byte)Math.Clamp((int)float.Parse(component, CultureInfo.InvariantCulture), byte.MinValue, byte.MaxValue)).ToArray();
                var hits = Search(targetIndex, key, query, count);

                ClassicAssert.Greater(hits.Length, 0, $"VSIM on node {targetIndex} returned nothing for an element of '{key}' that it holds");
                ClassicAssert.IsTrue(
                    hits[0].AsSpan().SequenceEqual(element),
                    $"VSIM on node {targetIndex} for the exact embedding of element [{string.Join(",", element)}] of '{key}' returned [{string.Join(",", hits[0])}] as its nearest neighbour, so the index is not navigable to its own elements");
            }

            // Random-query tails may differ, but hits must be real members and broadly agree.
            for (var q = 0; q < queries; q++)
            {
                var query = new byte[VectorDimensions];
                r.NextBytes(query);

                var sourceHits = Search(sourceIndex, key, query, count);
                var targetHits = Search(targetIndex, key, query, count);

                ClassicAssert.Greater(sourceHits.Length, 0, $"VSIM on node {sourceIndex} returned nothing for '{key}'");
                ClassicAssert.AreEqual(
                    sourceHits.Length,
                    targetHits.Length,
                    $"VSIM for query {q} on '{key}' returned {sourceHits.Length} hits on node {sourceIndex} but {targetHits.Length} on node {targetIndex}");

                var foreign = targetHits.Where(hit => !membership.Contains(Convert.ToHexString(hit))).ToList();
                ClassicAssert.IsEmpty(
                    foreign,
                    $"VSIM for query {q} on '{key}' returned {foreign.Count} neighbours on node {targetIndex} that were never VADDed into that set (first: [{string.Join(",", foreign.FirstOrDefault() ?? [])}])");

                var sourceSet = new HashSet<string>(sourceHits.Select(Convert.ToHexString));
                var overlap = targetHits.Count(hit => sourceSet.Contains(Convert.ToHexString(hit)));

                ClassicAssert.GreaterOrEqual(
                    overlap * 2,
                    sourceHits.Length,
                    $"VSIM for query {q} on '{key}' agreed on only {overlap} of {sourceHits.Length} neighbours between node {sourceIndex} and node {targetIndex}; the two indexes are not searching the same vectors");
            }
        }

        /// <summary>
        /// AOF offsets are insufficient: replicas queue VADDs onto VectorManager's replay
        /// channel, so offset sync can run ahead of index state.
        /// </summary>
        protected void WaitForVectorReplay(int nodeIndex)
            => GetStoreWrapper(context.nodes[nodeIndex]).DefaultDatabase.VectorManager.WaitForVectorOperationsToComplete();

        /// <summary>
        /// Reads the persisted DiskANN handle via Read_MainStore so ReadVectorIndex cannot
        /// lazily rebuild and rewrite it.
        /// </summary>
        protected nint ReadPersistedIndexPtr(int nodeIndex, string key)
        {
            var storeWrapper = GetStoreWrapper(context.nodes[nodeIndex]);
            var db = storeWrapper.DefaultDatabase;

            using var storageSession = new StorageSession(storeWrapper, new ScratchBufferBuilder(), new ScratchBufferAllocator(), null, null, db.Id, readSessionState: null, db.VectorManager, null);

            var keyBytes = GC.AllocateArray<byte>(Encoding.ASCII.GetByteCount(key), pinned: true);
            _ = Encoding.ASCII.GetBytes(key, keyBytes);

            StringInput input = new(RespCommand.VINFO);
            input.parseState.Initialize(1);
            input.parseState.SetArgument(0, PinnedSpanByte.FromPinnedSpan(keyBytes));

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSize];
            StringOutput output = new(SpanByteAndMemory.FromPinnedSpan(indexSpan));

            var status = storageSession.Read_MainStore(keyBytes, ref input, ref output, ref storageSession.stringBasicContext);
            ClassicAssert.AreEqual(GarnetStatus.OK, status, $"could not read the index record for '{key}' on node {nodeIndex}");
            ClassicAssert.IsTrue(output.SpanByteAndMemory.IsSpanByte, $"index record for '{key}' did not come back inline");
            ClassicAssert.AreEqual(VectorManager.IndexSize, output.SpanByteAndMemory.Length, $"value under '{key}' is not a Vector Set index record");

            VectorManager.ReadIndex(output.SpanByteAndMemory.Span, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            return indexPtr;
        }

        /// <summary>
        /// Asserts a node does not hold another node's DiskANN handle. A zero handle is valid
        /// after sanitization until lazy rebuild; only foreign non-zero pointers fail.
        /// </summary>
        protected void AssertOwnsItsIndex(int nodeIndex, int otherNodeIndex, string key)
        {
            var otherPtr = ReadPersistedIndexPtr(otherNodeIndex, key);
            var nodePtr = ReadPersistedIndexPtr(nodeIndex, key);

            if (nodePtr == nint.Zero || otherPtr == nint.Zero)
                return;

            ClassicAssert.AreNotEqual(
                otherPtr,
                nodePtr,
                $"node {nodeIndex} holds node {otherNodeIndex}'s DiskANN handle for '{key}' (0x{otherPtr:x}); it is pointing into another node's heap and will fault once the two are separate processes");
        }

        /// <summary>
        /// Asserts data equality before ownership because reads trigger lazy rebuild; checking
        /// ownership first can see the expected zero sanitized pointer.
        /// </summary>
        protected void AssertFullyReplicated(int sourceIndex, int targetIndex, string key)
        {
            AssertVectorSetsMatch(sourceIndex, targetIndex, key);

            var sourcePtr = ReadPersistedIndexPtr(sourceIndex, key);
            var targetPtr = ReadPersistedIndexPtr(targetIndex, key);

            ClassicAssert.AreNotEqual(nint.Zero, sourcePtr, $"node {sourceIndex} should have a live index for '{key}' after being read");
            ClassicAssert.AreNotEqual(nint.Zero, targetPtr, $"node {targetIndex} should have built its own index for '{key}' after being read");

            ClassicAssert.AreNotEqual(
                sourcePtr,
                targetPtr,
                $"node {targetIndex} holds node {sourceIndex}'s DiskANN handle for '{key}' (0x{sourcePtr:x}); it is pointing into another node's heap and will fault once the two are separate processes");
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(GarnetServer server);

        #endregion
    }
}