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
    /// Shared harness for Vector Set replication tests.
    ///
    /// <para>
    /// The fixtures built on this cover the four ways an index record can reach a node other than the
    /// one that built it: a diskless streaming full sync, a disk-based checkpoint full sync, recovery
    /// of a persisted checkpoint after a restart, and a failover that promotes a replica. Each is a
    /// separate transport with its own sanitization story, so each needs its own coverage.
    /// </para>
    /// <para>
    /// Two independent things are asserted throughout, because either can hold while the other is
    /// broken:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>Index ownership</b> (<see cref="AssertOwnsItsIndex"/>) — the structural invariant. A node may
    /// only dereference a native DiskANN handle it allocated itself.
    /// </item>
    /// <item>
    /// <b>Element-level equality</b> (<see cref="AssertVectorSetsMatch"/>) — every element VADDed on
    /// the source must exist on the target with a byte-identical embedding, the cardinality and
    /// dimensionality must agree, and VSIM must return the same neighbours from both. Matching
    /// cardinality alone is far too weak: an aliased index reports the <em>source's</em> count
    /// perfectly well.
    /// </item>
    /// </list>
    /// </summary>
    public abstract class VectorSetReplicationTestBase : TestBase
    {
        /// <summary>Length in bytes of an XB8 vector.</summary>
        protected const int VectorDimensions = 64;

        /// <summary>
        /// Enough plain keys to push the outstanding AOF past
        /// <c>replicaDisklessSyncFullSyncAofThreshold</c>, so a re-attach is guaranteed to take the
        /// full sync path instead of an incremental replay.
        /// </summary>
        protected const int FullSyncForcingKeys = 256;

        /// <summary>
        /// Marker <c>SyncMetadata</c> logs when a replica took a diskless streaming full sync rather
        /// than replaying AOF. Diskless-only: <c>SyncMetadata</c> lives entirely in the diskless path.
        /// </summary>
        private const string DisklessFullSyncMarker = "recoverFullSync:True";

        /// <summary>
        /// Disk-based equivalent. The checkpoint-transmitting path has no <c>SyncMetadata</c>, so the
        /// signal that a full sync happened is the primary shipping the main store checkpoint.
        /// </summary>
        private const string DiskBasedFullSyncMarker = "Sending main store checkpoint";

        /// <summary>
        /// Tees the node logs into a buffer so tests can assert on what replication actually did,
        /// rather than assuming it.
        /// </summary>
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
        protected readonly int testTimeout = (int)TimeSpan.FromSeconds(180).TotalSeconds;

        /// <summary>
        /// Everything written through <see cref="PopulateVectorSet"/>, so the replica can be checked
        /// element by element against what was actually added rather than against itself.
        /// </summary>
        private readonly Dictionary<string, List<byte[]>> writtenElements = [];

        /// <summary>
        /// Every fixture here needs Trace, because the only signal that distinguishes a full sync from
        /// an incremental AOF replay is <c>SyncMetadata</c>, logged at that level.
        /// </summary>
        protected abstract Dictionary<string, LogLevel> MonitorTests { get; }

        [SetUp]
        public virtual void Setup()
        {
            writtenElements.Clear();
            captureLogWriter = new(TestContext.Progress);

            context = new ClusterTestContext();
            context.logTextWriter = captureLogWriter;
            context.Setup(MonitorTests, testTimeoutSeconds: testTimeout);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        protected IPEndPoint Node(int index) => (IPEndPoint)context.endpoints[index];

        #region cluster formation

        /// <summary>
        /// Gives <paramref name="primaryIndex"/> the whole slot range and introduces every node to it,
        /// without attaching anything yet. Leaving the attach to the caller is what lets a fixture
        /// populate the primary <em>before</em> a replica exists, which is the only way to make a full
        /// sync carry a live index record.
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

        /// <summary>
        /// Detaches a replica and re-introduces it, so the subsequent attach starts from scratch.
        /// </summary>
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

        /// <summary>
        /// Moves the primary far enough ahead that a re-attach cannot be served incrementally.
        /// </summary>
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

        /// <summary>
        /// The precondition most of these fixtures rest on. If an attach quietly degrades into an
        /// incremental AOF replay the replica rebuilds its own index from VADD payloads and the test
        /// would pass for entirely the wrong reason.
        /// </summary>
        protected void AssertTookFullSync(int since = 0)
        {
            ClassicAssert.Greater(
                FullSyncCount(),
                since,
                "no new diskless streaming full sync was observed, so this test is not exercising the scenario it claims to");
        }

        /// <summary>
        /// Disk-based counterpart of <see cref="AssertTookFullSync"/>.
        /// </summary>
        protected void AssertTookDiskBasedFullSync(int since = 0)
        {
            ClassicAssert.Greater(
                DiskBasedFullSyncCount(),
                since,
                "the primary never shipped a main store checkpoint, so no disk-based full sync took place and this test is not exercising the scenario it claims to");
        }

        /// <summary>
        /// Takes a real checkpoint and waits for it to land. Going through <c>LASTSAVE</c> rather than
        /// just issuing <c>SAVE</c> is what makes this deterministic: <c>LASTSAVE</c> has one-second
        /// resolution, so a checkpoint taken within the same second as the previous one is
        /// indistinguishable from it.
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
        /// Spins until <paramref name="nodeIndex"/> answers for <paramref name="key"/> rather than
        /// redirecting. After a slot migration the receiving primary's replicas need a moment to pick
        /// up the new ownership, and a read issued in that window comes back as a MOVED error rather
        /// than as data.
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
        /// Adds <paramref name="count"/> deterministic elements, recording each one so the replica can
        /// later be verified against what was actually written.
        ///
        /// XB8 input combined with XPREQ8 round-trips exactly through VEMB, which is what makes an
        /// element-by-element embedding comparison meaningful rather than approximate.
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

        /// <summary>
        /// The read that reaches the native handle: VINFO's <c>size</c> field goes all the way down to
        /// <c>NativeDiskANNMethods.card</c>, which is exactly where the production SIGSEGV landed.
        /// </summary>
        protected long VectorSetSize(int nodeIndex, string key)
        {
            var reply = context.clusterTestUtils.Execute(Node(nodeIndex), "VINFO", [key], logger: context.logger);

            // ClusterTestUtils.Execute swallows exceptions and hands back the message as a bulk string,
            // so a node that died or rejected the read arrives here as a non-array reply.
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

        /// <summary>
        /// The stored embedding for a single element, or an empty array when the element is absent.
        /// </summary>
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
        /// Full element-level comparison of a Vector Set on two nodes.
        ///
        /// <para>
        /// Checks, in order of increasing strength: cardinality, dimensionality, that every element
        /// written to the source is present on the target with a byte-identical embedding, and that
        /// VSIM returns the same neighbours from both. The embedding sweep is the important one — it is
        /// the only check that would notice a replica which reports the right count but has lost, or
        /// silently substituted, the underlying vectors.
        /// </para>
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
        /// VSIM has to work on the target too. The embedding sweep proves the elements are stored; this
        /// proves the navigable graph built over them is functional and not merely present.
        ///
        /// <para>
        /// This deliberately does not demand identical result lists. A replica that rebuilt its index
        /// (after a full sync, or lazily after recovery) has a DiskANN graph constructed in a different
        /// insertion order from the primary's, so an approximate search legitimately diverges in the
        /// tail. What must hold is that the graph is navigable: an exact query for an element's own
        /// embedding finds that element, every neighbour returned is a real member of the set, and the
        /// bulk of a random query's neighbourhood agrees with the primary's.
        /// </para>
        /// </summary>
        protected void AssertSearchesAgree(int sourceIndex, int targetIndex, string key, int queries = 8, int count = 10)
        {
            var members = ElementsWrittenTo(key);
            var membership = new HashSet<string>(members.Select(Convert.ToHexString));

            // Exact queries: an element's own embedding must retrieve that element as the top hit.
            // This is the deterministic part - it holds regardless of how the graph was built.
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

            // Random queries: results need not be identical, but they must be real members and must
            // largely agree with the primary. A replica reading out of another set, or out of a stale
            // or half-built index, fails this.
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
        /// Blocks until <paramref name="nodeIndex"/> has actually applied every replicated VADD.
        ///
        /// <para>
        /// AOF offsets are not enough on their own. A replica does not apply VADDs inline: it queues
        /// them onto <c>VectorManager</c>'s replication replay channel and a pool of background tasks
        /// drains them, so the offset reported by <c>WaitForReplicaAofSync</c> runs ahead of the state
        /// of the index — the record itself is created by the replay. Reading in that window sees a set
        /// that is missing entirely, or short of elements.
        /// </para>
        /// </summary>
        protected void WaitForVectorReplay(int nodeIndex)
            => GetStoreWrapper(context.nodes[nodeIndex]).DefaultDatabase.VectorManager.WaitForVectorOperationsToComplete();

        /// <summary>
        /// Reads the native DiskANN handle a node currently has persisted for <paramref name="key"/>.
        ///
        /// This deliberately goes through <c>Read_MainStore</c> rather than
        /// <c>VectorManager.ReadVectorIndex</c>. The latter consults <c>NeedsRecreate</c> and will
        /// lazily rebuild the index, which would rewrite the very field under test.
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
        /// The <c>HandleIsLocal</c> invariant from <c>tla/VectorIndexLifetime.tla</c>, which is what
        /// every counterexample TLC produced actually violates. A node may only ever dereference an
        /// index handle it allocated itself.
        ///
        /// <para>
        /// This has to be asserted structurally rather than through the RESP surface. Cluster tests run
        /// every node inside a single process, so a foreign handle is still mapped and valid when the
        /// receiving node dereferences it: that node quietly answers out of the other's live index and
        /// every black-box read looks correct — including the element-level checks above. Only in a
        /// real deployment, where the nodes are separate processes, does the same aliasing become the
        /// observed SIGSEGV. This is why both kinds of assertion are needed.
        /// </para>
        /// <para>
        /// A zero handle satisfies the invariant trivially — it is the sanitized state, and the index
        /// is rebuilt lazily on first access — so this only fails when a node holds a genuinely foreign
        /// non-zero pointer.
        /// </para>
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
        /// Both halves of the contract in one call: the target holds exactly the data the source does
        /// <em>and</em> is not aliasing the source's index.
        ///
        /// <para>
        /// Order matters. The data assertions run first because reading is what drives the lazy
        /// <c>Service.RecreateIndex</c> rebuild; checking ownership beforehand would inspect a record
        /// whose pointer has been sanitized to zero but not yet repopulated, which tells us nothing.
        /// After the reads both nodes are expected to hold live, and necessarily distinct, handles.
        /// </para>
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