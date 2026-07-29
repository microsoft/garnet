// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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
    /// Regression tests for Vector Sets carried to a replica by a <em>diskless</em> full sync.
    ///
    /// <para>
    /// The diskless snapshot iterator streams raw Tsavorite log records verbatim
    /// (<c>ReplicationSnapshotIterator</c> -&gt; <c>DiskLogRecord.Serialize</c>). A Vector Set's index
    /// record persists the native DiskANN handle <c>IndexPtr</c> at offset 8 of its 56-byte value
    /// (<c>VectorManager.Index</c>), so the replica receives the <em>primary's</em> pointer.
    /// </para>
    /// <para>
    /// The only hook that zeroes that field is <c>GarnetRecordTriggers.OnDiskRead</c>, which by
    /// definition never fires for records streamed straight into memory. Since
    /// <c>VectorManager.NeedsRecreate</c> is exactly <c>indexPtr == 0</c>, a non-zero foreign pointer
    /// is indistinguishable from a healthy local one, the lazy <c>Service.RecreateIndex</c> rebuild is
    /// skipped, and the raw value reaches the P/Invoke unvalidated.
    /// </para>
    /// <para>
    /// Existing cluster Vector Set coverage cannot catch this. Those tests go through
    /// <c>SimpleSetupClusterAsync</c>, which never passes <c>enableDisklessSync</c>, and they attach
    /// replicas up front and then write, so each replica builds its own native index from replicated
    /// VADD payloads. The defect needs an <em>already-populated</em> Vector Set to travel over a full
    /// sync.
    /// </para>
    ///
    /// <para>
    /// These tests are the executable form of the TLA+ model in <c>tla/VectorIndexLifetime.tla</c>.
    /// Every counterexample TLC produced shares one 6-step trace: <c>SyncReset</c> -&gt;
    /// <c>PrimaryCreate(k1)</c> -&gt; <c>SyncSnapshot</c> -&gt; <c>BeginRead(k1)</c> -&gt;
    /// <c>Deref</c>. The three preconditions it isolates are what each test below sets up: (1) the
    /// replica takes a full sync, (2) the primary's record for the key carries a live non-zero handle
    /// at snapshot time, and (3) the replica subsequently reads that key. The
    /// <c>MC_Vec_QuiesceOnly_Buggy</c> scenario still failed, which is why none of these tests need to
    /// race anything: a quiet, fully serialized read after the sync completes is enough.
    /// </para>
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetDisklessSyncTests : TestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        /// <summary>Length in bytes of an XB8 vector.</summary>
        private const int VectorDimensions = 64;

        /// <summary>
        /// Enough plain keys written after the reset to push the outstanding AOF past
        /// <c>replicaDisklessSyncFullSyncAofThreshold</c>, so the re-attach is guaranteed to take the
        /// streaming snapshot path instead of an incremental replay.
        /// </summary>
        private const int FullSyncForcingKeys = 256;

        /// <summary>
        /// Tees the node logs into a buffer so the tests can assert on what replication actually did,
        /// rather than assuming it.
        /// </summary>
        private sealed class CaptureLogWriter(TextWriter passThrough) : TextWriter
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

        /// <summary>
        /// Every test here needs Trace, because the only signal that distinguishes a streaming full
        /// sync from an incremental AOF replay is <c>SyncMetadata</c>, logged at that level.
        /// </summary>
        private static readonly Dictionary<string, LogLevel> MonitorTests = new()
        {
            [nameof(VectorSetReadableOnReplicaAfterDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetsStayPartitionedAcrossDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReadableOnReplicaWithoutFullSync)] = LogLevel.Trace,
        };

        private ClusterTestContext context;
        private CaptureLogWriter captureLogWriter;

        private readonly int timeout = (int)TimeSpan.FromSeconds(15).TotalSeconds;
        private readonly int testTimeout = (int)TimeSpan.FromSeconds(120).TotalSeconds;

        [SetUp]
        public virtual void Setup()
        {
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

        /// <summary>
        /// Stand up a primary with a single attached replica, configured for diskless sync with the
        /// full-sync AOF threshold pinned low.
        /// </summary>
        private void SetupPrimaryAndReplica()
        {
            context.CreateInstances(
                2,
                enableAOF: true,
                enableDisklessSync: true,
                replicaDisklessSyncFullSyncAofThreshold: "1k",
                timeout: timeout);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(PrimaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(PrimaryIndex, PrimaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(ReplicaIndex, ReplicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(PrimaryIndex, ReplicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(PrimaryIndex, ReplicaIndex, logger: context.logger);

            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: ReplicaIndex, primaryNodeIndex: PrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);
        }

        /// <summary>
        /// The model's <c>SyncReset</c> followed by <c>SyncSnapshot</c>: detach the replica, move the
        /// primary far enough ahead that the re-attach cannot be served incrementally, then re-attach.
        /// </summary>
        private void ForceDisklessFullSync()
        {
            _ = context.clusterTestUtils.ClusterReset(ReplicaIndex, soft: true, expiry: 1, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(ReplicaIndex, logger: context.logger);
            while (!context.clusterTestUtils.IsKnown(ReplicaIndex, PrimaryIndex, logger: context.logger))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                context.clusterTestUtils.Meet(ReplicaIndex, PrimaryIndex, logger: context.logger);
            }

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            for (var i = 0; i < FullSyncForcingKeys; i++)
            {
                _ = context.clusterTestUtils.Execute(primary, "SET", [$"{{padding}}key{i}", new string('x', 64)], skipLogging: true);
            }

            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: ReplicaIndex, primaryNodeIndex: PrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            AssertTookFullSync();
        }

        /// <summary>
        /// The precondition the whole model rests on. <c>MC_Vec_NoFullSync</c> verified, so if the
        /// re-attach quietly degrades to an incremental AOF replay these tests would pass for entirely
        /// the wrong reason.
        /// </summary>
        private void AssertTookFullSync()
        {
            var log = captureLogWriter.Snapshot();
            ClassicAssert.IsTrue(
                log.Contains("recoverFullSync:True", StringComparison.Ordinal),
                "replica did not take a streaming full sync, so this test is not exercising the modelled scenario");
        }

        private void PopulateVectorSet(IPEndPoint endpoint, string key, int count, int seed)
        {
            var r = new Random(seed);
            var vector = new byte[VectorDimensions];
            var element = new byte[4];

            for (var i = 0; i < count; i++)
            {
                r.NextBytes(vector);
                BinaryPrimitives.WriteInt32LittleEndian(element, i);

                var added = (int)context.clusterTestUtils.Execute(endpoint, "VADD", [key, "XB8", vector, element, "XPREQ8"], skipLogging: true);
                ClassicAssert.AreEqual(1, added, $"VADD of element {i} into '{key}' should have inserted a new element");
            }
        }

        /// <summary>
        /// The model's <c>BeginRead</c> + <c>Deref</c>: the read that reaches the native handle.
        /// </summary>
        private long VectorSetSize(IPEndPoint endpoint, string key)
        {
            var reply = context.clusterTestUtils.Execute(endpoint, "VINFO", [key], logger: context.logger);

            // ClusterTestUtils.Execute swallows exceptions and hands back the message as a bulk string,
            // so a replica that died or rejected the read arrives here as a non-array reply.
            if (reply.Resp2Type != ResultType.Array)
                Assert.Fail($"VINFO on '{key}' did not return an array, got {reply.Resp2Type}: {reply}");

            var fields = (RedisValue[])reply;
            for (var i = 0; i + 1 < fields.Length; i += 2)
            {
                if (((string)fields[i]).Equals("size", StringComparison.OrdinalIgnoreCase))
                    return (long)fields[i + 1];
            }

            Assert.Fail($"VINFO reply for '{key}' had no 'size' field: [{string.Join(", ", fields.Select(static f => (string)f))}]");
            return -1;
        }

        private void MakeReplicaReadable()
        {
            var replica = (IPEndPoint)context.endpoints[ReplicaIndex];
            var ok = (string)context.clusterTestUtils.Execute(replica, "READONLY", [], logger: context.logger);
            ClassicAssert.AreEqual("OK", ok);
        }

        /// <summary>
        /// Reads the native DiskANN handle a node currently has persisted for <paramref name="key"/>.
        ///
        /// This deliberately goes through <c>Read_MainStore</c> rather than
        /// <c>VectorManager.ReadVectorIndex</c>. The latter consults <c>NeedsRecreate</c> and will
        /// lazily rebuild the index, which would rewrite the very field under test.
        /// </summary>
        private static nint ReadPersistedIndexPtr(GarnetServer server, string key)
        {
            var storeWrapper = GetStoreWrapper(server);
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
            ClassicAssert.AreEqual(GarnetStatus.OK, status, $"could not read the index record for '{key}'");
            ClassicAssert.IsTrue(output.SpanByteAndMemory.IsSpanByte, $"index record for '{key}' did not come back inline");
            ClassicAssert.AreEqual(VectorManager.IndexSize, output.SpanByteAndMemory.Length, $"value under '{key}' is not a Vector Set index record");

            VectorManager.ReadIndex(output.SpanByteAndMemory.Span, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            return indexPtr;
        }

        /// <summary>
        /// The <c>HandleIsLocal</c> invariant, which is what every counterexample in the model actually
        /// violates. A node may only ever dereference an index handle it allocated itself.
        ///
        /// This has to be asserted structurally rather than through the RESP surface. Cluster tests run
        /// every node inside a single process, so the primary's handle is still mapped and valid when
        /// the replica dereferences it: the replica quietly answers out of the primary's live index and
        /// every black-box read looks correct. Only in a real deployment, where the two nodes are
        /// separate processes, does the same aliasing become the observed SIGSEGV.
        /// </summary>
        private void AssertReplicaOwnsItsIndex(string key)
        {
            var primaryPtr = ReadPersistedIndexPtr(context.nodes[PrimaryIndex], key);
            var replicaPtr = ReadPersistedIndexPtr(context.nodes[ReplicaIndex], key);

            ClassicAssert.AreNotEqual(
                nint.Zero,
                primaryPtr,
                $"primary should have a live index for '{key}'");

            ClassicAssert.AreNotEqual(
                primaryPtr,
                replicaPtr,
                $"replica persisted the primary's DiskANN handle for '{key}' (0x{primaryPtr:x}); it is pointing into another node's heap and will fault once the two are separate processes");
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(GarnetServer server);

        /// <summary>
        /// Executable form of <c>MC_Vec_Buggy</c> / <c>MC_Vec_Segfault_Buggy</c>, which violate
        /// <c>HandleIsLocal</c> and <c>NoSegfault</c>.
        ///
        /// A Vector Set that already exists on the primary is carried to the replica by a diskless full
        /// sync, and the replica is then asked to read it. The replica receives the index record with
        /// the primary's <c>IndexPtr</c> intact, never rebuilds the index locally, and the first read
        /// that reaches <c>NativeDiskANNMethods.card</c> dereferences a handle it does not own.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(120_000)]
        public void VectorSetReadableOnReplicaAfterDisklessFullSync()
        {
            const string Key = "{vsdisk}solo";
            const int Elements = 500;

            SetupPrimaryAndReplica();

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var replica = (IPEndPoint)context.endpoints[ReplicaIndex];

            // Written while the replica is attached, so it arrives as VADD payloads and the replica
            // builds its own native index. This much already works today.
            PopulateVectorSet(primary, Key, Elements, seed: 2026_07_29_00);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReplicaReadable();
            ClassicAssert.AreEqual(Elements, VectorSetSize(primary, Key), "primary should hold every element that was added");
            ClassicAssert.AreEqual(Elements, VectorSetSize(replica, Key), "replica should agree before any full sync");

            ForceDisklessFullSync();

            AssertReplicaOwnsItsIndex(Key);

            MakeReplicaReadable();
            ClassicAssert.AreEqual(Elements, VectorSetSize(replica, Key), "replica element count must match the primary after a diskless full sync");

            // The replica must be able to search out of an index it actually owns.
            var query = new byte[VectorDimensions];
            new Random(2026_07_29_01).NextBytes(query);

            var hits = (byte[][])context.clusterTestUtils.Execute(replica, "VSIM", [Key, "XB8", query], logger: context.logger);
            ClassicAssert.IsNotNull(hits, "VSIM against the replica should return a reply");
            ClassicAssert.Greater(hits.Length, 0, "replica should return neighbours after a diskless full sync");
        }

        /// <summary>
        /// Executable form of <c>MC_Vec_WrongIndex_Buggy</c>, which violates <c>NoWrongIndex</c>.
        ///
        /// With more than one Vector Set in play a streamed foreign handle can resolve to a different
        /// live index rather than to unmapped memory, in which case the read succeeds and quietly
        /// reports another set's contents. Two sets of deliberately different sizes make that
        /// substitution visible: each key must report exactly its own element count.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(120_000)]
        public void VectorSetsStayPartitionedAcrossDisklessFullSync()
        {
            // Same hash slot, so both keys live on the single primary in this topology.
            const string SmallKey = "{vsdisk}small";
            const string LargeKey = "{vsdisk}large";
            const int SmallElements = 10;
            const int LargeElements = 400;

            SetupPrimaryAndReplica();

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var replica = (IPEndPoint)context.endpoints[ReplicaIndex];

            PopulateVectorSet(primary, LargeKey, LargeElements, seed: 2026_07_29_02);
            PopulateVectorSet(primary, SmallKey, SmallElements, seed: 2026_07_29_03);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            ClassicAssert.AreEqual(LargeElements, VectorSetSize(primary, LargeKey));
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(primary, SmallKey));

            ForceDisklessFullSync();

            AssertReplicaOwnsItsIndex(SmallKey);
            AssertReplicaOwnsItsIndex(LargeKey);

            MakeReplicaReadable();
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(replica, SmallKey), "small set must not report elements belonging to another Vector Set");
            ClassicAssert.AreEqual(LargeElements, VectorSetSize(replica, LargeKey), "large set must report exactly its own elements");
        }

        /// <summary>
        /// Executable form of the <c>MC_Vec_NoFullSync</c> control, which TLC verified.
        ///
        /// Identical to <see cref="VectorSetReadableOnReplicaAfterDisklessFullSync"/> except that the
        /// replica never takes a full sync. Removing that single step is what makes the model safe, so
        /// this test must pass even while the others fail; if it ever fails the defect is not the one
        /// the model describes and the rest of this fixture is measuring the wrong thing.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(120_000)]
        public void VectorSetReadableOnReplicaWithoutFullSync()
        {
            const string Key = "{vsdisk}control";
            const int Elements = 500;

            SetupPrimaryAndReplica();

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var replica = (IPEndPoint)context.endpoints[ReplicaIndex];

            PopulateVectorSet(primary, Key, Elements, seed: 2026_07_29_04);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReplicaReadable();
            ClassicAssert.AreEqual(Elements, VectorSetSize(primary, Key));
            ClassicAssert.AreEqual(Elements, VectorSetSize(replica, Key), "replica should agree with the primary when no full sync is involved");

            // The control's whole point: without a full sync the replica builds its own index from
            // replicated VADD payloads, so this holds today and pins down the full sync as the cause.
            AssertReplicaOwnsItsIndex(Key);

            var query = new byte[VectorDimensions];
            new Random(2026_07_29_05).NextBytes(query);

            var hits = (byte[][])context.clusterTestUtils.Execute(replica, "VSIM", [Key, "XB8", query], logger: context.logger);
            ClassicAssert.IsNotNull(hits);
            ClassicAssert.Greater(hits.Length, 0);
        }
    }
}
