// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Regression test for the primary-side VectorSet recreate livelock.
    ///
    /// Symptom (observed in production and on a 2-node repro cluster): the primary
    /// pegs all cores and a single vector set key spins forever in the recreate loop,
    /// logging <c>RECREATE ctx=0 dims=0 indexPtr=0</c> with no preceding
    /// <c>INITIAL-CREATE</c>. A fully-zeroed 56-byte index record is read back as
    /// <see cref="GarnetStatus.OK"/>, so <see cref="VectorManager.NeedsRecreate"/>
    /// (which only checks <c>indexPtr == 0</c>) returns true, then
    /// <c>RecreateIndex(dims: 0)</c> can never produce a valid pointer, so the loop
    /// never terminates.
    ///
    /// Root cause: a VADD is phrased as a read once the index exists, so after a
    /// successful add <see cref="VectorManager.ReplicateVectorSetAdd"/> issues a
    /// synthetic <c>VADDAppendLogArg</c> RMW on the index key purely to get the write
    /// into the AOF. That synthetic arg is only meaningful as a <em>CopyUpdater</em>
    /// on an already-existing record. VADD holds only a SHARED vector lock across the
    /// add + replicate, and UNLINK's raw main-store <c>DELETE</c> does NOT take the
    /// vector lock, so a concurrent UNLINK can tombstone the key in between. The
    /// synthetic RMW then lands on an absent key, where <c>NeedInitialUpdate</c>
    /// returned <c>true</c> and <c>InitialUpdater</c>'s no-op branch left the
    /// pre-sized 56-byte record zeroed — resurrecting the key as a phantom
    /// recreate-flagged record.
    ///
    /// This test drives that exact interleaving deterministically (create -> UNLINK ->
    /// synthetic replication RMW) and asserts the tombstoned key is NOT resurrected as
    /// a zeroed recreate-flagged record. It fails on the unfixed code and passes once
    /// <c>NeedInitialUpdate</c> refuses to create a record for the CU-only synthetic args.
    /// </summary>
    [TestFixture]
    public class VectorRecreateLivelockTests : TestBase
    {
        private global::Garnet.GarnetServer server;

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(global::Garnet.GarnetServer server);

        [TearDown]
        public void TearDown()
        {
            try { server.Dispose(); } catch { }
            TestUtils.OnTearDown();
        }

        [Test]
        public void SyntheticReplicationAddOnTombstonedKeyMustNotCreateZeroedIndex([Values(false, true)] bool enableAOF)
        {
            const string Key = nameof(SyntheticReplicationAddOnTombstonedKeyMustNotCreateZeroedIndex);

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: enableAOF, enableVectorSetPreview: true);
            server.Start();

            ref var storeWrapper = ref GetStoreWrapper(server);
            ClassicAssert.IsNotNull(storeWrapper, "Could not access storeWrapper via UnsafeAccessor");

            var vectorManager = storeWrapper.DefaultDatabase.VectorManager;
            ClassicAssert.IsNotNull(vectorManager, "VectorManager not initialised — enableVectorSetPreview must be true");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // 1. Create a real vector set index for Key (this is the only path that logs INITIAL-CREATE).
            var elem = new byte[4];
            var data = new byte[75];
            new Random(2026_07_15).NextBytes(data);
            _ = db.Execute("VADD", [Key, "XB8", (RedisValue)data, (RedisValue)elem, "XPREQ8"]);
            ClassicAssert.IsTrue(db.KeyExists(Key), "vector set should exist after VADD");

            // 2. UNLINK the key. This is a raw main-store tombstone that does NOT take the
            //    vector lock, so in production it can land while a concurrent VADD still holds
            //    only a shared vector lock and is about to replicate its add.
            _ = db.KeyDelete(Key);
            ClassicAssert.IsFalse(db.KeyExists(Key), "key should be gone after UNLINK");

            // 3. Fire the synthetic replication RMW that VectorStoreOps runs after a successful
            //    TryAdd. In production this races in AFTER the UNLINK above tombstoned the key.
            using var localSession = new LocalServerSession(storeWrapper);
            ref var ctx = ref localSession.storageSession.stringBasicContext;
            var keyBytes = Encoding.ASCII.GetBytes(Key);
            var vaddInput = new StringInput(RespCommand.VADD);
            vectorManager.ReplicateVectorSetAdd(keyBytes, ref vaddInput, ref ctx);

            // 4. The tombstoned key must NOT be resurrected by a CU-only synthetic append-log RMW.
            ClassicAssert.IsFalse(
                db.KeyExists(Key),
                "synthetic replication append-log RMW resurrected a tombstoned key as a phantom index record");

            // 5. If any record does exist, it must not be a zeroed index that NeedsRecreate flags forever.
            Span<byte> idx = stackalloc byte[VectorManager.IndexSizeBytes];
            var output = StringOutput.FromPinnedSpan(idx);
            var readInput = new StringInput(RespCommand.VADD);
            var status = localSession.storageSession.Read_MainStore(keyBytes, ref readInput, ref output, ref ctx);
            if (status == GarnetStatus.OK)
            {
                ClassicAssert.IsFalse(
                    vectorManager.NeedsRecreate(output.SpanByteAndMemory.ReadOnlySpan),
                    "resurrected record is a zeroed index wrongly flagged for recreate");
            }
        }
    }
}
