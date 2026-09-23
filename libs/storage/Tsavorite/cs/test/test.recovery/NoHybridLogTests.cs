// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.sumstore
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<AdId.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<AdId.Comparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Recovery reports why it could not find a HybridLog token, so that callers can tell a location that was
    /// never checkpointed apart from one whose checkpoints exist but cannot be read.
    /// </summary>
    [TestFixture]
    class NoHybridLogTests : TestBase
    {
        TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        IDevice log;
        string chkptDir;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            chkptDir = Path.Join(MethodTestDir, "ckpt");
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void NoTokensReportsEmptyScan()
        {
            CreateStore();

            var ex = Assert.ThrowsAsync<TsavoriteNoHybridLogException>(async () => await store.RecoverAsync());
            ClassicAssert.AreEqual(0, ex.CandidateTokenCount, "an empty checkpoint location has no candidate tokens");
            ClassicAssert.AreEqual(0, ex.UnreadableTokenCount);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task UnreadableTokenIsReportedAsRejected()
        {
            CreateStore();

            var value = 1L;
            var session = store.NewSession<AdId, AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            _ = session.BasicContext.Upsert(new AdId { adId = 1 }, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            ClassicAssert.IsTrue(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot));
            await store.CompleteCheckpointAsync();
            session.Dispose();

            store.Dispose();
            store = null;
            log.Dispose();
            log = null;

            ZeroCheckpointMetadata();

            CreateStore();
            var ex = Assert.ThrowsAsync<TsavoriteNoHybridLogException>(async () => await store.RecoverAsync());
            ClassicAssert.AreEqual(1, ex.CandidateTokenCount, "the unreadable token must still be counted as a candidate");
            ClassicAssert.AreEqual(1, ex.UnreadableTokenCount);
        }

        private void CreateStore()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "nohlog.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointDir = chkptDir
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        /// <summary>
        /// Overwrites the checkpoint metadata with zeros, preserving its length, so the reader takes a zero record
        /// length from its leading bytes and rejects the file as truncated or corrupt.
        /// </summary>
        private void ZeroCheckpointMetadata()
        {
            var metadata = Directory.GetFiles(Path.Join(chkptDir, "cpr-checkpoints"), "info.dat*", SearchOption.AllDirectories);
            ClassicAssert.AreEqual(1, metadata.Length, "expected exactly one HybridLog checkpoint metadata file");

            var length = new FileInfo(metadata[0]).Length;
            ClassicAssert.Greater(length, 0L);

            using var fs = new FileStream(metadata[0], FileMode.Open, FileAccess.Write, FileShare.None);
            fs.Write(new byte[length]);
        }
    }
}