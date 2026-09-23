// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.test;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Supplies a fixed slot-to-logical-database mapping, standing in for a host that relabels its
    /// databases (Garnet's SWAPDB).
    /// </summary>
    public class CheckpointManagerWithDatabaseMapping : DeviceLogCommitCheckpointManager
    {
        private readonly int[] mapping;
        private readonly long epoch;

        public CheckpointManagerWithDatabaseMapping(int[] mapping, long epoch, INamedDeviceFactoryCreator deviceFactoryCreator,
            ICheckpointNamingScheme checkpointNamingScheme, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated: false, logger: logger)
        {
            this.mapping = mapping;
            this.epoch = epoch;
        }

        public override int[] GetDatabaseMapping(out long swapEpoch)
        {
            swapEpoch = epoch;
            return mapping;
        }
    }

    /// <summary>
    /// Covers persistence of the host-supplied database mapping in <see cref="HybridLogRecoveryInfo"/>:
    /// that it survives a real checkpoint, that a host which supplies none is unaffected, and that the
    /// version gate keeps downlevel checkpoints readable.
    /// </summary>
    [TestFixture]
    public class DatabaseMappingRecoveryTests : TestBase
    {
        IDevice log;
        DeviceLogCommitCheckpointManager checkpointManager;
        TsavoriteKV<LongStoreFunctions, LongAllocator> store;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            checkpointManager?.Dispose();
            checkpointManager = null;
            log?.Dispose();
            log = null;
            TestUtils.OnTearDown();
        }

        private void CreateStore(DeviceLogCommitCheckpointManager manager)
        {
            checkpointManager = manager;
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog.log"), deleteOnClose: true);
            store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(
                new()
                {
                    IndexSize = 1L << 16,
                    LogDevice = log,
                    MutableFraction = 1,
                    PageSize = TestUtils.MinKvLogPageSize,
                    LogMemorySize = 1L << 20,
                    CheckpointManager = checkpointManager
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        private async Task<Guid> WriteOneCheckpointAsync()
        {
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var key = 1L;
            var value = 42L;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref value));

            ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.FoldOver));
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
            return token;
        }

        private HybridLogRecoveryInfo ReadBack(Guid token)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token);
            ClassicAssert.IsNotNull(metadata);

            HybridLogRecoveryInfo info = new();
            using var reader = new StreamReader(new MemoryStream(metadata));
            info.Initialize(reader);
            return info;
        }

        /// <summary>
        /// The mapping and epoch a host supplies at checkpoint time must come back verbatim.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task DatabaseMappingSurvivesCheckpoint()
        {
            // A three-way permutation, so a pairwise-only implementation cannot pass by accident.
            int[] mapping = [2, 0, 1];
            const long epoch = 7;

            CreateStore(new CheckpointManagerWithDatabaseMapping(mapping, epoch,
                new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(Path.Join(TestUtils.MethodTestDir, "chkpt"))));

            var info = ReadBack(await WriteOneCheckpointAsync().ConfigureAwait(false));

            ClassicAssert.AreEqual(mapping, info.databaseMapping);
            ClassicAssert.AreEqual(epoch, info.swapEpoch);
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, info.hybridLogRecoveryVersion);
        }

        /// <summary>
        /// A host that supplies no mapping records the identity, which is how every existing host
        /// behaves via the default <see cref="DeviceLogCommitCheckpointManager"/> implementation.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task NoDatabaseMappingRecordsIdentity()
        {
            CreateStore(new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(Path.Join(TestUtils.MethodTestDir, "chkpt")), removeOutdated: false));

            var info = ReadBack(await WriteOneCheckpointAsync().ConfigureAwait(false));

            ClassicAssert.IsTrue(info.databaseMapping == null || info.databaseMapping.Length == 0);
            ClassicAssert.AreEqual(0, info.swapEpoch);
        }

        /// <summary>
        /// Per-database checkpoints are taken independently, so each records the epoch in force when it
        /// ran. A reader must see both to be able to pick the most recent.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task EachCheckpointReportsItsOwnEpoch()
        {
            var namingScheme = new DefaultCheckpointNamingScheme(Path.Join(TestUtils.MethodTestDir, "chkpt"));

            CreateStore(new CheckpointManagerWithDatabaseMapping([0, 1], epoch: 3,
                new LocalStorageNamedDeviceFactoryCreator(), namingScheme));
            var stale = ReadBack(await WriteOneCheckpointAsync().ConfigureAwait(false));

            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            CreateStore(new CheckpointManagerWithDatabaseMapping([1, 0], epoch: 4,
                new LocalStorageNamedDeviceFactoryCreator(), namingScheme));
            var fresh = ReadBack(await WriteOneCheckpointAsync().ConfigureAwait(false));

            ClassicAssert.AreEqual(3, stale.swapEpoch);
            ClassicAssert.AreEqual(4, fresh.swapEpoch);
            ClassicAssert.Greater(fresh.swapEpoch, stale.swapEpoch);
            ClassicAssert.AreEqual(new[] { 1, 0 }, fresh.databaseMapping);
        }

        /// <summary>
        /// The mapping and epoch round-trip through the metadata serializer, and the checksum stays
        /// valid whether or not a mapping is present - they are host-supplied and sit outside it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void MetadataRoundTripsWithAndWithoutMapping([Values] bool withMapping)
        {
            HybridLogRecoveryInfo written = new();
            written.Initialize(Guid.NewGuid(), _version: 5);
            written.beginAddress = 64;
            written.finalLogicalAddress = 1024;
            if (withMapping)
            {
                written.databaseMapping = [1, 0];
                written.swapEpoch = 11;
            }

            HybridLogRecoveryInfo read = new();
            using var reader = new StreamReader(new MemoryStream(written.ToByteArray()));
            read.Initialize(reader);

            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, read.hybridLogRecoveryVersion);
            ClassicAssert.AreEqual(written.guid, read.guid);
            ClassicAssert.AreEqual(written.finalLogicalAddress, read.finalLogicalAddress);
            ClassicAssert.AreEqual(written.swapEpoch, read.swapEpoch);
            if (withMapping)
                ClassicAssert.AreEqual(written.databaseMapping, read.databaseMapping);
            else
                ClassicAssert.IsTrue(read.databaseMapping == null || read.databaseMapping.Length == 0);
        }

        /// <summary>
        /// A checkpoint written before the mapping existed must still recover, reporting its own version
        /// and the identity mapping. This is what lets an existing store be read by this build.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void DownlevelMetadataReadsWithDefaults()
        {
            HybridLogRecoveryInfo written = new();
            written.Initialize(Guid.NewGuid(), _version: 5);
            written.beginAddress = 64;
            written.finalLogicalAddress = 1024;

            HybridLogRecoveryInfo read = new();
            using var reader = new StreamReader(new MemoryStream(Downlevel(written.ToByteArray())));
            read.Initialize(reader);

            ClassicAssert.AreEqual(HybridLogRecoveryInfo.MinRecoverableCheckpointVersion, read.hybridLogRecoveryVersion);
            ClassicAssert.AreEqual(written.guid, read.guid);
            ClassicAssert.AreEqual(written.finalLogicalAddress, read.finalLogicalAddress);
            ClassicAssert.IsTrue(read.databaseMapping == null || read.databaseMapping.Length == 0);
            ClassicAssert.AreEqual(0, read.swapEpoch);
        }

        /// <summary>
        /// Versions outside the recoverable range are still rejected, so the tolerant reader has not
        /// become an accept-anything reader.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void UnsupportedVersionsAreRejected(
            [Values(HybridLogRecoveryInfo.MinRecoverableCheckpointVersion - 1, HybridLogRecoveryInfo.CheckpointVersion + 1)] int version)
        {
            HybridLogRecoveryInfo written = new();
            written.Initialize(Guid.NewGuid(), _version: 5);

            var lines = SplitPayload(written.ToByteArray());
            lines[0] = version.ToString();

            HybridLogRecoveryInfo read = new();
            using var reader = new StreamReader(new MemoryStream(JoinPayload(lines)));
            _ = Assert.Throws<TsavoriteException>(() => read.Initialize(reader));
        }

        private static List<string> SplitPayload(byte[] payload)
        {
            var lines = Encoding.UTF8.GetString(payload).Split(Environment.NewLine).ToList();

            // WriteLine terminates the final field, leaving one trailing empty element.
            while (lines.Count > 0 && lines[^1].Length == 0)
                lines.RemoveAt(lines.Count - 1);
            return lines;
        }

        private static byte[] JoinPayload(List<string> lines)
            => Encoding.UTF8.GetBytes(string.Concat(lines.Select(l => l + Environment.NewLine)));

        /// <summary>
        /// Rewrites a payload that carries no mapping as a downlevel one: stamp the older version and
        /// drop the two trailing lines that version does not have. With no mapping those lines are
        /// exactly the mapping length and the swap epoch, so no parsing or searching is needed.
        /// </summary>
        private static byte[] Downlevel(byte[] payload)
        {
            var lines = SplitPayload(payload);
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion.ToString(), lines[0]);
            ClassicAssert.AreEqual("0", lines[^2], "Expected a zero-length mapping");
            ClassicAssert.AreEqual("0", lines[^1], "Expected a zero swap epoch");

            lines[0] = HybridLogRecoveryInfo.MinRecoverableCheckpointVersion.ToString();
            lines.RemoveRange(lines.Count - 2, 2);
            return JoinPayload(lines);
        }
    }
}