// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Verifies that recovery rejects a checkpoint whose recorded log geometry does not match the geometry this store is
    /// configured with. PageSize and SegmentSize determine how logical addresses resolve to segment files and offsets, and
    /// ObjectLogSegmentSize is the bit position at which <see cref="ObjectLogFilePositionInfo"/> splits its packed
    /// segment/offset word; recovering under any of the wrong values reads the wrong bytes with nothing else to detect it.
    ///
    /// The downlevel (v7) skip is covered by <see cref="V7DownlevelRecoveryTests"/>: v7 metadata records no geometry, so all
    /// three values read back as zero and would fail every comparison if the version guard were not honored.
    /// </summary>
    [TestFixture]
    public class LogGeometryVerificationTests : TestBase
    {
        const string MainLogName = "geom.log";
        const string ObjectLogName = "geom.obj.log";
        const string CheckpointDirName = "checkpoints";

        const int BasePageSize = MinKvLogPageSize;
        const long BaseSegmentSize = 1L << 20;
        const long BaseObjectLogSegmentSize = 1L << 22;
        const int NumRecords = 8;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        static TsavoriteKV<ObjStoreFunctions, ObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, string checkpointDir,
                int pageSize, long segmentSize, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                SegmentSize = segmentSize,
                ObjectLogSegmentSize = objectLogSegmentSize,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static byte[] MakePayload(int key)
        {
            var v = new byte[64];
            for (var i = 0; i < v.Length; i++)
                v[i] = (byte)((key * 31) + i);
            return v;
        }

        // Writes a current-format FoldOver checkpoint at the base geometry and returns its token.
        Guid BuildCheckpoint()
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, BasePageSize, BaseSegmentSize, BaseObjectLogSegmentSize);
                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var key = 0; key < NumRecords; key++)
                        _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key) });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
            return token;
        }

        // Recovers the checkpoint under the given geometry, returning the TsavoriteException thrown, or null if recovery succeeded.
        async Task<TsavoriteException> TryRecoverWith(Guid token, int pageSize, long segmentSize, long objectLogSegmentSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, pageSize, segmentSize, objectLogSegmentSize);
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                }
                catch (TsavoriteException ex)
                {
                    return ex;
                }
                return null;
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task MatchingGeometryRecovers()
        {
            var token = BuildCheckpoint();
            var ex = await TryRecoverWith(token, BasePageSize, BaseSegmentSize, BaseObjectLogSegmentSize).ConfigureAwait(false);
            ClassicAssert.IsNull(ex, $"recovery at the original geometry must succeed, but threw: {ex?.Message}");
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task MismatchedPageSizeFailsRecovery()
        {
            var token = BuildCheckpoint();
            var ex = await TryRecoverWith(token, BasePageSize * 2, BaseSegmentSize, BaseObjectLogSegmentSize).ConfigureAwait(false);
            ClassicAssert.IsNotNull(ex, "recovery with a mismatched PageSize must fail");
            StringAssert.Contains("PageSize mismatch", ex.Message);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task MismatchedSegmentSizeFailsRecovery()
        {
            var token = BuildCheckpoint();
            var ex = await TryRecoverWith(token, BasePageSize, BaseSegmentSize * 2, BaseObjectLogSegmentSize).ConfigureAwait(false);
            ClassicAssert.IsNotNull(ex, "recovery with a mismatched SegmentSize must fail");
            StringAssert.Contains("SegmentSize mismatch", ex.Message);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task MismatchedObjectLogSegmentSizeFailsRecovery()
        {
            var token = BuildCheckpoint();
            var ex = await TryRecoverWith(token, BasePageSize, BaseSegmentSize, BaseObjectLogSegmentSize * 2).ConfigureAwait(false);
            ClassicAssert.IsNotNull(ex, "recovery with a mismatched ObjectLogSegmentSize must fail");
            StringAssert.Contains("ObjectLogSegmentSize mismatch", ex.Message);
        }
    }
}