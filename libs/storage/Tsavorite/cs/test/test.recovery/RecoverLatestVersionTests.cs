// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Covers "recover to the latest checkpoint" when the checkpoint directories are not enumerated newest-first.
    /// <see cref="LocalStorageNamedDeviceFactory.ListContents"/> orders them by directory timestamp, which does not
    /// order checkpoints: the timestamp comes from the system clock, whose file-time granularity is coarse (~15.6ms
    /// on Windows), so two checkpoints taken in quick succession can share one, and ties fall back to whatever order
    /// the filesystem enumerates. Recovery must therefore choose by checkpoint version, not by enumeration order.
    /// </summary>
    [TestFixture]
    public class RecoverLatestVersionTests : TestBase
    {
        private const long FirstValue = 111;
        private const long SecondValue = 222;
        private const long Key = 42;

        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            RecreateDirectory(MethodTestDir);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "RecoverLatestVersion.log"), deleteOnClose: false);
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        private TsavoriteKV<LongStoreFunctions, LongAllocator> CreateStore() => new(new()
        {
            IndexSize = 1L << 13,
            LogDevice = log,
            MutableFraction = 1,
            PageSize = MinKvLogPageSize,
            LogMemorySize = 1L << 20,
            CheckpointDir = MethodTestDir
        }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
            , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
        );

        private static void Upsert(TsavoriteKV<LongStoreFunctions, LongAllocator> store, long value)
        {
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var keyArray = new byte[sizeof(long)];
            var valueArray = new byte[sizeof(long)];
            new Span<byte>(keyArray).AsRef<long>() = Key;
            new Span<byte>(valueArray).AsRef<long>() = value;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(keyArray), new Span<byte>(valueArray));
        }

        private static long Read(TsavoriteKV<LongStoreFunctions, LongAllocator> store)
        {
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;
            var keyArray = new byte[sizeof(long)];
            new Span<byte>(keyArray).AsRef<long>() = Key;

            long output = default;
            var status = bContext.Read(TestSpanByteKey.FromArray(keyArray), ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }
            ClassicAssert.IsTrue(status.Found, $"status = {status}");
            return output;
        }

        /// <summary>Backdate <paramref name="token"/>'s checkpoint directories so the newer checkpoint is enumerated
        /// last, which is what a coarse or tied directory timestamp produces on a real filesystem.</summary>
        private static void BackdateCheckpointDirectories(Guid token)
        {
            var backdated = DateTime.Now.AddMinutes(-10);
            foreach (var basePath in new[] { "cpr-checkpoints", "index-checkpoints" })
            {
                var dir = Path.Join(MethodTestDir, basePath, token.ToString());
                ClassicAssert.IsTrue(Directory.Exists(dir), $"expected checkpoint directory {dir}");
                Directory.SetLastWriteTime(dir, backdated);
            }
        }

        private static int CheckpointDirectoryCount(string basePath)
            => Directory.GetDirectories(Path.Join(MethodTestDir, basePath)).Length;

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoverLatestIgnoresDirTimeOrder(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            Guid secondToken;
            using (var store = CreateStore())
            {
                Upsert(store, FirstValue);
                while (!store.TryInitiateFullCheckpoint(out _, checkpointType))
                    await Task.Yield();
                await store.CompleteCheckpointAsync();

                Upsert(store, SecondValue);
                while (!store.TryInitiateFullCheckpoint(out secondToken, checkpointType))
                    await Task.Yield();
                await store.CompleteCheckpointAsync();
            }

            // Both checkpoints must still be on disk, otherwise the ordering this test covers cannot arise.
            ClassicAssert.AreEqual(2, CheckpointDirectoryCount("cpr-checkpoints"));

            BackdateCheckpointDirectories(secondToken);

            using (var store = CreateStore())
            {
                _ = await store.RecoverAsync();
                ClassicAssert.AreEqual(SecondValue, Read(store), "recovered an older checkpoint than the latest one on disk");
            }
        }

        /// <summary>Recovering to a specific version must still choose that version rather than whichever checkpoint
        /// happens to be enumerated first.</summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoverToVersionPicksThatVersion()
        {
            long firstVersion;
            using (var store = CreateStore())
            {
                Upsert(store, FirstValue);
                while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot))
                    await Task.Yield();
                await store.CompleteCheckpointAsync();
                firstVersion = store.CurrentVersion - 1;

                Upsert(store, SecondValue);
                while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot))
                    await Task.Yield();
                await store.CompleteCheckpointAsync();
            }

            using (var store = CreateStore())
            {
                ClassicAssert.AreEqual(firstVersion, store.GetRecoverVersion(firstVersion));
            }
        }
    }
}