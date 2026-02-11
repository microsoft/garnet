// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.SingleWriter
{
    using IntAllocator = BlittableAllocator<int, int, StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>>;
    using IntStoreFunctions = StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>;

    internal class SingleWriterTestFunctions : SimpleSimpleFunctions<int, int>
    {
        internal WriteReason actualReason;

        public override bool SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            ClassicAssert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
            return true;
        }

        public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            ClassicAssert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
        }
    }

    [AllureNUnit]
    [TestFixture]
    class SingleWriterTests : AllureTestBase
    {
        const int NumRecords = 1000;
        const int ValueMult = 1_000_000;
        const WriteReason NoReason = (WriteReason)255;

        SingleWriterTestFunctions functions;

        private TsavoriteKV<int, int, IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, int, int, Empty, SingleWriterTestFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, int, int, Empty, SingleWriterTestFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);

            functions = new();
            KVSettings<int, int> kvSettings = new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 22,
                ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                CheckpointDir = MethodTestDir
            };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                    {
                        kvSettings.ReadCachePageSize = 1L << 12;
                        kvSettings.ReadCacheMemorySize = 1L << 22;
                        kvSettings.ReadCacheEnabled = true;
                        kvSettings.ReadCopyOptions = default;
                    }
                    break;
                }
            }

            store = new(kvSettings
                , StoreFunctions<int, int>.Create(IntKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, SingleWriterTestFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        void Populate()
        {
            int input = (int)WriteReason.Upsert;
            int output = 0;
            for (int key = 0; key < NumRecords; key++)
                ClassicAssert.False(bContext.Upsert(key, input, key * ValueMult, ref output).IsPending);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void SingleWriterReasonsTest([Values] ReadCopyDestination readCopyDestination)
        {
            functions.actualReason = NoReason;
            Populate();
            ClassicAssert.AreEqual(WriteReason.Upsert, functions.actualReason);

            store.Log.FlushAndEvict(wait: true);

            functions.actualReason = NoReason;
            int key = 42;
            WriteReason expectedReason = readCopyDestination == ReadCopyDestination.ReadCache ? WriteReason.CopyToReadCache : WriteReason.CopyToTail;
            int input = (int)expectedReason;
            var status = bContext.Read(key, input, out int output);
            ClassicAssert.IsTrue(status.IsPending);
            _ = bContext.CompletePending(wait: true);
            ClassicAssert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            key = 64;
            expectedReason = WriteReason.CopyToTail;
            input = (int)expectedReason;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };
            status = bContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending && !status.IsCompleted);
            _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
            ClassicAssert.IsTrue(!status.IsPending && status.IsCompleted && status.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(status.Found && !status.NotFound && status.Record.Copied);
            ClassicAssert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            expectedReason = WriteReason.Compaction;
            input = (int)expectedReason;
            _ = store.Log.Compact<int, int, Empty, SingleWriterTestFunctions>(functions, ref input, ref output, store.Log.SafeReadOnlyAddress, CompactionType.Scan);
            ClassicAssert.AreEqual(expectedReason, functions.actualReason);
        }
    }
}