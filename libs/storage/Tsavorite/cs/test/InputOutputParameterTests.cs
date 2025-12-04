// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.InputOutputParameterTests
{
    using IntAllocator = BlittableAllocator<int, int, StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>>;
    using IntStoreFunctions = StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>;

    [TestFixture]
    class InputOutputParameterTests
    {
        const int AddValue = 10_000;
        const int MultValue = 100;
        const int NumRecs = 10;

        private TsavoriteKV<int, int, IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, int, int, Empty, UpsertInputFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, int, int, Empty, UpsertInputFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        internal class UpsertInputFunctions : SessionFunctionsBase<int, int, int, int, Empty>
        {
            internal long lastWriteAddress;

            /// <inheritdoc/>
            public override bool Reader(ref int key, ref int input, ref int value, ref int output, ref ReadInfo readInfo)
            {
                ClassicAssert.AreEqual(key * input, value);
                lastWriteAddress = readInfo.Address;
                output = value + AddValue;
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
                => InitialWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo);

            /// <inheritdoc/>
            public override bool InitialWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = upsertInfo.Address;
                dst = output = src * input;
                return true;
            }
            /// <inheritdoc/>
            public override void PostInitialWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo)
            {
                ClassicAssert.AreEqual(lastWriteAddress, upsertInfo.Address);
                ClassicAssert.AreEqual(key * input, dst);
                ClassicAssert.AreEqual(dst, output);
            }

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
                => InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

            public override bool InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                value = output = key * input;
                return true;
            }
            /// <inheritdoc/>
            public override void PostInitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo)
            {
                ClassicAssert.AreEqual(lastWriteAddress, rmwInfo.Address);
                ClassicAssert.AreEqual(key * input, value);
                ClassicAssert.AreEqual(value, output);
            }
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = TestUtils.CreateTestDevice(TestUtils.TestDeviceType.LocalMemory, Path.Combine(TestUtils.MethodTestDir, "Device.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MemorySize = 1L << 22,
                SegmentSize = 1L << 22,
                PageSize = 1L << 10
            }, StoreFunctions<int, int>.Create(IntKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, UpsertInputFunctions>(new UpsertInputFunctions());
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        // Simple Upsert test with Input
        [Test]
        [Category(TestUtils.TsavoriteKVTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InputOutputParametersTest([Values] bool useRMW)
        {
            int input = MultValue;
            Status status;
            int output = -1;
            bool loading = true;

            void doWrites()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    var tailAddress = store.Log.TailAddress;
                    status = useRMW
                        ? bContext.RMW(ref key, ref input, ref output, out var recordMetadata)
                        : bContext.Upsert(ref key, ref input, ref key, ref output, out recordMetadata);
                    if (loading)
                    {
                        if (useRMW)
                            ClassicAssert.IsFalse(status.Found, status.ToString());
                        else
                            ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                        ClassicAssert.AreEqual(tailAddress, session.functions.lastWriteAddress);
                    }
                    else
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());

                    ClassicAssert.AreEqual(key * input, output);
                    ClassicAssert.AreEqual(session.functions.lastWriteAddress, recordMetadata.Address);
                }
            }

            void doReads()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    _ = bContext.Read(ref key, ref input, ref output);
                    ClassicAssert.AreEqual(key * input + AddValue, output);
                }
            }

            // SingleWriter (records do not yet exist)
            doWrites();
            doReads();

            loading = false;
            input *= input;

            // InPlaceWriter (update existing records)
            doWrites();
            doReads();
        }
    }
}

#endif // LOGRECORD_TODO