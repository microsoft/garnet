// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.InputOutputParameterTests
{
    using IntAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>>;
    using IntStoreFunctions = StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    class InputOutputParameterTests : AllureTestBase
    {
        const int AddValue = 10_000;
        const int MultValue = 100;
        const int NumRecs = 10;

        private TsavoriteKV<IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, Empty, UpsertInputFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, Empty, UpsertInputFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        internal class UpsertInputFunctions : SessionFunctionsBase<int, int, Empty>
        {
            internal long lastWriteAddress;

            /// <inheritdoc/>
            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref ReadInfo readInfo)
            {
                ClassicAssert.AreEqual(srcLogRecord.Key.AsRef<int>() * input, srcLogRecord.ValueSpan.AsRef<int>());
                lastWriteAddress = readInfo.Address;
                output = srcLogRecord.ValueSpan.AsRef<int>() + AddValue;
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> src, ref int output, ref UpsertInfo upsertInfo)
                => InitialWriter(ref logRecord, in sizeInfo, ref input, src, ref output, ref upsertInfo);

            /// <inheritdoc/>
            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> src, ref int output, ref UpsertInfo upsertInfo)
            {
                lastWriteAddress = upsertInfo.Address;
                ref var value = ref logRecord.ValueSpan.AsRef<int>();
                value = output = src.AsRef<int>() * input;
                return true;
            }
            /// <inheritdoc/>
            public override void PostInitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> src, ref int output, ref UpsertInfo upsertInfo)
            {
                ClassicAssert.AreEqual(lastWriteAddress, upsertInfo.Address);
                ref var value = ref dstLogRecord.ValueSpan.AsRef<int>();
                ClassicAssert.AreEqual(dstLogRecord.Key.AsRef<int>() * input, value);
                ClassicAssert.AreEqual(value, output);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
                => InitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                ref var value = ref logRecord.ValueSpan.AsRef<int>();
                value = output = logRecord.Key.AsRef<int>() * input;
                return true;
            }
            /// <inheritdoc/>
            public override void PostInitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                ClassicAssert.AreEqual(lastWriteAddress, rmwInfo.Address);
                ref var value = ref dstLogRecord.ValueSpan.AsRef<int>();
                ClassicAssert.AreEqual(dstLogRecord.Key.AsRef<int>() * input, value);
                ClassicAssert.AreEqual(value, output);
            }

            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(int), ValueIsObject = false };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref int input)
                => new() { KeySize = key.Length, ValueSize = sizeof(int), ValueIsObject = false };
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
                LogMemorySize = 1L << 22,
                SegmentSize = 1L << 22,
                PageSize = 1L << 10
            }, StoreFunctions.Create(IntKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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
                    var upsertOptions = new UpsertOptions();
                    status = useRMW
                        ? bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input, ref output, out var recordMetadata)
                        : bContext.Upsert(SpanByte.FromPinnedVariable(ref key), ref input, SpanByte.FromPinnedVariable(ref key), ref output, ref upsertOptions, out recordMetadata);
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
                    _ = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output);
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