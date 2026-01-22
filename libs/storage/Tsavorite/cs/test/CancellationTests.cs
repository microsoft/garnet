// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Cancellation
{
    // Use an int in these tests just to get a different length underlying the SpanByte
    using IntAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>>;
    using IntStoreFunctions = StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>;

    [TestFixture]
    class CancellationTests
    {
        internal enum CancelLocation
        {
            None,
            NeedInitialUpdate,
            InitialUpdater,
            NeedCopyUpdate,
            CopyUpdater,
            InPlaceUpdater,
            InitialWriter,
            InPlaceWriter
        }

        public class CancellationFunctions : SessionFunctionsBase<int, int, Empty>
        {
            internal CancelLocation cancelLocation = CancelLocation.None;
            internal CancelLocation lastFunc = CancelLocation.None;

            public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.NeedInitialUpdate;
                if (cancelLocation == CancelLocation.NeedInitialUpdate)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return true;
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.NeedCopyUpdate;
                if (cancelLocation == CancelLocation.NeedCopyUpdate)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return true;
            }

            /// <inheritdoc/>
            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.CopyUpdater;
                ClassicAssert.AreNotEqual(CancelLocation.NeedCopyUpdate, cancelLocation);
                if (cancelLocation == CancelLocation.CopyUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo);
            }

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.InitialUpdater;
                ClassicAssert.AreNotEqual(CancelLocation.NeedInitialUpdate, cancelLocation);
                ClassicAssert.AreNotEqual(CancelLocation.InPlaceUpdater, cancelLocation);
                if (cancelLocation == CancelLocation.InitialUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref input), in sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.InPlaceUpdater;
                if (cancelLocation == CancelLocation.InPlaceUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref input), in sizeInfo);
            }

            // Upsert functions
            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> srcValue, ref int output, ref UpsertInfo upsertInfo)
            {
                lastFunc = CancelLocation.InitialWriter;
                if (cancelLocation == CancelLocation.InitialWriter)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> srcValue, ref int output, ref UpsertInfo upsertInfo)
            {
                lastFunc = CancelLocation.InPlaceWriter;
                if (cancelLocation == CancelLocation.InPlaceWriter)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
            }

            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(int) };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref int input)
                => new() { KeySize = key.Length, ValueSize = sizeof(int) };
            /// <inheritdoc/>
            public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref int input)
                => new() { KeySize = key.Length, ValueSize = value.Length };
            /// <inheritdoc/>
            public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref int input)
                => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        }

        IDevice log;
        CancellationFunctions functions;
        TsavoriteKV<IntStoreFunctions, IntAllocator> store;
        ClientSession<int, int, Empty, CancellationFunctions, IntStoreFunctions, IntAllocator> session;
        BasicContext<int, int, Empty, CancellationFunctions, IntStoreFunctions, IntAllocator> bContext;

        const int NumRecs = 100;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                LogMemorySize = 1L << 17,
                PageSize = 1L << 12
            }, StoreFunctions.Create(IntKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new CancellationFunctions();
            session = store.NewSession<int, int, Empty, CancellationFunctions>(functions);
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
            DeleteDirectory(MethodTestDir);
        }

        private unsafe void Populate()
        {
            // Single alloc outside the loop, to the max length we'll need.
            for (int keyNum = 0; keyNum < NumRecs; keyNum++)
            {
                var valueNum = keyNum * NumRecs * 10;
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref valueNum));
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void InitialUpdaterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs, valueNum = keyNum * NumRecs * 10;
            var key = SpanByte.FromPinnedVariable(ref keyNum);

            functions.cancelLocation = CancelLocation.NeedInitialUpdate;
            var status = bContext.RMW(key, ref valueNum);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.NeedInitialUpdate, functions.lastFunc);

            functions.cancelLocation = CancelLocation.InitialUpdater;
            valueNum *= 2;
            status = bContext.RMW(key, ref valueNum);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.InitialUpdater, functions.lastFunc);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void CopyUpdaterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs / 2, valueNum = keyNum * NumRecs * 10;

            void do_it()
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);
                for (int lap = 0; lap < 2; ++lap)
                {
                    functions.cancelLocation = CancelLocation.NeedCopyUpdate;
                    var status = bContext.RMW(key, ref valueNum);
                    ClassicAssert.IsTrue(status.IsCanceled);
                    ClassicAssert.AreEqual(CancelLocation.NeedCopyUpdate, functions.lastFunc);

                    functions.cancelLocation = CancelLocation.CopyUpdater;
                    status = bContext.RMW(key, ref valueNum);
                    ClassicAssert.IsTrue(status.IsCanceled);
                    ClassicAssert.AreEqual(CancelLocation.CopyUpdater, functions.lastFunc);
                }
            }

            // First lap tests readonly space
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            do_it();

            // Second lap tests OnDisk
            store.Log.FlushAndEvict(wait: true);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void InPlaceUpdaterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs / 2, valueNum = keyNum * NumRecs * 10;
            var key = SpanByte.FromPinnedVariable(ref keyNum);

            // Note: ExpirationTests tests the combination of CancelOperation and DeleteRecord
            functions.cancelLocation = CancelLocation.InPlaceUpdater;
            var status = bContext.RMW(key, ref valueNum);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.InPlaceUpdater, functions.lastFunc);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void InitialWriterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs + 1, valueNum = keyNum * NumRecs * 10;
            var key = SpanByte.FromPinnedVariable(ref keyNum);
            var value = SpanByte.FromPinnedVariable(ref valueNum);

            functions.cancelLocation = CancelLocation.InitialWriter;
            var status = bContext.Upsert(key, value);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.InitialWriter, functions.lastFunc);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void InPlaceWriterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs / 2, valueNum = keyNum * NumRecs * 10;
            var key = SpanByte.FromPinnedVariable(ref keyNum);
            var value = SpanByte.FromPinnedVariable(ref valueNum);

            functions.cancelLocation = CancelLocation.InPlaceWriter;
            var status = bContext.Upsert(key, value);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.InPlaceWriter, functions.lastFunc);
        }
    }
}