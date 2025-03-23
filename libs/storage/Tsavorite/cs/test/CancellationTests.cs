// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Cancellation
{
    // Use an int in these tests just to get a different length underlying the SpanByte
    using IntAllocator = SpanByteAllocator<StoreFunctions<SpanByte, IntKeyComparer, SpanByteRecordDisposer>>;
    using IntStoreFunctions = StoreFunctions<SpanByte, IntKeyComparer, SpanByteRecordDisposer>;

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
            SingleWriter,
            ConcurrentWriter
        }

        public class CancellationFunctions : SessionFunctionsBase<SpanByte, int, int, Empty>
        {
            internal CancelLocation cancelLocation = CancelLocation.None;
            internal CancelLocation lastFunc = CancelLocation.None;

            public override bool NeedInitialUpdate(SpanByte key, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.NeedInitialUpdate;
                if (cancelLocation == CancelLocation.NeedInitialUpdate)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return true;
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
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
            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.CopyUpdater;
                ClassicAssert.AreNotEqual(CancelLocation.NeedCopyUpdate, cancelLocation);
                if (cancelLocation == CancelLocation.CopyUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo);
            }

            public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.InitialUpdater;
                ClassicAssert.AreNotEqual(CancelLocation.NeedInitialUpdate, cancelLocation);
                ClassicAssert.AreNotEqual(CancelLocation.InPlaceUpdater, cancelLocation);
                if (cancelLocation == CancelLocation.InitialUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpan(SpanByteFrom(ref input), ref sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                lastFunc = CancelLocation.InPlaceUpdater;
                if (cancelLocation == CancelLocation.InPlaceUpdater)
                {
                    rmwInfo.Action = RMWAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpan(SpanByteFrom(ref input), ref sizeInfo);
            }

            // Upsert functions
            public override bool SingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref int input, SpanByte srcValue, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                lastFunc = CancelLocation.SingleWriter;
                if (cancelLocation == CancelLocation.SingleWriter)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpan(srcValue, ref sizeInfo);
            }

            public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref int input, SpanByte srcValue, ref int output, ref UpsertInfo upsertInfo)
            {
                lastFunc = CancelLocation.ConcurrentWriter;
                if (cancelLocation == CancelLocation.ConcurrentWriter)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                return logRecord.TrySetValueSpan(srcValue, ref sizeInfo);
            }

            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref int input)
                => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = sizeof(int) };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref int input)
                => new() { KeyDataSize = key.Length, ValueDataSize = sizeof(int) };
            /// <inheritdoc/>
            public override RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref int input)
                => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };
        }

        IDevice log;
        CancellationFunctions functions;
        TsavoriteKV<IntStoreFunctions, IntAllocator> store;
        ClientSession<SpanByte, int, int, Empty, CancellationFunctions, IntStoreFunctions, IntAllocator> session;
        BasicContext<SpanByte, int, int, Empty, CancellationFunctions, IntStoreFunctions, IntAllocator> bContext;

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
                MemorySize = 1L << 17,
                PageSize = 1L << 12
            }, StoreFunctions<SpanByte>.Create(IntKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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
                _ = bContext.Upsert(SpanByteFrom(ref keyNum), SpanByteFrom(ref valueNum));
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
            SpanByte key = SpanByteFrom(ref keyNum);

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
            SpanByte key = SpanByteFrom(ref keyNum);

            void do_it()
            {
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
            SpanByte key = SpanByteFrom(ref keyNum);

            // Note: ExpirationTests tests the combination of CancelOperation and DeleteRecord
            functions.cancelLocation = CancelLocation.InPlaceUpdater;
            var status = bContext.RMW(key, ref valueNum);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.InPlaceUpdater, functions.lastFunc);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void SingleWriterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs + 1, valueNum = keyNum * NumRecs * 10;
            SpanByte key = SpanByteFrom(ref keyNum), value = SpanByteFrom(ref valueNum);

            functions.cancelLocation = CancelLocation.SingleWriter;
            var status = bContext.Upsert(key, value);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.SingleWriter, functions.lastFunc);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void ConcurrentWriterTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            Populate();
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            int keyNum = NumRecs / 2, valueNum = keyNum * NumRecs * 10;
            SpanByte key = SpanByteFrom(ref keyNum), value = SpanByteFrom(ref valueNum);

            functions.cancelLocation = CancelLocation.ConcurrentWriter;
            var status = bContext.Upsert(key, value);
            ClassicAssert.IsTrue(status.IsCanceled);
            ClassicAssert.AreEqual(CancelLocation.ConcurrentWriter, functions.lastFunc);
        }
    }
}