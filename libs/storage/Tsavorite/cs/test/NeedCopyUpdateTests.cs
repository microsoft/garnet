// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class NeedCopyUpdateTests
    {
        private TsavoriteKV<int, RMWValue> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/tests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/tests.obj.log", deleteOnClose: true);

            store = new TsavoriteKV<int, RMWValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                serializerSettings: new SerializerSettings<int, RMWValue> { valueSerializer = () => new RMWValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void TryAddTest()
        {
            TryAddTestFunctions functions = new();
            using var session = store.NewSession<RMWValue, RMWValue, Status, TryAddTestFunctions>(functions);

            Status status;
            var key = 1;
            var value1 = new RMWValue { value = 1 };
            var value2 = new RMWValue { value = 2 };

            functions.noNeedInitialUpdater = true;
            status = session.RMW(ref key, ref value1); // needInitialUpdater false + NOTFOUND
            Assert.IsFalse(status.Found, status.ToString());
            Assert.IsFalse(value1.flag); // InitialUpdater is not called
            functions.noNeedInitialUpdater = false;

            status = session.RMW(ref key, ref value1); // InitialUpdater + NotFound
            Assert.IsFalse(status.Found, status.ToString());
            Assert.IsTrue(value1.flag); // InitialUpdater is called

            status = session.RMW(ref key, ref value2); // InPlaceUpdater + Found
            Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());

            store.Log.Flush(true);
            status = session.RMW(ref key, ref value2); // NeedCopyUpdate returns false, so RMW returns simply Found
            Assert.IsTrue(status.Found, status.ToString());

            store.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, new(StatusCode.Found), 0); // PENDING + NeedCopyUpdate + Found
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var outputs, true);

            var output = new RMWValue();
            (status, output) = GetSinglePendingResult(outputs);
            Assert.IsTrue(status.Found, status.ToString()); // NeedCopyUpdate returns false, so RMW returns simply Found

            // Test stored value. Should be value1
            status = session.Read(ref key, ref value1, ref output, new(StatusCode.Found), 0);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePending(true);

            status = session.Delete(ref key);
            Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            session.CompletePending(true);
            store.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, new(StatusCode.NotFound | StatusCode.CreatedRecord), 0); // PENDING + InitialUpdater + NOTFOUND
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePending(true);
        }

        internal class RMWValue
        {
            public int value;
            public bool flag;
        }

        internal class RMWValueSerializer : BinaryObjectSerializer<RMWValue>
        {
            public override void Serialize(ref RMWValue value)
            {
                writer.Write(value.value);
            }

            public override void Deserialize(out RMWValue value)
            {
                value = new RMWValue
                {
                    value = reader.ReadInt32()
                };
            }
        }

        internal class TryAddTestFunctions : TryAddFunctions<int, RMWValue, Status>
        {
            internal bool noNeedInitialUpdater;

            public override bool NeedInitialUpdate(ref int key, ref RMWValue input, ref RMWValue output, ref RMWInfo rmwInfo)
            {
                return !noNeedInitialUpdater && base.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);
            }

            public override bool InitialUpdater(ref int key, ref RMWValue input, ref RMWValue value, ref RMWValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                input.flag = true;
                base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                return true;
            }

            public override bool CopyUpdater(ref int key, ref RMWValue input, ref RMWValue oldValue, ref RMWValue newValue, ref RMWValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                Assert.Fail("CopyUpdater");
                return false;
            }

            public override void RMWCompletionCallback(ref int key, ref RMWValue input, ref RMWValue output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.AreEqual(ctx, status);

                if (!status.Found)
                    Assert.IsTrue(input.flag); // InitialUpdater is called.
            }

            public override void ReadCompletionCallback(ref int key, ref RMWValue input, ref RMWValue output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.AreEqual(output.value, input.value);
            }
        }
    }

    [TestFixture]
    internal class NeedCopyUpdateTestsSinglePage
    {
        private TsavoriteKV<long, long> store;
        private IDevice log;

        const int pageSizeBits = 16;
        const int recsPerPage = (1 << pageSizeBits) / 24;   // 24 bits in RecordInfo, key, value

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);

            store = new TsavoriteKV<long, long>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = pageSizeBits, PageSizeBits = pageSizeBits });
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CopyUpdateFromHeadReadOnlyPageTest()
        {
            RMWSinglePageFunctions functions = new();
            using var session = store.NewSession<long, long, Empty, RMWSinglePageFunctions>(functions);

            // Two records is the most that can "fit" into the first Constants.kFirstValueAddress "range"; therefore when we close pages
            // after flushing, ClosedUntilAddress will be aligned with the end of the page, so we will succeed in the allocation that
            // caused the HeadAddress to be moved above logicalAddress in CreateNewRecordRMW.
            const int padding = 2;

            for (int key = 0; key < recsPerPage - padding; key++)
            {
                var status = session.RMW(key, key << 32 + key);
                Assert.IsTrue(status.IsCompletedSuccessfully, status.ToString());
            }

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // This should trigger CopyUpdater, after flushing the oldest page (closest to HeadAddress).
            for (int key = 0; key < recsPerPage - padding; key++)
            {
                var status = session.RMW(key, key << 32 + key);
                if (status.IsPending)
                    session.CompletePending(wait: true);
            }
        }

        internal class RMWSinglePageFunctions : SimpleFunctions<long, long>
        {
        }
    }
}