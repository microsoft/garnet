// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.InputOutputParameterTests
{
    [TestFixture]
    class InputOutputParameterTests
    {
        const int AddValue = 10_000;
        const int MultValue = 100;
        const int NumRecs = 10;

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, UpsertInputFunctions> session;
        private IDevice log;

        internal class UpsertInputFunctions : FunctionsBase<int, int, int, int, Empty>
        {
            internal long lastWriteAddress;

            public override bool ConcurrentReader(ref int key, ref int input, ref int value, ref int output, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = readInfo.Address;
                return SingleReader(ref key, ref input, ref value, ref output, ref readInfo);
            }

            /// <inheritdoc/>
            public override bool SingleReader(ref int key, ref int input, ref int value, ref int output, ref ReadInfo readInfo)
            {
                Assert.AreEqual(key * input, value);
                lastWriteAddress = readInfo.Address;
                output = value + AddValue;
                return true;
            }

            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
                => SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, WriteReason.Upsert, ref recordInfo);

            /// <inheritdoc/>
            public override bool SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                lastWriteAddress = upsertInfo.Address;
                dst = output = src * input;
                return true;
            }
            /// <inheritdoc/>
            public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reasons)
            {
                Assert.AreEqual(lastWriteAddress, upsertInfo.Address);
                Assert.AreEqual(key * input, dst);
                Assert.AreEqual(dst, output);
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
                Assert.AreEqual(lastWriteAddress, rmwInfo.Address);
                Assert.AreEqual(key * input, value);
                Assert.AreEqual(value, output);
            }
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = TestUtils.CreateTestDevice(TestUtils.DeviceType.LocalMemory, Path.Combine(TestUtils.MethodTestDir, "Device.log"));
            store = new TsavoriteKV<int, int>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = store.NewSession<int, int, Empty, UpsertInputFunctions>(new UpsertInputFunctions());
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
        public async Task InputOutputParametersTest([Values] bool useRMW, [Values] bool isAsync)
        {
            int input = MultValue;
            Status status;
            int output = -1;
            bool loading = true;

            async Task doWrites()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    var tailAddress = store.Log.TailAddress;
                    RecordMetadata recordMetadata;
                    if (isAsync)
                    {
                        if (useRMW)
                        {
                            var r = await session.RMWAsync(ref key, ref input);
                            if ((key & 0x1) == 0)
                            {
                                while (r.Status.IsPending)
                                    r = await r.CompleteAsync();
                                status = r.Status;
                                output = r.Output;
                                recordMetadata = r.RecordMetadata;
                            }
                            else
                            {
                                (status, output) = r.Complete(out recordMetadata);
                            }
                        }
                        else
                        {
                            var r = await session.UpsertAsync(ref key, ref input, ref key);
                            if ((key & 0x1) == 0)
                            {
                                while (r.Status.IsPending)
                                    r = await r.CompleteAsync();
                                status = r.Status;
                                output = r.Output;
                                recordMetadata = r.RecordMetadata;
                            }
                            else
                            {
                                (status, output) = r.Complete(out recordMetadata);
                            }
                        }
                    }
                    else
                    {
                        status = useRMW
                            ? session.RMW(ref key, ref input, ref output, out recordMetadata)
                            : session.Upsert(ref key, ref input, ref key, ref output, out recordMetadata);
                    }
                    if (loading)
                    {
                        if (useRMW)
                            Assert.IsFalse(status.Found, status.ToString());
                        else
                            Assert.IsTrue(status.Record.Created, status.ToString());
                        Assert.AreEqual(tailAddress, session.functions.lastWriteAddress);
                    }
                    else
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());

                    Assert.AreEqual(key * input, output);
                    Assert.AreEqual(session.functions.lastWriteAddress, recordMetadata.Address);
                }
            }

            void doReads()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    session.Read(ref key, ref input, ref output);
                    Assert.AreEqual(key * input + AddValue, output);
                }
            }

            // SingleWriter (records do not yet exist)
            await doWrites();
            doReads();

            loading = false;
            input *= input;

            // ConcurrentWriter (update existing records)
            await doWrites();
            doReads();
        }
    }
}