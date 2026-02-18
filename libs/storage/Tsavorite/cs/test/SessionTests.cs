// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Session
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class SessionTests : AllureTestBase
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                LogMemorySize = 1L << 29,
            }, StoreFunctions.Create(KeyStruct.Comparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
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
        public void SessionTest1()
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SessionTest2()
        {
            using var session1 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext1 = session1.BasicContext;
            using var session2 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext2 = session2.BasicContext;
            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };
            var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
            var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

            _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value1), Empty.Default);
            _ = bContext2.Upsert(SpanByte.FromPinnedVariable(ref key2), SpanByte.FromPinnedVariable(ref value2), Empty.Default);

            var status = bContext1.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext1.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value1.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value1.vfield2, output.value.vfield2);

            status = bContext2.Read(SpanByte.FromPinnedVariable(ref key2), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext2.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value2.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value2.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SessionTest3()
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            Task.CompletedTask.ContinueWith((t) =>
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }).Wait();
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SessionTest4()
        {
            using var session1 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext1 = session1.BasicContext;
            using var session2 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext2 = session2.BasicContext;
            var t1 = Task.CompletedTask.ContinueWith((t) =>
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
                var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

                _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value1), Empty.Default);
                var status = bContext1.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext1.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value1.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value1.vfield2, output.value.vfield2);
            });

            var t2 = Task.CompletedTask.ContinueWith((t) =>
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
                var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

                _ = bContext2.Upsert(SpanByte.FromPinnedVariable(ref key2), SpanByte.FromPinnedVariable(ref value2), Empty.Default);

                var status = bContext2.Read(SpanByte.FromPinnedVariable(ref key2), ref input, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext2.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value2.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value2.vfield2, output.value.vfield2);
            });

            t1.Wait();
            t2.Wait();
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SessionTest5()
        {
            // Not 'using' as we Dispose and recreate
            var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 16, kfield2 = 17 };
            var value1 = new ValueStruct { vfield1 = 26, vfield2 = 27 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value1), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value1.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value1.vfield2, output.value.vfield2);

            session.Dispose();

            session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            bContext = session.BasicContext;

            var key2 = new KeyStruct { kfield1 = 17, kfield2 = 18 };
            var value2 = new ValueStruct { vfield1 = 27, vfield2 = 28 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key2), SpanByte.FromPinnedVariable(ref value2), Empty.Default);

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key2), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsTrue(status.Found);

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key2), ref input, ref output, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value2.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value2.vfield2, output.value.vfield2);

            session.Dispose();
        }
    }
}