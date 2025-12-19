// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    using ClassAllocator = GenericAllocator<byte[], byte[], StoreFunctions<byte[], byte[], ByteArrayEC, DefaultRecordDisposer<byte[], byte[]>>>;
    using ClassStoreFunctions = StoreFunctions<byte[], byte[], ByteArrayEC, DefaultRecordDisposer<byte[], byte[]>>;

    [AllureNUnit]
    [TestFixture]
    internal class GenericByteArrayTests : AllureTestBase
    {
        private TsavoriteKV<byte[], byte[], ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<byte[], byte[], byte[], byte[], Empty, MyByteArrayFuncs, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<byte[], byte[], byte[], byte[], Empty, MyByteArrayFuncs, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "GenericStringTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "GenericStringTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 14,
                PageSize = 1L << 9
            }, StoreFunctions<byte[], byte[]>.Create(new ByteArrayEC(), () => new ByteArrayBinaryObjectSerializer(), () => new ByteArrayBinaryObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<byte[], byte[], Empty, MyByteArrayFuncs>(new MyByteArrayFuncs());
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
            objlog?.Dispose();
            objlog = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private static byte[] GetByteArray(int i)
        {
            return BitConverter.GetBytes(i);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ByteArrayBasicTest()
        {
            const int totalRecords = 2000;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = GetByteArray(i);
                var _value = GetByteArray(i);
                _ = bContext.Upsert(ref _key, ref _value, Empty.Default);
            }
            _ = bContext.CompletePending(true);

            for (int i = 0; i < totalRecords; i++)
            {
                byte[] input = default;
                byte[] output = default;
                var key = GetByteArray(i);
                var value = GetByteArray(i);

                if (bContext.Read(ref key, ref input, ref output, Empty.Default).IsPending)
                    _ = bContext.CompletePending(true);
                else
                    ClassicAssert.IsTrue(output.SequenceEqual(value));
            }
        }

        class MyByteArrayFuncs : SimpleSimpleFunctions<byte[], byte[]>
        {
            public override void ReadCompletionCallback(ref byte[] key, ref byte[] input, ref byte[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(output.SequenceEqual(key));
            }
        }
    }

    class ByteArrayEC : IKeyComparer<byte[]>
    {
        public bool Equals(ref byte[] k1, ref byte[] k2)
        {
            return k1.SequenceEqual(k2);
        }

        public unsafe long GetHashCode64(ref byte[] k)
        {
            fixed (byte* b = k)
            {
                return Utility.HashBytes(b, k.Length);
            }
        }
    }
}