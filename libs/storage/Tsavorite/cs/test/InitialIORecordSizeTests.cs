// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    [TestFixture]
    internal class InitialIORecordSizeTests : TestBase
    {
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        private TsavoriteKV<LongStoreFunctions, LongAllocator> CreateStore(int initialIORecordSize = KVSettings.UseDefaultInitialIORecordSize)
        {
            string filename = Path.Join(MethodTestDir, "test.log");
            log = Devices.CreateLogDevice(filename, deleteOnClose: true);
            var kvSettings = new KVSettings
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                PageSize = 1L << 12,
                LogMemorySize = 1L << 15,
                InitialIORecordSize = initialIORecordSize,
            };
            return new(kvSettings,
                StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [Test]
        public void DefaultIORecordSizeConstantTest()
        {
            // Verify the constant has the expected value
            ClassicAssert.AreEqual(128, IStreamBuffer.DefaultInitialIORecordSize);
        }

        [Test]
        public void UseDefaultInitialIORecordSizeConstantTest()
        {
            // Verify the sentinel constant value
            ClassicAssert.AreEqual(-1, KVSettings.UseDefaultInitialIORecordSize);
        }

        [Test]
        public void StoreLevel_InitialIORecordSizeTest()
        {
            // Store-level InitialIORecordSize should propagate to the TsavoriteKV instance
            using var store = CreateStore(initialIORecordSize: 512);
            ClassicAssert.AreEqual(512, store.InitialIORecordSize);
        }

        [Test]
        public void StoreLevel_DefaultTest()
        {
            // When not specified, store-level should be UseDefaultInitialIORecordSize
            using var store = CreateStore();
            ClassicAssert.AreEqual(KVSettings.UseDefaultInitialIORecordSize, store.InitialIORecordSize);
        }

        [Test]
        public void SessionLevel_InitialIORecordSizeTest()
        {
            // Session-level InitialIORecordSize should be stored in the session context
            using var store = CreateStore();
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(
                new SimpleLongSimpleFunctions(), initialIORecordSize: 1024);

            // The session was created with initialIORecordSize=1024; verify it is accessible through the session context
            ClassicAssert.AreEqual(1024, session.ctx.InitialIORecordSize);
        }

        [Test]
        public void ReadOptions_InitialIORecordSizeTest()
        {
            // ReadOptions should carry the InitialIORecordSize
            var readOptions = new ReadOptions { InitialIORecordSize = 2048 };
            ClassicAssert.AreEqual(2048, readOptions.InitialIORecordSize);

            // Default value should be UseDefaultInitialIORecordSize
            var defaultReadOptions = new ReadOptions();
            ClassicAssert.AreEqual(KVSettings.UseDefaultInitialIORecordSize, defaultReadOptions.InitialIORecordSize);
        }

        [Test]
        public void RMWOptions_InitialIORecordSizeTest()
        {
            // RMWOptions should carry the InitialIORecordSize
            var rmwOptions = new RMWOptions { InitialIORecordSize = 4096 };
            ClassicAssert.AreEqual(4096, rmwOptions.InitialIORecordSize);

            // Default value should be UseDefaultInitialIORecordSize
            var defaultRmwOptions = new RMWOptions();
            ClassicAssert.AreEqual(KVSettings.UseDefaultInitialIORecordSize, defaultRmwOptions.InitialIORecordSize);
        }

        [Test]
        public void NullDevice_SectorSizeTest()
        {
            // NullDevice should use the DefaultDeviceSectorSize
            using var device = new NullDevice();
            ClassicAssert.AreEqual((uint)IDevice.MinDeviceSectorSize, device.SectorSize);
        }

        [Test]
        public void GetExpectedIORecordSize_BasicTest()
        {
            // Minimal record: 8-byte key, 8-byte value, no optionals
            var size = LogRecord.GetExpectedIORecordSize(keyDataLength: 8, valueDataLength: 8);
            // Should be > 0 and aligned to kRecordAlignment (8)
            ClassicAssert.IsTrue(size > 0);
            ClassicAssert.AreEqual(0, size % Constants.kRecordAlignment);

            // RecordInfo(8) + NumIndicatorBytes(3) + KeyLengthBytes(1) + RecordLengthBytes(?) + Key(8) + Value(8)
            // = 8 + 3 + 1 + 1 + 8 + 8 = 29 -> rounded up to 32
            ClassicAssert.AreEqual(32, size);
        }

        [Test]
        public void GetExpectedIORecordSize_WithOptionalsTest()
        {
            // With ETag and Expiration
            var sizeWithOptionals = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: 8,
                expectETag: true, expectExpiration: true);

            var sizeWithout = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: 8);

            // With optionals should be larger by ETagSize + ExpirationSize = 16
            ClassicAssert.AreEqual(sizeWithout + LogRecord.ETagSize + LogRecord.ExpirationSize, sizeWithOptionals);
        }

        [Test]
        public void GetExpectedIORecordSize_WithObjectTest()
        {
            // Object records include ObjectLogPositionSize
            var sizeWithObj = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: ObjectIdMap.ObjectIdSize,
                expectObject: true);

            var sizeWithout = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: ObjectIdMap.ObjectIdSize);

            // Object records add ObjectLogPositionSize (8 bytes)
            ClassicAssert.AreEqual(sizeWithout + LogRecord.ObjectLogPositionSize, sizeWithObj);
        }

        [Test]
        public void GetExpectedIORecordSize_WithNamespaceTest()
        {
            // Extended namespace adds extra bytes
            var sizeWithNs = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: 8, extendedNamespaceLength: 4);

            var sizeWithoutNs = LogRecord.GetExpectedIORecordSize(
                keyDataLength: 8, valueDataLength: 8);

            // Extended namespace of 4 adds 4 bytes; result is still record-aligned
            ClassicAssert.IsTrue(sizeWithNs >= sizeWithoutNs + 4);
            ClassicAssert.AreEqual(0, sizeWithNs % Constants.kRecordAlignment);
        }

        [Test]
        public void GetExpectedIORecordSize_AlignmentTest()
        {
            // All results must be record-aligned
            for (int klen = 1; klen <= 32; klen++)
            {
                for (int vlen = 0; vlen <= 32; vlen++)
                {
                    var size = LogRecord.GetExpectedIORecordSize(klen, vlen);
                    ClassicAssert.AreEqual(0, size % Constants.kRecordAlignment,
                        $"Not aligned for keyLen={klen}, valLen={vlen}, size={size}");
                    ClassicAssert.IsTrue(size > 0,
                        $"Size should be positive for keyLen={klen}, valLen={vlen}");
                }
            }
        }
    }
}
