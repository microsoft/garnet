﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    [TestFixture]
    public class NativeReadCacheTests
    {
        private TsavoriteKV<KeyStruct, ValueStruct> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "NativeReadCacheTests.log"), deleteOnClose: true);
            store = new TsavoriteKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings });
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeDiskWriteReadCache()
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                bContext.Upsert(ref key1, ref value, Empty.Default);
            }
            bContext.CompletePending(true);

            // Evict all records from main memory of hybrid log
            store.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };
                bContext.Upsert(ref key1, ref value, Empty.Default);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = bContext.RMW(ref key1, ref input, ref output, Empty.Default);
                if (status.IsPending)
                {
                    bContext.CompletePending(true);
                }
                else
                {
                    Assert.AreEqual(i + 1, output.value.vfield1);
                    Assert.AreEqual(i + 2, output.value.vfield2);
                }
            }

            // Read 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void NativeDiskWriteReadCache2()
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                bContext.Upsert(ref key1, ref value, Empty.Default);
            }
            bContext.CompletePending(true);

            // Dispose the hybrid log from memory entirely
            store.Log.DisposeFromMemory();

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }
    }
}