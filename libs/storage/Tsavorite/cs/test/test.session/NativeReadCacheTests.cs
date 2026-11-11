// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>;
    [TestFixture]
    public class NativeReadCacheTests : TestBase
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "NativeReadCacheTests.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = TestUtils.MinKvLogPageSize,
                ReadCacheMemorySize = 1L << 17,
                ReadCachePageSize = TestUtils.MinKvLogPageSize,
                ReadCacheEnabled = true
            }, StoreFunctions.Create(new KeyStruct.Comparer(), SpanByteRecordTriggers.Instance)
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
            TestUtils.OnTearDown();
        }

        class SecondChanceFunctions : Functions
        {
            internal bool cancelRead;
            internal Action afterRead;

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
            {
                if (cancelRead)
                {
                    readInfo.Action = ReadAction.CancelOperation;
                    return false;
                }
                var result = base.Reader(in srcLogRecord, ref input, ref output, ref readInfo);
                var callback = afterRead;
                afterRead = null;
                callback?.Invoke();
                return result;
            }

            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (!cancelRead)
                    base.ReadCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);
            }
        }

        /// <summary>
        /// Verifies that abandoning a second-chance copy preserves the successful read and leaves the cache usable.
        /// </summary>
        /// <param name="pending">Complete a disk read that discovers a cached copy, rather than reading the cache directly.</param>
        /// <param name="evictSource">Advance the eviction frontier past the source; otherwise let a competing promotion win the head CAS.</param>
        [Test]
        [Category("TsavoriteKV")]
        public void ReadCacheSecondChanceAbandonsCopy([Values] bool pending, [Values] bool evictSource)
        {
            // Separate sessions let a pending reader discover a cache entry populated after its disk read began.
            // Only the reader under test injects an action between consuming the value and attempting promotion.
            using var populationSession = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var populationContext = populationSession.BasicContext;
            var functions = new SecondChanceFunctions();
            using var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, SecondChanceFunctions>(functions,
                readCopyOptions: new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache));
            var context = session.BasicContext;
            var key = new KeyStruct { kfield1 = 42, kfield2 = 43 };
            var value = new ValueStruct { vfield1 = 42, vfield2 = 43 };
            InputStruct input = default;
            OutputStruct output = default;

            // Persist the source, optionally start the pending read, then populate the cache and make it eligible for second chance.
            ClassicAssert.IsTrue(populationContext.Upsert(key, SpanByte.FromPinnedVariable(ref value)).Record.Created);
            store.Log.FlushAndEvict(wait: true);
            if (pending)
                ClassicAssert.IsTrue(context.Read(key, ref input, ref output).IsPending);

            ClassicAssert.IsTrue(populationContext.Read(key, ref input, ref output).IsPending);
            populationContext.CompletePending(wait: true);
            store.ReadCache.ShiftReadOnlyAddress(store.ReadCache.TailAddress, wait: true);

            // Advancing HeadAddress makes allocation reject the source; wait:false avoids waiting for eviction
            // while this read holds the epoch. Alternatively, a concurrent reader changes the hash-entry head,
            // making this read's promotion CAS fail. A reader can share our lock; a writer would block on it.
            functions.afterRead = evictSource
                ? () => store.ReadCache.ShiftHeadAddress(store.ReadCache.TailAddress, wait: false)
                : () => Task.Run(() =>
                {
                    using var competingSession = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions(),
                        readCopyOptions: new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache));
                    OutputStruct competingOutput = default;
                    ClassicAssert.IsTrue(competingSession.BasicContext.Read(key, ref competingOutput).Record.CopiedToReadCache);
                }).GetAwaiter().GetResult();

            // Exercise the same injected failure through either direct lookup or pending-read completion.
            Status status;
            if (pending)
            {
                context.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = TestUtils.GetSinglePendingResult(outputs);
            }
            else
                status = context.Read(key, ref input, ref output);

            // Promotion is best-effort: abandoning it must not discard the value already read.
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.IsFalse(status.Record.CopiedToReadCache, status.ToString());
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);

            // Eviction requires a fresh disk read; a competing promotion leaves a usable cache hit.
            status = context.Read(key, ref input, ref output);
            ClassicAssert.AreEqual(evictSource, status.IsPending, status.ToString());
            if (status.IsPending)
            {
                context.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = TestUtils.GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ReadCacheSecondChance([Values] bool pending, [Values] bool readOnly, [Values] bool cancelRead,
            [Values(ReadCopyFrom.None, ReadCopyFrom.Device, ReadCopyFrom.AllImmutable)] ReadCopyFrom copyFrom)
        {
            using var populationSession = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var populationContext = populationSession.BasicContext;
            var functions = new SecondChanceFunctions { cancelRead = cancelRead };
            using var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, SecondChanceFunctions>(functions,
                readCopyOptions: new(copyFrom, ReadCopyTo.ReadCache));
            var context = session.BasicContext;
            var key = new KeyStruct { kfield1 = 42, kfield2 = 43 };
            var value = new ValueStruct { vfield1 = 42, vfield2 = 43 };
            InputStruct input = default;
            OutputStruct output = default;

            ClassicAssert.IsTrue(populationContext.Upsert(key, SpanByte.FromPinnedVariable(ref value)).Record.Created);
            store.Log.FlushAndEvict(wait: true);

            // Leave this session's disk read pending while another session populates the cache.
            if (pending)
                ClassicAssert.IsTrue(context.Read(key, ref input, ref output).IsPending);

            ClassicAssert.IsTrue(populationContext.Read(key, ref input, ref output).IsPending);
            populationContext.CompletePendingWithOutputs(out var populationOutputs, wait: true);
            var (populationStatus, _) = TestUtils.GetSinglePendingResult(populationOutputs);
            ClassicAssert.IsTrue(populationStatus.Record.CopiedToReadCache);

            if (readOnly)
                store.ReadCache.ShiftReadOnlyAddress(store.ReadCache.TailAddress, wait: true);

            var cacheTail = store.ReadCache.TailAddress;
            var originalCacheTail = cacheTail;
            var mainTail = store.Log.TailAddress;
            Status status;
            if (pending)
            {
                context.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = TestUtils.GetSinglePendingResult(outputs);
            }
            else
                status = context.Read(key, ref input, ref output);

            ClassicAssert.AreEqual(cancelRead, status.IsCanceled, status.ToString());
            ClassicAssert.AreEqual(!cancelRead, status.Found, status.ToString());
            var expectCopy = readOnly && !cancelRead && copyFrom == ReadCopyFrom.AllImmutable;
            ClassicAssert.AreEqual(expectCopy, status.Record.CopiedToReadCache, status.ToString());
            ClassicAssert.AreEqual(expectCopy, store.ReadCache.TailAddress > cacheTail);
            ClassicAssert.AreEqual(mainTail, store.Log.TailAddress);
            if (cancelRead)
                return;

            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);

            // A second hit either uses the fresh copy or retains the no-copy policy.
            cacheTail = store.ReadCache.TailAddress;
            status = context.Read(key, ref input, ref output);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.IsFalse(status.Record.CopiedToReadCache, status.ToString());
            ClassicAssert.AreEqual(cacheTail, store.ReadCache.TailAddress);

            if (expectCopy)
            {
                store.ReadCache.ShiftHeadAddress(originalCacheTail, wait: true);
                status = context.Read(key, ref input, ref output);
                ClassicAssert.IsTrue(status.Found && !status.IsPending, status.ToString());
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeDiskWriteReadCache()
        {
            using var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }
            _ = bContext.CompletePending(true);

            // Evict all records from main memory of hybrid log
            store.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"i == {i}");
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1, $"i == {i}");
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2, $"i == {i}");
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };
                _ = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = bContext.RMW(key1, ref input, ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePending(true);
                }
                else
                {
                    ClassicAssert.AreEqual(i + 1, output.value.vfield1);
                    ClassicAssert.AreEqual(i + 2, output.value.vfield2);
                }
            }

            // Read 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void NativeDiskWriteReadCache2()
        {
            using var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }
            _ = bContext.CompletePending(true);

            store.Log.FlushAndEvict(wait: true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }
    }
}