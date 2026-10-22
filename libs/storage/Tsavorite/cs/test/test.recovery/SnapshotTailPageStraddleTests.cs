// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// When the recovery memory budget puts the head cutoff inside the Snapshot's tail page, the object-load pass flushes that page to the
    /// main log. Past the recovered tail, the Snapshot's image of the page holds the fuzzy next-version suffix: records that recovery does
    /// not process and whose object-log position words were never stamped. The flush must stop at the recovered tail.
    /// </summary>
    [TestFixture]
    public class SnapshotTailPageStraddleTests : TestBase
    {
        const long PageSize = 1L << 14;
        const int ValueSize = 8 * 1024;

        sealed class LenientLargeFunctions : TestLargeObjectFunctions
        {
            public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestLargeObjectInput input, ref TestLargeObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata) { }
        }

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown()
        {
            ObjectFlushInjection.Reset();
            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task StraddleFlushOfTailPageStopsAtRecoveredTail([Values] bool recoveryBudget)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("needs the DEBUG flush injection points");

            const int stableCount = 100;
            const int nextVersionCount = 40;
            Guid token;

            var (log, objlog, store) = Prepare();
            try
            {
                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, LenientLargeFunctions>(new LenientLargeFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < stableCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestLargeObjectValue(ValueSize), Empty.Default);
                var tailPage = store.hlogBase.GetPage(store.Log.TailAddress);

                // Hold the Snapshot flush at its first record on the tail page; the page's device write is issued after the walk.
                using var reached = new ManualResetEventSlim(false);
                using var release = new ManualResetEventSlim(false);
                var hookThread = -1;
                var blocked = 0;
                ObjectFlushInjection.Hook = (phase, address) =>
                {
                    if (phase != ObjectFlushPhase.AfterRecordWritten || store.hlogBase.GetPage(address) != tailPage || Interlocked.Exchange(ref blocked, 1) != 0)
                        return;
                    hookThread = Environment.CurrentManagedThreadId;
                    reached.Set();
                    _ = release.Wait(TimeSpan.FromSeconds(30));
                };

                try
                {
                    ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                    Assert.That(reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "Snapshot flush never reached the tail page");
                    Assert.That(hookThread, Is.Not.EqualTo(Environment.CurrentManagedThreadId), "flush hook ran on the test thread");

                    // (v+1) records now fill the tail page behind the recovered tail, before the Snapshot writes that page.
                    var recoveredTail = store._hybridLogCheckpoint.info.recoveredTailAddress;
                    for (var i = 0; i < nextVersionCount; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = 10_000 + i }, new TestLargeObjectValue(ValueSize), Empty.Default);
                    Assert.That(store.hlogBase.GetPage(recoveredTail), Is.EqualTo(tailPage));
                }
                finally
                {
                    release.Set();
                }
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                ObjectFlushInjection.Reset();
                Destroy(log, objlog, store);
            }

            (log, objlog, store) = Prepare();
            try
            {
                if (recoveryBudget)
                {
                    var targetSize = LogSizeTracker.MinTargetPageCount * PageSize;
                    var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null);
                    store.Log.SetLogSizeTracker(tracker);
                }

                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, LenientLargeFunctions>(new LenientLargeFunctions());
                var bContext = session.BasicContext;
                var missing = 0;
                for (var i = 0; i < stableCount; i++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = i }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();
                    if (!status.Found || output.valueObject?.value is not { Length: ValueSize } bytes || bytes[0] != unchecked((byte)ValueSize) || bytes[^1] != unchecked((byte)(ValueSize * 2 - 1)))
                        missing++;
                }
                Assert.That(missing, Is.Zero);
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        static (IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store) Prepare()
        {
            var log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "straddle.log"));
            var objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "straddle.obj.log"));
            var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                ObjectLogDevice = objlog,
                PageSize = PageSize,
                LogMemorySize = 32 * PageSize,
                SegmentSize = 1L << 22,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            return (log, objlog, store);
        }

        static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
    }
}