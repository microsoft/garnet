// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.test;
using NUnit.Framework;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.stress
{
    [TestFixture]
    class LongStressChainTests : TestBase
    {
        private readonly ReadCacheTests.LongStressChainTests worker = new();

        [SetUp]
        public void Setup() => worker.Setup();

        [TearDown]
        public void TearDown() => worker.TearDown();

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter (modRange is used by worker.Setup())
        public void LongRcMultiThreadStressTest([Values] HashModulo modRange, [Values(1, 8)] int numReadThreads, [Values(1, 8)] int numWriteThreads,
                                                [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
#pragma warning restore IDE0060 // Remove unused parameter
            => worker.LongRcMultiThreadWorker(numReadThreads, numWriteThreads, updateOp);
    }

    [TestFixture]
    class SpanByteStressChainTests : TestBase
    {
        private readonly ReadCacheTests.SpanByteStressChainTests worker = new();

        [SetUp]
        public void Setup() => worker.Setup();

        [TearDown]
        public void TearDown() => worker.TearDown();

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter (modRange is used by worker.Setup())
        public void SpanByteRcMultiThreadStressTest([Values] HashModulo modRange, [Values(1, 8)] int numReadThreads, [Values(1, 8)] int numWriteThreads,
                                                    [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
#pragma warning restore IDE0060 // Remove unused parameter
            => worker.SpanByteRcMultiThreadWorker(numReadThreads, numWriteThreads, updateOp);
    }
}