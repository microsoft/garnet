// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class CoarseTimeProviderTests : TestBase
    {
        [Test]
        public void UtcNowIsCloseToWallClock()
        {
            var wallClock = DateTime.UtcNow;
            var coarseUtcNow = CoarseTimeProvider.Instance.UtcNow;
            var coarseGetUtcNow = CoarseTimeProvider.Instance.GetUtcNow().UtcDateTime;

            // Both accessors should be within a few refresh periods of the wall clock.
            // Generous bound to avoid flakes on slow CI.
            var bound = CoarseTimeProvider.RefreshPeriod * 3;
            ClassicAssert.LessOrEqual((wallClock - coarseUtcNow).Duration(), bound, "UtcNow drift exceeded bound");
            ClassicAssert.LessOrEqual((wallClock - coarseGetUtcNow).Duration(), bound, "GetUtcNow drift exceeded bound");
        }

        [Test]
        public void ValueRefreshesAfterRefreshPeriod()
        {
            var before = CoarseTimeProvider.Instance.UtcNow;

            Thread.Sleep(CoarseTimeProvider.RefreshPeriod * 2);

            var after = CoarseTimeProvider.Instance.UtcNow;

            ClassicAssert.Greater(after, before, "Coarse clock did not advance after waiting");
        }
    }
}
