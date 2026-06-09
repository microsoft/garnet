// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for <see cref="CoarseTimeProvider"/>. The cache and its refresh timer
    /// are static (process-wide, always wired to <see cref="TimeProvider.System"/>), so
    /// instances are functionally indistinguishable and refresh is only observable via a
    /// real wall-clock wait.
    /// </summary>
    [TestFixture]
    public class CoarseTimeProviderTests : TestBase
    {
        [Test]
        public void System_IsSingleton()
        {
            ClassicAssert.AreSame(CoarseTimeProvider.Instance, CoarseTimeProvider.Instance);
            ClassicAssert.IsNotNull(CoarseTimeProvider.Instance);
        }

        [Test]
        public void Constructor_IsPublicAndAllocatable()
        {
            // Public parameterless ctor must exist — its sole purpose is to let callers
            // hand the resulting instance to APIs that take a TimeProvider abstract base.
            var instance = new CoarseTimeProvider();
            ClassicAssert.IsNotNull(instance);
            ClassicAssert.IsInstanceOf<TimeProvider>(instance);
        }

        [Test]
        public void MultipleInstances_ShareTheSameCachedTime()
        {
            // Independent instances must observe the same static cache snapshot —
            // the whole point of the type is process-wide amortization.
            var a = new CoarseTimeProvider();
            var b = new CoarseTimeProvider();

            for (var i = 0; i < 10; i++)
            {
                ClassicAssert.AreEqual(a.GetUtcNow().UtcTicks, b.GetUtcNow().UtcTicks, "Instances must read from the shared static cache");
                ClassicAssert.AreEqual(a.GetUtcNow().UtcTicks, CoarseTimeProvider.Instance.GetUtcNow().UtcTicks);
            }
        }

        [Test]
        public void GetUtcNow_IsCloseToWallClock()
        {
            // Cache lag is bounded by RefreshPeriod (~1s) plus Timer slack; pad generously
            // for CI ThreadPool starvation but assert it isn't wildly off (e.g., wrong epoch).
            var wall = DateTimeOffset.UtcNow;
            var cached = CoarseTimeProvider.Instance.GetUtcNow();
            var skew = (cached - wall).Duration();
            ClassicAssert.LessOrEqual(skew, TimeSpan.FromSeconds(30), $"Cached time skew {skew} from wall clock is unreasonable");
        }

        [Test]
        public void UtcNow_KindIsUtc()
        {
            ClassicAssert.AreEqual(DateTimeKind.Utc, CoarseTimeProvider.Instance.UtcNow.Kind);
        }

        [Test]
        public void AccessorsAgreeAtSnapshot()
        {
            // UtcNow.Ticks and GetUtcNow().UtcTicks must read the same underlying cached
            // value. Loop to catch a tick-boundary refresh between reads.
            for (var i = 0; i < 32; i++)
            {
                var utcNow = CoarseTimeProvider.Instance.UtcNow.Ticks;
                var getUtcNow = CoarseTimeProvider.Instance.GetUtcNow().UtcTicks;
                if (utcNow == getUtcNow) return;
            }
            Assert.Fail("Accessors never agreed across 32 reads — cache is unstable.");
        }

        [Test]
        public void VirtualGetUtcNow_ReturnsCachedValue()
        {
            // The point of inheriting from TimeProvider is that callers can up-cast to
            // the abstract base and still hit the cached path through the virtual dispatch.
            TimeProvider asBase = CoarseTimeProvider.Instance;
            var viaVirtual = asBase.GetUtcNow().UtcTicks;
            var viaInstance = CoarseTimeProvider.Instance.GetUtcNow().UtcTicks;
            ClassicAssert.AreEqual(viaInstance, viaVirtual, "Virtual dispatch must hit the cached override, not the base");
        }

        [Test]
        public void TimestampFrequency_DelegatesToSystem()
        {
            // Only GetUtcNow is coarse; monotonic clock / timestamp surface must passthrough.
            ClassicAssert.AreEqual(TimeProvider.System.TimestampFrequency, CoarseTimeProvider.Instance.TimestampFrequency);
        }

        [Test]
        public void LocalTimeZone_DelegatesToSystem()
        {
            ClassicAssert.AreEqual(TimeProvider.System.LocalTimeZone, CoarseTimeProvider.Instance.LocalTimeZone);
        }

        [Test]
        public void CreateTimer_DelegatesToSystem()
        {
            // CreateTimer must passthrough — we don't wrap timer creation.
            using var timer = CoarseTimeProvider.Instance.CreateTimer(static _ => { }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            ClassicAssert.IsNotNull(timer);
        }

        [Test]
        public void GetTimestamp_MonotonicallyAdvances()
        {
            var t0 = CoarseTimeProvider.Instance.GetTimestamp();
            Thread.SpinWait(10_000);
            var t1 = CoarseTimeProvider.Instance.GetTimestamp();
            ClassicAssert.GreaterOrEqual(t1, t0, "Monotonic timestamp must not go backwards");
        }

        [Test, Explicit("Real-time test (~3s wait); run manually when modifying the refresh path.")]
        public void Cache_RefreshesOverRealTime()
        {
            // Static-state design wires the cache to TimeProvider.System, so the only way to
            // observe a refresh is a real wall-clock wait. RefreshPeriod is 1s — sleep 3s
            // to absorb Timer slack and ThreadPool latency. We only assert forward progress
            // (no upper bound) so CI ThreadPool starvation cannot turn this into a flake.
            var before = CoarseTimeProvider.Instance.GetUtcNow().UtcTicks;
            Thread.Sleep(TimeSpan.FromSeconds(3));
            var after = CoarseTimeProvider.Instance.GetUtcNow().UtcTicks;
            ClassicAssert.Greater(after, before, "Cache must advance after >1 refresh period");
        }
    }
}
