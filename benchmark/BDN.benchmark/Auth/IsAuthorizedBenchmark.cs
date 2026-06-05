// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Auth
{
    /// <summary>
    /// Microbenchmark mirroring <c>GarnetAadAuthenticator.IsAuthorized()</c> — the
    /// hot path checked twice per command via <c>IsAuthenticated</c> on
    /// AAD-authenticated sessions. Compares the legacy <see cref="DateTime.UtcNow"/>
    /// path against the cached <see cref="CoarseDateTime"/> path and an
    /// <see cref="Environment.TickCount64"/>-based variant (suggested in PR review:
    /// monotonic ms-since-boot, no Timer thread, no static state).
    /// </summary>
    [MemoryDiagnoser]
    public class IsAuthorizedBenchmark
    {
        // Same field set GarnetAadAuthenticator uses on the hot path.
        private bool _authorized;
        private DateTime _validFrom;
        private DateTime _validateTo;
        // Tick projections of the validity window for the Ticks-based variant.
        private long _validFromTicks;
        private long _validToTicks;
        // TickCount64 ms-since-boot projections of the validity window. TickCount64
        // is monotonic but unrelated to wall-clock; converting once at token-validation
        // time lets the hot path do two long compares without any wall-clock read.
        private long _validFromTickMs;
        private long _validToTickMs;

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Simulate a freshly-validated AAD token (~1 hour validity centred on now).
            var now = DateTime.UtcNow;
            _authorized = true;
            _validFrom = now.AddMinutes(-30);
            _validateTo = now.AddMinutes(30);
            _validFromTicks = _validFrom.Ticks;
            _validToTicks = _validateTo.Ticks;

            // Project the validity window into TickCount64-space at "Authenticate" time.
            var nowTickMs = Environment.TickCount64;
            _validFromTickMs = nowTickMs + (long)(_validFrom - now).TotalMilliseconds;
            _validToTickMs = nowTickMs + (long)(_validateTo - now).TotalMilliseconds;
        }

        [Benchmark(Baseline = true)]
        public bool IsAuthorized_DateTimeUtcNow()
        {
            var now = DateTime.UtcNow;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        [Benchmark]
        public bool IsAuthorized_CoarseDateTime()
        {
            var now = CoarseDateTime.UtcNow;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        [Benchmark]
        public bool IsAuthorized_CoarseDateTimeTicks()
        {
            var nowTicks = CoarseDateTime.UtcNowTicks;
            return _authorized && nowTicks >= _validFromTicks && nowTicks <= _validToTicks;
        }

        [Benchmark]
        public bool IsAuthorized_TickCount64()
        {
            var nowMs = Environment.TickCount64;
            return _authorized && nowMs >= _validFromTickMs && nowMs <= _validToTickMs;
        }
    }
}
