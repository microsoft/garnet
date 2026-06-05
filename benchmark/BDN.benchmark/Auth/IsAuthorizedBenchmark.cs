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
    /// path with the cached <see cref="CoarseUtcNow"/> path, plus the raw clock
    /// primitives for context.
    /// </summary>
    [MemoryDiagnoser]
    public class IsAuthorizedBenchmark
    {
        // Same field set GarnetAadAuthenticator uses on the hot path.
        private bool _authorized;
        private DateTime _validFrom;
        private DateTime _validateTo;

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Simulate a freshly-validated AAD token (~1 hour validity centred on now).
            var now = DateTime.UtcNow;
            _authorized = true;
            _validFrom = now.AddMinutes(-30);
            _validateTo = now.AddMinutes(30);
        }

        // ---- Production-shape methods (verbatim copies from GarnetAadAuthenticator) ----

        [Benchmark(Baseline = true)]
        public bool IsAuthorized_DateTimeUtcNow()
        {
            var now = DateTime.UtcNow;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        [Benchmark]
        public bool IsAuthorized_CoarseUtcNow()
        {
            var now = CoarseUtcNow.Value;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        // ---- Raw clock primitives (for reference) ----

        [Benchmark]
        public DateTime Raw_DateTimeUtcNow() => DateTime.UtcNow;

        [Benchmark]
        public DateTime Raw_CoarseUtcNowValue() => CoarseUtcNow.Value;

        [Benchmark]
        public long Raw_CoarseUtcNowTicks() => CoarseUtcNow.Ticks;
    }
}
