// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Auth
{
    /// <summary>
    /// Microbenchmark for the <c>GarnetAadAuthenticator.IsAuthorized</c> hot path.
    /// Compares <see cref="DateTime.UtcNow"/> and <see cref="CoarseTimeProvider"/>.
    /// </summary>
    [MemoryDiagnoser]
    public class IsAuthorizedBenchmark
    {
        private bool _authorized;
        private DateTime _validFrom;
        private DateTime _validateTo;
        private long _validFromTicks;
        private long _validToTicks;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var now = DateTime.UtcNow;
            _authorized = true;
            _validFrom = now.AddMinutes(-30);
            _validateTo = now.AddMinutes(30);
            _validFromTicks = _validFrom.Ticks;
            _validToTicks = _validateTo.Ticks;
        }

        [Benchmark(Baseline = true)]
        public bool IsAuthorized_DateTimeUtcNow()
        {
            var now = DateTime.UtcNow;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        [Benchmark]
        public bool IsAuthorized_CoarseTimeProvider()
        {
            var now = CoarseTimeProvider.Instance.GetUtcNow();
            return _authorized && now.UtcDateTime >= _validFrom && now.UtcDateTime <= _validateTo;
        }

        [Benchmark]
        public bool IsAuthorized_CoarseTimeProviderTicks()
        {
            var nowTicks = CoarseTimeProvider.Instance.GetUtcNow().UtcTicks;
            return _authorized && nowTicks >= _validFromTicks && nowTicks <= _validToTicks;
        }
    }
}