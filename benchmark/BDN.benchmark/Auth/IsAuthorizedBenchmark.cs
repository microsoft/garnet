// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Auth
{
    /// <summary>
    /// Microbenchmark for the <c>GarnetAadAuthenticator.IsAuthorized</c> hot path.
    /// Compares <see cref="DateTime.UtcNow"/>, <see cref="CoarseTimeProvider"/>,
    /// <see cref="Environment.TickCount64"/>, and <see cref="Stopwatch.GetTimestamp"/>.
    /// Monotonic-counter variants project the validity window into their own
    /// time-space once so the hot path is two long compares.
    /// </summary>
    [MemoryDiagnoser]
    public class IsAuthorizedBenchmark
    {
        private bool _authorized;
        private DateTime _validFrom;
        private DateTime _validateTo;
        private long _validFromTicks;
        private long _validToTicks;
        private long _validFromTickMs;
        private long _validToTickMs;
        private long _validFromStopwatchTicks;
        private long _validToStopwatchTicks;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var now = DateTime.UtcNow;
            _authorized = true;
            _validFrom = now.AddMinutes(-30);
            _validateTo = now.AddMinutes(30);
            _validFromTicks = _validFrom.Ticks;
            _validToTicks = _validateTo.Ticks;

            var nowTickMs = Environment.TickCount64;
            _validFromTickMs = nowTickMs + (long)(_validFrom - now).TotalMilliseconds;
            _validToTickMs = nowTickMs + (long)(_validateTo - now).TotalMilliseconds;

            var nowStopwatch = Stopwatch.GetTimestamp();
            _validFromStopwatchTicks = nowStopwatch + (long)((_validFrom - now).TotalSeconds * Stopwatch.Frequency);
            _validToStopwatchTicks = nowStopwatch + (long)((_validateTo - now).TotalSeconds * Stopwatch.Frequency);
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

        [Benchmark]
        public bool IsAuthorized_TickCount64()
        {
            var nowMs = Environment.TickCount64;
            return _authorized && nowMs >= _validFromTickMs && nowMs <= _validToTickMs;
        }

        [Benchmark]
        public bool IsAuthorized_StopwatchTimestamp()
        {
            var nowTicks = Stopwatch.GetTimestamp();
            return _authorized && nowTicks >= _validFromStopwatchTicks && nowTicks <= _validToStopwatchTicks;
        }
    }
}
