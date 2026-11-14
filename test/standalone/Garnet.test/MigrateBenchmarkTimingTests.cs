// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

extern alias MigrateBenchmark;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class MigrateBenchmarkTimingTests : TestBase
    {
        [Test]
        public void ReportsStopwatchTicksAsSeconds()
        {
            var logger = new TimingLogger();
            var start = Stopwatch.GetTimestamp() - (2 * Stopwatch.Frequency);
            long totalElapsed = 0;
            var result = MigrateBenchmark::MigrateBench.MigrateRequest.MeasureElapsed(
                start, Task.FromResult(42).GetAwaiter(), "test", ref totalElapsed, verbose: true, logger: logger);

            Assert.That(result, Is.EqualTo(42));
            Assert.That(logger.Seconds, Is.EqualTo((double)totalElapsed / Stopwatch.Frequency).Within(0.000001));
            Assert.That(logger.Seconds, Is.GreaterThanOrEqualTo(2));
        }

        sealed class TimingLogger : ILogger
        {
            public double Seconds { get; private set; }

            public IDisposable BeginScope<TState>(TState state) => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
                => Seconds = (double)((IEnumerable<KeyValuePair<string, object>>)state).Single(pair => pair.Key == "elapsed").Value;
        }
    }
}