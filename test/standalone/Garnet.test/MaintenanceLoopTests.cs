// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// The retry contract shared by the background maintenance loops (compaction, object collect and
    /// expired key deletion). A cycle that throws must be logged and retried on the next interval rather
    /// than ending the task, because a terminated task stays dead for the lifetime of the process.
    /// </summary>
    [TestFixture]
    public class MaintenanceLoopTests : TestBase
    {
        /// <summary>Longest any of these loops should need; exceeded only if the loop stopped running.</summary>
        static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

        static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(10);

        [Test]
        public async Task AFailedCycleIsLoggedAndTheLoopKeepsRunning()
        {
            using var cts = new CancellationTokenSource();
            var logger = new RecordingLogger();
            var cycles = 0;
            var reachedSecondCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var loop = StoreWrapper.RunMaintenanceLoopAsync((_) =>
            {
                // Fail the first cycle only; the loop must come back for a second.
                if (Interlocked.Increment(ref cycles) == 1)
                    throw new InvalidOperationException("injected first-cycle failure");

                reachedSecondCycle.TrySetResult();
                return ValueTask.CompletedTask;
            }, Interval, "test task", logger, cts.Token);

            Assert.That(await Task.WhenAny(reachedSecondCycle.Task, Task.Delay(WaitTimeout)), Is.SameAs(reachedSecondCycle.Task),
                "the loop did not run another cycle after one threw");

            await cts.CancelAsync();
            await loop;

            Assert.Multiple(() =>
            {
                Assert.That(logger.Entries, Has.Exactly(1).Matches<LogEntry>(e =>
                    e.Level == LogLevel.Error && e.Exception is InvalidOperationException),
                    "the failed cycle should be logged exactly once, as an error");
                Assert.That(logger.Entries, Has.None.Matches<LogEntry>(e => e.Level == LogLevel.Critical),
                    "a recoverable cycle failure must not be logged as terminal");
            });
        }

        [Test]
        public async Task TheLoopKeepsRunningWhenEveryCycleFails()
        {
            using var cts = new CancellationTokenSource();
            var logger = new RecordingLogger();
            var cycles = 0;
            var reachedThirdCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var loop = StoreWrapper.RunMaintenanceLoopAsync((_) =>
            {
                if (Interlocked.Increment(ref cycles) >= 3)
                    reachedThirdCycle.TrySetResult();
                throw new InvalidOperationException("injected failure");
            }, Interval, "test task", logger, cts.Token);

            Assert.That(await Task.WhenAny(reachedThirdCycle.Task, Task.Delay(WaitTimeout)), Is.SameAs(reachedThirdCycle.Task),
                "repeated failures ended the loop instead of being retried");

            await cts.CancelAsync();
            await loop;

            Assert.That(logger.Entries, Has.None.Matches<LogEntry>(e => e.Level == LogLevel.Critical),
                "repeated recoverable failures must not be logged as terminal");
        }

        [Test]
        public async Task CancellationEndsTheLoopWithoutFaultingOrLogging()
        {
            using var cts = new CancellationTokenSource();
            var logger = new RecordingLogger();
            var ranOnce = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var loop = StoreWrapper.RunMaintenanceLoopAsync((_) =>
            {
                ranOnce.TrySetResult();
                return ValueTask.CompletedTask;
            }, Interval, "test task", logger, cts.Token);

            Assert.That(await Task.WhenAny(ranOnce.Task, Task.Delay(WaitTimeout)), Is.SameAs(ranOnce.Task),
                "the loop never ran a cycle");

            await cts.CancelAsync();

            Assert.That(await Task.WhenAny(loop, Task.Delay(WaitTimeout)), Is.SameAs(loop), "cancellation did not end the loop");
            await loop;

            Assert.Multiple(() =>
            {
                Assert.That(loop.IsCompletedSuccessfully, Is.True, "cancellation must not fault the task");
                Assert.That(logger.Entries, Is.Empty, "cancellation is not a failure and must not be logged");
            });
        }

        /// <summary>
        /// A cycle that observes cancellation and throws for that reason is shutdown, not a failure, so it
        /// must not be logged as a retryable error.
        /// </summary>
        [Test]
        public async Task ACycleCancelledDuringShutdownIsNotLoggedAsAFailure()
        {
            using var cts = new CancellationTokenSource();
            var logger = new RecordingLogger();

            var loop = StoreWrapper.RunMaintenanceLoopAsync(async (cancellationToken) =>
            {
                await cts.CancelAsync();
                cancellationToken.ThrowIfCancellationRequested();
            }, Interval, "test task", logger, cts.Token);

            Assert.That(await Task.WhenAny(loop, Task.Delay(WaitTimeout)), Is.SameAs(loop), "the loop did not end after cancellation");
            await loop;

            Assert.That(logger.Entries, Is.Empty, "a cycle cancelled by shutdown must not be logged as a failure");
        }

        readonly record struct LogEntry(LogLevel Level, Exception Exception);

        sealed class RecordingLogger : ILogger
        {
            readonly List<LogEntry> entries = [];

            internal LogEntry[] Entries
            {
                get
                {
                    lock (entries)
                        return [.. entries];
                }
            }

            public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
                Func<TState, Exception, string> formatter)
            {
                lock (entries)
                    entries.Add(new LogEntry(logLevel, exception));
            }
        }
    }
}