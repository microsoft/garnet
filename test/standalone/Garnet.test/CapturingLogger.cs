// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Garnet.test
{
    /// <summary>
    /// Test <see cref="ILogger"/> that captures each log entry together with its structured state fields, so
    /// tests can assert on structured log data (e.g. a specific <c>reason</c> value) instead of matching
    /// formatted message strings. Thread-safe.
    /// </summary>
    public sealed class CapturingLogger : ILogger
    {
        /// <summary>A single captured log entry.</summary>
        public sealed record Entry(LogLevel Level, string Message, Exception Exception, IReadOnlyDictionary<string, object> State)
        {
            /// <summary>Structured field value as a string, or null when the field is absent.</summary>
            public string Field(string name) => State.TryGetValue(name, out var v) ? v?.ToString() : null;
        }

        private readonly ConcurrentQueue<Entry> entries = new();

        /// <summary>All captured entries, in log order.</summary>
        public IReadOnlyList<Entry> Entries => entries.ToArray();

        /// <summary>
        /// Entries whose structured state contains <paramref name="field"/> equal (by string value) to
        /// <paramref name="value"/>.
        /// </summary>
        public IReadOnlyList<Entry> EntriesWithField(string field, object value)
            => Entries.Where(e => string.Equals(e.Field(field), value?.ToString(), StringComparison.Ordinal)).ToList();

        public IDisposable BeginScope<TState>(TState state) => default!;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            var fields = new Dictionary<string, object>(StringComparer.Ordinal);
            if (state is IReadOnlyList<KeyValuePair<string, object>> kvps)
            {
                foreach (var kvp in kvps)
                    fields[kvp.Key] = kvp.Value;
            }

            entries.Enqueue(new Entry(logLevel, formatter(state, exception), exception, fields));
        }
    }

    /// <summary>
    /// <see cref="ILoggerProvider"/> that returns a single shared <see cref="CapturingLogger"/> for every
    /// category, so it can be attached to an existing <see cref="ILoggerFactory"/> (e.g. a multi-node cluster
    /// test's shared factory) to capture structured log entries emitted by any component.
    /// </summary>
    public sealed class CapturingLoggerProvider(CapturingLogger logger) : ILoggerProvider
    {
        private readonly CapturingLogger logger = logger;

        public ILogger CreateLogger(string categoryName) => logger;

        public void Dispose() { }
    }
}