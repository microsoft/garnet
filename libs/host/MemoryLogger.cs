// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    /// <summary>
    /// Custom logger that writes logs to memory
    /// </summary>
    internal class MemoryLogger : ILogger
    {
        private readonly List<(LogLevel, Exception, string)> _memoryLog = new();

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            => this._memoryLog.Add((logLevel, exception, formatter(state, exception)));

        /// <summary>
        /// Flushes logger entries into a destination logger.
        /// </summary>
        /// <param name="dstLogger">The logger to which to flush log entries</param>
        public void FlushLogger(ILogger dstLogger)
        {
            foreach (var entry in this._memoryLog)
            {
#pragma warning disable CA2254 // Template should be a static expression
                dstLogger.Log(entry.Item1, entry.Item2, entry.Item3);
#pragma warning restore CA2254 // Template should be a static expression
            }
            this._memoryLog.Clear();
        }
    }

    /// <summary>
    /// Custom logger provider that creates instances of MemoryLogger
    /// </summary>
    internal class MemoryLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, MemoryLogger> _memoryLoggers = new(StringComparer.OrdinalIgnoreCase);

        public ILogger CreateLogger(string categoryName) => this._memoryLoggers.GetOrAdd(categoryName, _ => new MemoryLogger());

        public void Dispose()
        {
            _memoryLoggers.Clear();
        }
    }

    internal static class LoggingBuilderExtensions
    {
        public static ILoggingBuilder AddMemory(this ILoggingBuilder builder) => builder.AddProvider(new MemoryLoggerProvider());
    }
}