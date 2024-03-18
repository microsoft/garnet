// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.test
{
    public class NUnitLoggerProvider : ILoggerProvider
    {
        private readonly TextWriter textWriter;
        private readonly string scope;

        static readonly string[] lvl = new string[]
        {
            "trce",
            "dbug",
            "info",
            "warn",
            "errr",
            "crit",
        };

        public NUnitLoggerProvider(TextWriter textWriter, string scope = "")
        {
            this.textWriter = textWriter;
            this.scope = scope;
        }

        public ILogger CreateLogger(string categoryName) => new NUnitLogger(categoryName, textWriter, scope);

        public void Dispose()
        { }

        private class NUnitLogger : ILogger
        {
            private readonly string categoryName;
            private readonly TextWriter textWriter;
            private readonly string scope;
            private int loggerId;

            public NUnitLogger(string categoryName, TextWriter textWriter, string scope)
            {
                this.categoryName = $"{categoryName}:{Interlocked.Increment(ref loggerId)}";
                this.textWriter = textWriter;
                this.scope = scope;
            }

            public IDisposable BeginScope<TState>(TState state) => default!;

            public bool IsEnabled(LogLevel logLevel) => true;

            private string GetLevelStr(LogLevel ll) => lvl[(int)ll];

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                var msg = string.Format("[{0:D3}.{1}.({2})] |{3}| <{4}> {5} ^{6}^",
                    eventId.Id,
                    LogFormatter.FormatDate(DateTime.UtcNow),
                    GetLevelStr(logLevel),
                    scope,
                    categoryName,
                    exception,
                    state);

                textWriter.Write(msg);
            }
        }
    }
}