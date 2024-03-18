// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.test
{
    public class NUnitLoggerProvider : ILoggerProvider
    {
        private readonly TextWriter textWriter;
        private readonly string scope;
        private readonly HashSet<string> skipCmd;
        private readonly bool recvOnly;
        private readonly bool matchLevel;
        private readonly LogLevel logLevel;

        static readonly string[] lvl = new string[]
        {
            "trce",
            "dbug",
            "info",
            "warn",
            "errr",
            "crit",
        };

        public NUnitLoggerProvider(TextWriter textWriter, string scope = "", HashSet<string> skipCmd = null, bool recvOnly = false, bool matchLevel = false, LogLevel logLevel = LogLevel.None)
        {
            this.textWriter = textWriter;
            this.scope = scope;
            this.skipCmd = skipCmd;
            this.recvOnly = recvOnly;
            this.matchLevel = matchLevel;
            this.logLevel = logLevel;
        }

        public ILogger CreateLogger(string categoryName) => new NUnitLogger(categoryName, textWriter, scope, skipCmd, recvOnly: recvOnly, matchLevel: matchLevel, logLevel: logLevel);

        public void Dispose()
        { }

        private class NUnitLogger : ILogger
        {
            private readonly string categoryName;
            private readonly TextWriter textWriter;
            private readonly string scope;
            private readonly HashSet<string> skipCmd;
            private readonly bool recvOnly;
            private readonly bool matchLevel;
            private readonly LogLevel logLevel;

            public NUnitLogger(string categoryName, TextWriter textWriter, string scope, HashSet<string> skipCmd = null, bool recvOnly = false, bool matchLevel = false, LogLevel logLevel = LogLevel.None)
            {
                this.categoryName = categoryName;
                this.textWriter = textWriter;
                this.scope = scope;
                this.skipCmd = skipCmd;
                this.recvOnly = recvOnly;
                this.matchLevel = matchLevel;
                this.logLevel = logLevel;
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
                if ((matchLevel && logLevel == this.logLevel) || !matchLevel)
                {
                    var msg = string.Format("[{0:D3}.{1}.({2})] |{3}| <{4}> {5} ^{6}^",
                        eventId.Id,
                        LogFormatter.FormatDate(DateTime.UtcNow),
                        GetLevelStr(logLevel),
                        scope,
                        categoryName,
                        exception,
                        formatter(state, exception));
                    textWriter.Write(msg);
                }
            }
        }
    }
}