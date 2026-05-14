// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        /// <summary>
        /// Array of enabled test logging flag types
        /// </summary>
        public bool[] GarnetTestLoggingEvents = [.. Enum.GetValues<GarnetTestLoggingEventType>().Select(_ => false)];

        static readonly string[] lvl =
        [
            "trce",
            "dbug",
            "info",
            "warn",
            "errr",
            "crit",
        ];

        public NUnitLoggerProvider(TextWriter textWriter, string scope = "", HashSet<string> skipCmd = null, bool recvOnly = false, bool matchLevel = false, LogLevel logLevel = LogLevel.None)
        {
            this.textWriter = textWriter;
            this.scope = scope;
            this.skipCmd = skipCmd;
            this.recvOnly = recvOnly;
            this.matchLevel = matchLevel;
            this.logLevel = logLevel;
        }

        public ILogger CreateLogger(string categoryName) => new NUnitLogger(this, categoryName, textWriter, scope, skipCmd, recvOnly: recvOnly, matchLevel: matchLevel, logLevel: logLevel);

        public void Dispose()
        { }

        private class NUnitLogger : ILogger
        {
            private readonly NUnitLoggerProvider provider;
            private readonly string categoryName;
            private readonly TextWriter textWriter;
            private readonly string scope;
            private readonly HashSet<string> skipCmd;
            private readonly bool recvOnly;
            private readonly bool matchLevel;
            private readonly LogLevel logLevel;

            public NUnitLogger(NUnitLoggerProvider provider, string categoryName, TextWriter textWriter, string scope, HashSet<string> skipCmd = null, bool recvOnly = false, bool matchLevel = false, LogLevel logLevel = LogLevel.None)
            {
                this.provider = provider;
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

            private static string GetLevelStr(LogLevel ll) => lvl[(int)ll];

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                if (state is GarnetTestLoggingEvent _state)
                {
                    if (provider.GarnetTestLoggingEvents[(int)_state.Type])
                    {
                        var msg = string.Format("[{0:d1}.{1}.({2})] |{3}| <{4}> {5} ^{6}^",
                            eventId.Id,
                            LogFormatter.FormatTime(DateTime.UtcNow),
                            GetLevelStr(logLevel),
                            scope,
                            categoryName,
                            exception,
                            formatter(state, exception));
                        textWriter.Write(msg);
                    }
                }
                else if ((matchLevel && logLevel == this.logLevel) || !matchLevel)
                {
                    var msg = string.Format("[{0:d1}.{1}.({2})] |{3}| <{4}> {5} ^{6}^",
                        eventId.Id,
                        LogFormatter.FormatTime(DateTime.UtcNow),
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