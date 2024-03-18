// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Embedded.perftest
{

    /// <summary>
    /// Logger to output stress-test status
    /// </summary>
    public class PerformanceTestLoggerProvider : ILoggerProvider
    {
        private readonly TextWriter textWriter;

        static readonly string[] lvl = new string[]
        {
            "trce",
            "dbug",
            "info",
            "warn",
            "errr",
            "crit",
        };

        public PerformanceTestLoggerProvider(TextWriter textWriter)
        {
            this.textWriter = textWriter;
        }

        public ILogger CreateLogger(string categoryName) => new PerformanceTestLogger(categoryName, textWriter);

        public void Dispose()
        {
            textWriter.Dispose();
            GC.SuppressFinalize(this);
        }

        private class PerformanceTestLogger : ILogger
        {
            private readonly string categoryName;
            private readonly TextWriter textWriter;

            public PerformanceTestLogger(string categoryName, TextWriter textWriter)
            {
                this.categoryName = categoryName;
                this.textWriter = textWriter;
            }

            public IDisposable BeginScope<TState>(TState state) => default!;

            public bool IsEnabled(LogLevel logLevel) => true;

            private static string GetLevelStr(LogLevel ll) => lvl[(int)ll];


            /// <summary>
            /// Specify the log message given the information
            /// </summary>
            /// <typeparam name="TState"></typeparam>
            /// <param name="logLevel"></param>
            /// <param name="eventId"></param>
            /// <param name="state"></param>
            /// <param name="exception"></param>
            /// <param name="formatter"></param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                string msg = logLevel switch
                {
                    LogLevel.Information => string.Format("[{0:D3}.{1}.({2})] |{3}| {4}\n",
                                                                eventId.Id,
                                                                DateTime.Now.ToString("hh:mm:ss"),
                                                                GetLevelStr(logLevel),
                                                                categoryName,
                                                                state),
                    _ => string.Format("[{0:D3}.{1}.({2})] |{3}| <{4}> {5}\n",
                                                eventId.Id,
                                                DateTime.Now.ToString("hh:mm:ss"),
                                                GetLevelStr(logLevel),
                                                categoryName,
                                                exception,
                                                state),
                };
                textWriter.Write(msg);
            }
        }
    }
}