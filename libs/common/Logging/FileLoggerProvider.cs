// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Extension methods to configure ILoggingBuilder with FileLoggerProvider
    /// </summary>
    public static class FileLoggerProviderExtensions
    {
        /// <summary>
        /// Add FileLoggerProvider for filename
        /// </summary>
        /// <param name="builder">The logging builder.</param>
        /// <param name="filename">The log file path</param>
        /// <param name="flushInterval">The flush interval in milliseconds</param>
        /// <returns>The logging builder.</returns>
        public static ILoggingBuilder AddFile(
            this ILoggingBuilder builder,
            string filename,
            int flushInterval = default)
        {
            if (builder == null)
                throw new ArgumentNullException(nameof(builder));
            builder.AddProvider(new FileLoggerProvider(new FileLoggerOutput(filename, flushInterval)));
            return builder;
        }
    }

    /// <summary>
    /// Output to file for logging
    /// </summary>
    public class FileLoggerOutput : IDisposable
    {
        private readonly StreamWriter streamWriter;
        private readonly object lockObj;

        /// <summary>
        /// Create a file logger output
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="flushInterval"></param>
        public FileLoggerOutput(string filename, int flushInterval = default)
        {
            streamWriter = new StreamWriter(File.Open(filename, FileMode.Append, FileAccess.Write, FileShare.ReadWrite), Encoding.UTF8);
            lockObj = new object();
        }

        /// <summary>
        /// Dispose FileLoggerOutput
        /// </summary>
        public void Dispose()
        {
            streamWriter.Dispose();
        }

        /// <summary>
        /// Logs a message.
        /// </summary>
        /// <typeparam name="TState">The type of <paramref name="state"/>.</typeparam>
        /// <param name="logLevel">The log level.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="state">The state.</param>
        /// <param name="exception">The exception.</param>
        /// <param name="formatter">The formatter.</param>
        /// <param name="categoryName">The category.</param>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter, string categoryName)
        {
            var msg = string.Format("[{0:D3}.{1}] ({2}) <{3}> {4}",
                eventId.Id,
                LogFormatter.FormatDate(DateTime.UtcNow),
                logLevel,
                categoryName,
                formatter(state, exception));

            lock (lockObj)
            {
                streamWriter.WriteLine(msg);
                streamWriter.Flush();
            }
        }
    }

    /// <summary>
    /// FileLoggerProvider
    /// </summary>
    public class FileLoggerProvider : ILoggerProvider
    {
        private readonly FileLoggerOutput loggerOutput;

        /// <summary>
        /// FileLoggerProvider constructor
        /// </summary>
        public FileLoggerProvider(FileLoggerOutput loggerOutput)
        {
            this.loggerOutput = loggerOutput;
        }

        /// <summary>
        /// Create FileLogger instance
        /// </summary>
        /// <param name="categoryName"></param>
        /// <returns></returns>
        public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, loggerOutput);

        /// <summary>
        /// Dispose FileLoggerProvider
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Dispose() { }

        private class FileLogger : ILogger
        {
            private readonly FileLoggerOutput loggerOutput;
            private readonly string categoryName;

            public FileLogger(string categoryName, FileLoggerOutput loggerOutput)
            {
                this.loggerOutput = loggerOutput;
                this.categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state) => default!;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter) => loggerOutput.Log(logLevel, eventId, state, exception, formatter, categoryName);
        }
    }
}