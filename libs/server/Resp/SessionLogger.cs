// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    class SessionLogger : ILogger
    {
        private readonly ILogger _logger;
        private readonly string _sessionIdPrefix;

        public SessionLogger(ILogger logger, string sessionId)
        {
            Debug.Assert(logger != null);
            _logger = logger;
            _sessionIdPrefix = sessionId;
        }

        public IDisposable BeginScope<TState>(TState state)
            => _logger.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel)
            => _logger.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            _logger.Log(logLevel, eventId, state, exception, (s, ex) => _sessionIdPrefix + formatter(state, exception));
        }
    }
}