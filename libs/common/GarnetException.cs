// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Garnet exception base type
    /// </summary>
    public class GarnetException : Exception
    {
        /// <summary>
        /// LogLevel for this exception
        /// </summary>
        public LogLevel LogLevel { get; } = LogLevel.Trace;
        public bool ClientResponse { get; } = true;

        /// <summary>
        /// Throw Garnet exception
        /// </summary>
        public GarnetException(LogLevel logLevel = LogLevel.Trace, bool clientResponse = true)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
        }

        /// <summary>
        /// Throw Garnet exception with message
        /// </summary>
        /// <param name="message"></param>
        public GarnetException(string message, LogLevel logLevel = LogLevel.Trace, bool clientResponse = true) : base(message)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
        }

        /// <summary>
        /// Throw Garnet exception with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public GarnetException(string message, Exception innerException, LogLevel logLevel = LogLevel.Trace, bool clientResponse = true) : base(message, innerException)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
        }

        /// <summary>
        /// Throw helper that throws a GarnetException.
        /// </summary>
        /// <param name="message">Exception message.</param>
        [DoesNotReturn]
        public static void Throw(string message, LogLevel logLevel = LogLevel.Trace) =>
            throw new GarnetException(message, logLevel);
    }
}