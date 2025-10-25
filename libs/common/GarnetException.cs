// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
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
        public bool Panic { get; } = false;
        public bool DisposeSession { get; } = true;

        /// <summary>
        /// Throw Garnet exception.
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="clientResponse"></param>
        /// <param name="panic"></param>
        /// <param name="disposeSession"></param>
        public GarnetException(LogLevel logLevel = LogLevel.Trace, bool clientResponse = true, bool panic = false, bool disposeSession = true)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
            Panic = panic;
            DisposeSession = disposeSession;
        }

        /// <summary>
        /// Throw Garnet exception with message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="logLevel"></param>
        /// <param name="clientResponse"></param>
        /// <param name="panic"></param>
        /// <param name="disposeSession"></param>
        public GarnetException(string message, LogLevel logLevel = LogLevel.Trace, bool clientResponse = true, bool panic = false, bool disposeSession = true) : base(message)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
            Panic = panic;
            DisposeSession = disposeSession;
        }

        /// <summary>
        /// Throw Garnet exception with message.
        /// </summary>
        /// <param name="messageBytes"></param>
        /// <param name="logLevel"></param>
        /// <param name="clientResponse"></param>
        /// <param name="panic"></param>
        /// <param name="disposeSession"></param>
        public GarnetException(ReadOnlySpan<byte> messageBytes, LogLevel logLevel = LogLevel.Trace, bool clientResponse = true, bool panic = false, bool disposeSession = true)
            : this(Encoding.ASCII.GetString(messageBytes))
        {
        }

        /// <summary>
        /// Throw Garnet exception with message and inner exception.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        /// <param name="logLevel"></param>
        /// <param name="clientResponse"></param>
        /// <param name="panic"></param>
        /// <param name="disposeSession"></param>
        public GarnetException(string message, Exception innerException, LogLevel logLevel = LogLevel.Trace, bool clientResponse = true, bool panic = false, bool disposeSession = true) : base(message, innerException)
        {
            LogLevel = logLevel;
            ClientResponse = clientResponse;
            Panic = panic;
            DisposeSession = disposeSession;
        }

        /// <summary>
        /// Throw helper that throws a GarnetException.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="logLevel"></param>
        /// <exception cref="GarnetException"></exception>
        [DoesNotReturn]
        public static void Throw(string message, LogLevel logLevel = LogLevel.Trace) =>
            throw new GarnetException(message, logLevel);
    }
}