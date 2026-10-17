// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Faults the receive loop of a connection whose session was torn down while one of its commands was
    /// suspended. The suspended command is never resumed, so the loop has nothing left to wait for. The
    /// connection is closing anyway, which makes this an ordinary end of the loop and not a failure.
    /// </summary>
    public sealed class SessionTornDownException : ObjectDisposedException
    {
        /// <summary>
        /// What is traced where the receive loop of a session ends because the session was torn down while one
        /// of its commands was suspended.
        /// </summary>
        public const string LogMessage = "The session was torn down while a command was suspended";

        /// <summary>
        /// What is traced where a suspended command, resumed by the cancellation of what it was waiting for,
        /// unwinds because the teardown of its session did the cancelling.
        /// </summary>
        public const string ResumeLogMessage = "The session was torn down while a suspended command was resuming";

        /// <summary>
        /// Creates the exception.
        /// </summary>
        /// <param name="sessionName">Name of the session type that was torn down.</param>
        public SessionTornDownException(string sessionName)
            : base(sessionName, LogMessage)
        {
        }
    }
}