// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// The first failure observed while issuing or completing the asynchronous device requests that make up an index
    /// checkpoint or recovery. The detail and the exception that caused it are held in one immutable object so that a
    /// single <see cref="System.Threading.Interlocked.CompareExchange{T}(ref T, T, T)"/> publishes both together: the
    /// requests run concurrently, so several may fail at once and only the first to be recorded is reported.
    /// </summary>
    internal sealed class IoFailure
    {
        /// <summary>Identifies which request failed and how, for example the chunk or level index and the byte counts.</summary>
        internal readonly string Detail;

        /// <summary>
        /// The exception the device reported, or null when the failure was reported as an error code or a short
        /// transfer rather than an exception.
        /// </summary>
        internal readonly Exception Exception;

        internal IoFailure(string detail, Exception exception = null)
        {
            Detail = detail;
            Exception = exception;
        }

        /// <summary>
        /// Build the exception to fail the operation with, preserving <see cref="Exception"/> as the inner exception so
        /// the device's own error, and its stack trace, survive to the caller.
        /// </summary>
        /// <param name="summary">Describes the operation that failed, used as the prefix of the exception message.</param>
        internal TsavoriteIOException ToException(string summary) => new($"{summary}: {Detail}", Exception);
    }
}