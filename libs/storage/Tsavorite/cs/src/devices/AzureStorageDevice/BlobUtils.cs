// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Threading.Tasks;

    static class BlobUtils
    {
        /// <summary>
        /// Checks whether the given storage exception is transient, and 
        /// therefore meaningful to retry.
        /// </summary>
        /// <param name="exception">The storage exception.</param>
        /// <returns>Whether this is a transient storage exception.</returns>
        public static bool IsTransientStorageError(Exception exception)
        {
            // handle Azure V12 SDK exceptions
            if (exception is Azure.RequestFailedException e1 && httpStatusIndicatesTransientError(e1.Status))
            {
                return true;
            }

            // Empirically observed: timeouts on synchronous calls
            if (exception.InnerException is TimeoutException)
            {
                return true;
            }

            // Empirically observed: transient cancellation exceptions that are not application initiated
            if (exception is OperationCanceledException || exception.InnerException is OperationCanceledException)
            {
                return true;
            }

            // Empirically observed: transient exception ('An existing connection was forcibly closed by the remote host')
            if (exception.InnerException is System.Net.Http.HttpRequestException && exception.InnerException?.InnerException is System.IO.IOException)
            {
                return true;
            }

            // Empirically observed: transient socket exceptions
            if (exception is System.IO.IOException && exception.InnerException is System.Net.Sockets.SocketException)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Checks whether the given exception is a timeout exception.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <returns>Whether this is a timeout storage exception.</returns>
        public static bool IsTimeout(Exception exception)
        {
            return exception is TimeoutException
                || (exception is Azure.RequestFailedException e1 && (e1.Status == 408 || e1.ErrorCode == "OperationTimedOut"))
                || (exception is TaskCanceledException & exception.Message.StartsWith("The operation was cancelled because it exceeded the configured timeout"));
        }

        // Transient http status codes as documented at https://docs.microsoft.com/en-us/azure/architecture/best-practices/retry-service-specific#azure-storage
        static bool httpStatusIndicatesTransientError(int? statusCode) =>
            (statusCode == 408    //408 Request Timeout
            || statusCode == 429  //429 Too Many Requests
            || statusCode == 500  //500 Internal Server Error
            || statusCode == 502  //502 Bad Gateway
            || statusCode == 503  //503 Service Unavailable
            || statusCode == 504); //504 Gateway Timeout
    }
}