// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;

namespace Garnet.common
{
    /// <summary>
    /// Outcome of a socket send or receive, carrying either a transferred byte count or the error that
    /// ended the operation. Returned by value so a synchronously completed operation never touches a
    /// completion source.
    /// </summary>
    public readonly struct SocketOperationResult
    {
        /// <summary>
        /// Bytes transferred. Meaningful only when <see cref="HasError"/> is false.
        /// </summary>
        public readonly int BytesTransferred;

        /// <summary>
        /// Error that ended the operation, or <see cref="SocketError.Success"/>.
        /// </summary>
        public readonly SocketError SocketError;

        /// <summary>
        /// Creates a successful result.
        /// </summary>
        /// <param name="bytesTransferred">Bytes transferred by the operation.</param>
        public SocketOperationResult(int bytesTransferred)
        {
            BytesTransferred = bytesTransferred;
            SocketError = SocketError.Success;
        }

        /// <summary>
        /// Creates a failed result.
        /// </summary>
        /// <param name="socketError">Error that ended the operation.</param>
        public SocketOperationResult(SocketError socketError)
        {
            BytesTransferred = 0;
            SocketError = socketError;
        }

        /// <summary>
        /// Whether the operation ended in an error.
        /// </summary>
        public bool HasError => SocketError != SocketError.Success;
    }
}