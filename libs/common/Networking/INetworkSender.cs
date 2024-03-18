// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.networking
{
    /// <summary>
    /// Interface for Network Sender
    /// </summary>
    public interface INetworkSender : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        MaxSizeSettings GetMaxSizeSettings { get; }

        /// <summary>
        /// Remote endpoint name
        /// </summary>
        string RemoteEndpointName { get; }

        /// <summary>
        /// Allocate a new response object
        /// </summary>
        void GetResponseObject();

        /// <summary>
        /// Free response object
        /// </summary>
        void ReturnResponseObject();

        /// <summary>
        /// Get current response object head ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectHead();

        /// <summary>
        /// Get current response object tail ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectTail();

        /// <summary>
        /// Send payload stored at response object, from offset to offset + size
        /// </summary>
        /// <param name="offset">Offset of response from which to start sending</param>
        /// <param name="size">Number of bytes to send, starting from offset</param>
        /// <returns>Whether the send succeeded</returns>
        bool SendResponse(int offset, int size);

        /// <summary>
        /// Send response (caller owns buffer space)
        /// </summary>
        void SendResponse(byte[] buffer, int offset, int count, object context);

        /// <summary>
        /// Send response (caller owns buffer space)
        /// </summary>
        void SendCallback(object context);

        /// <summary>
        /// Dispose, optionally waiting for ongoing outgoing calls to complete
        /// </summary>
        void DisposeNetworkSender(bool waitForSendCompletion);

        /// <summary>
        /// Throttle mechanism for preventing too many sends outstanding (blocking)
        /// </summary>
        void Throttle();
    }
}