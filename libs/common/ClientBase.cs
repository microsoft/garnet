// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using Garnet.networking;

namespace Garnet.common
{
    /// <summary>
    /// Abstract base class for client session provider.
    /// </summary>
    public abstract unsafe class ClientBase : IDisposable
    {
        /// <summary>
        /// The host endpoint
        /// </summary>
        protected readonly EndPoint endpoint;

        /// <summary>
        /// connection status
        /// </summary>
        protected bool connected;

        /// <summary>
        /// Buffersize
        /// </summary>
        protected int Buffersize;

        /// <summary>
        /// outstanding requests
        /// </summary>
        protected volatile int numPendingRequests;

        /// <summary>
        /// networkSender
        /// </summary>
        protected INetworkSender networkSender;

        /// <summary>
        /// Create client
        /// </summary>
        /// <param name="endpoint">The host endpoint to connect to</param>   
        /// <param name="bufferSize">The buffer size</param>        
        public ClientBase(EndPoint endpoint, int bufferSize)
        {
            this.endpoint = endpoint;
            this.Buffersize = bufferSize;
            this.connected = false;
            this.numPendingRequests = 0;
        }

        /// <summary>
        /// Get head of internal buffer
        /// </summary>
        public byte* GetInternalBufferHead() => networkSender.GetResponseObjectHead();

        /// <summary>
        /// Get tail of internal buffer
        /// </summary>
        public byte* GetInternalBufferTail() => networkSender.GetResponseObjectTail();

        /// <summary>
        /// Connect
        /// </summary>
        public abstract void Connect();

        /// <summary>
        /// Send specified number of bytes from buffer array.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="len"></param>
        /// <param name="numTokens"></param>
        public abstract void Send(byte[] buf, int len, int numTokens);

        /// <summary>
        /// Send specified number of bytes from buffer pointer.
        /// </summary>        
        /// <param name="len"></param>
        /// <param name="numTokens"></param>
        public abstract void Send(int len, int numTokens = 1);

        /// <summary>
        /// Spin-wait for all responses to come back. 
        /// Return true if pending requests have been completed or false if the timeout specified has been reached.
        /// </summary>
        public virtual bool CompletePendingRequests(int timeout = -1, CancellationToken token = default)
        {
            var deadline = timeout == -1 ? DateTime.MaxValue.Ticks : DateTime.Now.AddMilliseconds(timeout).Ticks;
            while (numPendingRequests > 0 && DateTime.Now.Ticks < deadline)
            {
                if (token.IsCancellationRequested) return false;
                Thread.Yield();
            }

            //TODO: Re-enable to catch token counting errors.
            //Debug.Assert(numPendingRequests == 0, $"numPendingRequests cannot be nonzero, numPendingRequests = {numPendingRequests} | " +
            //    $"timeout = {timeout}, deadline: {deadline} > now: {DateTime.Now.Ticks}");
            return numPendingRequests == 0;
        }

        /// <summary>
        /// Authenticate
        /// </summary>
        /// <param name="auth">Auth string</param>
        public abstract void Authenticate(string auth);

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose() { }
    }
}