// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// TCP network sender
    /// </summary>
    public class ClientTcpNetworkSender : GarnetTcpNetworkSender
    {
        readonly SimpleObjectPool<SocketAsyncEventArgs> reusableSaea;
        readonly Action<object> callback;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="callback"></param>
        /// <param name="networkBufferSettings"></param>
        /// <param name="networkPool"></param>
        /// <param name="networkSendThrottleMax"></param>
        public ClientTcpNetworkSender(Socket socket, Action<object> callback, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, int networkSendThrottleMax)
            : base(socket, networkBufferSettings, networkPool, networkSendThrottleMax)
        {
            this.callback = callback;
            this.reusableSaea = new SimpleObjectPool<SocketAsyncEventArgs>(() =>
            {
                var s = new SocketAsyncEventArgs();
                s.Completed += SeaaBuffer_Completed;
                return s;
            });
        }

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt > ThrottleMax)
                throttle.Wait();

            // The slot is released on this path too, so a sender queued behind this one is not stranded
            if (!IsOpen)
            {
                CompleteBlockingSend();
                throw new Exception("The network sender is closed");
            }

            var s = reusableSaea.Checkout();
            s.SetBuffer(buffer, offset, count);
            s.UserToken = context;

            bool pending;
            try
            {
                pending = socket.SendAsync(s);
            }
            catch
            {
                reusableSaea.Return(s);
                CompleteBlockingSend();
                // Rethrow exception as session is not usable
                throw;
            }

            // The socket finished the send synchronously, so there is no completion callback to wait for
            if (!pending)
                SeaaBuffer_Completed(null, s);
        }

        /// <inheritdoc />
        public override void SendCallback(object context)
        {
            callback(context);
        }

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
        {
            base.DisposeNetworkSender(waitForSendCompletion);

            // Unlike the server's sender, this one is disposed from application threads, and its owner expects
            // every send callback to have run before disposal returns
            while (!IsClosed || Volatile.Read(ref throttleCount) > 0)
                Thread.Yield();

            reusableSaea.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                callback(e.UserToken);
            }
            finally
            {
                reusableSaea.Return(e);
                CompleteBlockingSend();
            }
        }
    }
}