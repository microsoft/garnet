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
        /// <param name="networkPool"></param>
        /// <param name="networkSendThrottleMax"></param>
        public ClientTcpNetworkSender(Socket socket, Action<object> callback, LimitedFixedBufferPool networkPool, int networkSendThrottleMax)
            : base(socket, networkPool, networkSendThrottleMax)
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
            if (cnt < 0)
            {
                Interlocked.Decrement(ref throttleCount);
                throw new Exception("Throttle count is negative");
            }
            if (cnt > ThrottleMax)
                throttle.Wait();

            var s = reusableSaea.Checkout();
            s.SetBuffer(buffer, offset, count);
            s.UserToken = context;
            try
            {
                if (!socket.SendAsync(s))
                    SeaaBuffer_Completed(null, s);
            }
            catch
            {
                reusableSaea.Return(s);
                if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                    throttle.Release();
                // Rethrow exception as session is not usable
                throw;
            }
        }

        /// <inheritdoc />
        public override void SendCallback(object context)
        {
            callback(context);
        }

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
        {
            if (!waitForSendCompletion)
                socket.Dispose();

            base.DisposeNetworkSender(waitForSendCompletion);
            reusableSaea.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            callback(e.UserToken);
            reusableSaea.Return(e);
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }
    }
}