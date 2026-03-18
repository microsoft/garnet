// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
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
            ObjectDisposedException.ThrowIf(!sendMonitor.TryEnter(out var cnt), nameof(ClientTcpNetworkSender));

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
                if (sendMonitor.Exit() >= ThrottleMax)
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
            if (sendMonitor.Exit() >= ThrottleMax)
                throttle.Release();
        }
    }
}