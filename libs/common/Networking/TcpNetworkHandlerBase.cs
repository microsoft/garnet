// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// TCP network handler
    /// </summary>
    /// <typeparam name="TServerHook"></typeparam>
    /// <typeparam name="TNetworkSender"></typeparam>
    public abstract class TcpNetworkHandlerBase<TServerHook, TNetworkSender> : NetworkHandler<TServerHook, TNetworkSender>
        where TServerHook : IServerHook
        where TNetworkSender : INetworkSender
    {
        readonly ILogger logger;
        readonly Socket socket;
        readonly string remoteEndpoint;
        readonly string localEndpoint;
        int closeRequested;

        /// <summary>
        /// Constructor
        /// </summary>
        public TcpNetworkHandlerBase(TServerHook serverHook, TNetworkSender networkSender, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null)
            : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
            this.logger = logger;
            this.socket = socket;
            this.closeRequested = 0;

            remoteEndpoint = socket.RemoteEndPoint is IPEndPoint remote ? $"{remote.Address}:{remote.Port}" : "";
            localEndpoint = socket.LocalEndPoint is IPEndPoint local ? $"{local.Address}:{local.Port}" : "";
            AllocateNetworkReceiveBuffer();
        }

        /// <inheritdoc />
        public override string RemoteEndpointName => remoteEndpoint;

        /// <inheritdoc />
        public override string LocalEndpointName => localEndpoint;

        /// <inheritdoc />
        public override void Start(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start(tlsOptions != null);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start(tlsOptions != null);
            await base.StartAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start(tlsOptions != null);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start(tlsOptions != null);
            await base.StartAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override bool TryClose()
        {
            // Only one caller gets to invoke Close, as we'd expect subsequent ones to fail and throw
            if (Interlocked.CompareExchange(ref closeRequested, 0, 1) != 0)
            {
                return false;
            }

            try
            {
                // This close should cause all outstanding requests to fail.
                // 
                // We don't distinguish between clients closing their end of the Socket
                // and us forcing it closed on request.
                socket.Close();
            }
            catch
            {
                // Best effort, just swallow any exceptions
            }

            return true;
        }

        void Start(bool useTLS)
        {
            var receiveEventArgs = new SocketAsyncEventArgs { AcceptSocket = socket };
            receiveEventArgs.SetBuffer(networkReceiveBuffer, 0, networkReceiveBuffer.Length);
            receiveEventArgs.Completed += useTLS ? RecvEventArgCompletedWithTLS : RecvEventArgCompletedWithoutTLS;

            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            try
            {
                if (!socket.ReceiveAsync(receiveEventArgs))
                {
                    if (useTLS)
                        Task.Run(() => RecvEventArgCompletedWithTLS(null, receiveEventArgs));
                    else
                        Task.Run(() => RecvEventArgCompletedWithoutTLS(null, receiveEventArgs));
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred at Start.ReceiveAsync");
                Dispose(receiveEventArgs);
            }
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            socket.Dispose();
        }

        void Dispose(SocketAsyncEventArgs e)
        {
            e.AcceptSocket.Dispose();
            DisposeImpl();
            e.Dispose();
        }

        void RecvEventArgCompletedWithTLS(object sender, SocketAsyncEventArgs e) =>
            _ = HandleReceiveWithTLSAsync(sender, e);

        void RecvEventArgCompletedWithoutTLS(object sender, SocketAsyncEventArgs e) =>
            HandleReceiveWithoutTLS(sender, e);

        private void HandleReceiveWithoutTLS(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || serverHook.Disposed)
                    {
                        // No more things to receive
                        Dispose(e);
                        break;
                    }
                    OnNetworkReceiveWithoutTLS(e.BytesTransferred);
                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                } while (!e.AcceptSocket.ReceiveAsync(e));
            }
            catch (Exception ex)
            {
                HandleReceiveFailure(ex, e);
            }
        }

        private async ValueTask HandleReceiveWithTLSAsync(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || serverHook.Disposed)
                    {
                        // No more things to receive
                        Dispose(e);
                        break;
                    }
                    var receiveTask = OnNetworkReceiveWithTLSAsync(e.BytesTransferred);
                    if (!receiveTask.IsCompletedSuccessfully)
                    {
                        await receiveTask;
                    }
                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                } while (!e.AcceptSocket.ReceiveAsync(e));
            }
            catch (Exception ex)
            {
                HandleReceiveFailure(ex, e);
            }
        }

        void HandleReceiveFailure(Exception ex, SocketAsyncEventArgs e)
        {
            if (ex is ObjectDisposedException ex2 && ex2.ObjectName == "System.Net.Sockets.Socket")
                logger?.LogTrace("Accept socket was disposed at RecvEventArg_Completed");
            else
                logger?.LogError(ex, "An error occurred at RecvEventArg_Completed");
            Dispose(e);
        }

        unsafe void AllocateNetworkReceiveBuffer()
        {
            networkReceiveBufferEntry = networkPool.Get(networkBufferSettings.initialReceiveBufferSize);
            networkReceiveBuffer = networkReceiveBufferEntry.entry;
            networkReceiveBufferPtr = networkReceiveBufferEntry.entryPtr;
        }
    }
}