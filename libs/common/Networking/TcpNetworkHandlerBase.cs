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
        public TcpNetworkHandlerBase(TServerHook serverHook, TNetworkSender networkSender, Socket socket, NetworkBuffers networkBuffers, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null)
            : base(serverHook, networkSender, networkBuffers, useTLS, messageConsumer: messageConsumer, logger: logger)
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
            Start();
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start();
            await base.StartAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start();
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            Start();
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

        void Start()
        {
            var receiveEventArgs = new SocketAsyncEventArgs { AcceptSocket = socket };
            receiveEventArgs.SetBuffer(networkReceiveBuffer, 0, networkReceiveBuffer.Length);
            receiveEventArgs.Completed += RecvEventArg_Completed;

            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            try
            {
                if (!socket.ReceiveAsync(receiveEventArgs))
                    Task.Run(() => RecvEventArg_Completed(null, receiveEventArgs));
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

        void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
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
                    OnNetworkReceive(e.BytesTransferred);
                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                } while (!e.AcceptSocket.ReceiveAsync(e));
            }
            catch (Exception ex)
            {
                if (ex is ObjectDisposedException ex2 && ex2.ObjectName == "System.Net.Sockets.Socket")
                    logger?.LogTrace("Accept socket was disposed at RecvEventArg_Completed");
                else
                    logger?.LogError(ex, "An error occurred at RecvEventArg_Completed");
                Dispose(e);
            }
        }

        unsafe void AllocateNetworkReceiveBuffer()
        {
            networkReceiveBufferEntry = networkBuffers.recvBufferPool.Get(networkBuffers.recvBufferPool.MinAllocationSize);
            networkReceiveBuffer = networkReceiveBufferEntry.entry;
            networkReceiveBufferPtr = networkReceiveBufferEntry.entryPtr;
        }
    }
}