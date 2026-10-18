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
        readonly string remoteEndpointName;
        readonly string localEndpointName;
        readonly bool isLocalConnection;

        /// <summary>
        /// Box home for <see cref="ResumeReceiveWithoutTLSAsync"/>. One per connection, because a connection
        /// suspends its receive loop at most once at a time: the suspension is what stops it receiving.
        /// </summary>
        readonly FireAndForgetBoxHolder resumeBox = new();

        int closeRequested;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tlsHandshakeTimeout">Longest a server-side TLS handshake may take before the connection is closed. Zero or negative waits indefinitely.</param>
        public TcpNetworkHandlerBase(TServerHook serverHook, TNetworkSender networkSender, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null, TimeSpan tlsHandshakeTimeout = default)
            : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger, tlsHandshakeTimeout: tlsHandshakeTimeout)
        {
            this.logger = logger;
            this.socket = socket;
            var remoteEndpoint = socket.RemoteEndPoint;
            this.remoteEndpointName = remoteEndpoint?.ToString() ?? string.Empty;
            this.localEndpointName = socket.LocalEndPoint?.ToString() ?? string.Empty;
            this.isLocalConnection = remoteEndpoint is IPEndPoint ip
                ? IPAddress.IsLoopback(ip.Address)
                : remoteEndpoint is UnixDomainSocketEndPoint;
            this.closeRequested = 0;

            AllocateNetworkReceiveBuffer();
        }

        /// <inheritdoc />
        public override string RemoteEndpointName => remoteEndpointName;

        /// <inheritdoc />
        public override string LocalEndpointName => localEndpointName;

        /// <inheritdoc />
        public override bool IsLocalConnection() => isLocalConnection;

        /// <inheritdoc />
        public override void Start(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_TcpNetworkHandlerBase_Start_Server);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_TcpNetworkHandlerBase_Start_Server);
            return base.StartAsync(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            await base.StartAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override bool TryClose()
        {
            // Only one caller gets to invoke Close, as we'd expect subsequent ones to fail and throw
            if (Interlocked.CompareExchange(ref closeRequested, 1, 0) != 0)
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

        /// <summary>
        /// Start the socket receive - after this call, NetworkHandler may get disposed at any time
        /// such as when the socket is disconnected.
        /// </summary>
        /// <param name="useTLS"></param>
        void Start(bool useTLS)
        {
            // The receive loop holds the buffers until it ends in Dispose(SocketAsyncEventArgs). A handler disposed
            // before it starts has already returned them, so it does not receive at all.
            if (!TryTakeBufferHold())
                return;

            var receiveEventArgs = new SocketAsyncEventArgs { AcceptSocket = socket };
            receiveEventArgs.SetBuffer(networkReceiveBuffer, 0, networkReceiveBuffer.Length);
            receiveEventArgs.Completed += useTLS ? RecvEventArgCompletedWithTLS : RecvEventArgCompletedWithoutTLS;

            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            try
            {
                if (!socket.ReceiveAsync(receiveEventArgs))
                {
                    if (useTLS)
                        _ = Task.Run(() => RecvEventArgCompletedWithTLS(null, receiveEventArgs));
                    else
                        _ = Task.Run(() => RecvEventArgCompletedWithoutTLS(null, receiveEventArgs));
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
            try
            {
                if (socket.Connected)
                {
                    // Gracefully shutdown the socket connection
                    socket.Shutdown(SocketShutdown.Both);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error shutting down socket");
            }
            finally
            {
                // Always close the socket to release the resources
                socket.Close();
                // Dispose of the socket to free up unmanaged resources
                socket.Dispose();
            }
            DisposeImpl();
        }

        /// <summary>
        /// Dispose resources - call ONLY if Start was not called on network handler
        /// </summary>
        public void DisposeResources()
        {
            DisposeImpl();
        }

        /// <summary>
        /// Ends the receive loop. Every way out of the loop comes here, with no receive outstanding.
        /// </summary>
        void Dispose(SocketAsyncEventArgs e)
        {
            try
            {
                if (e.AcceptSocket.Connected)
                {
                    e.AcceptSocket.Shutdown(SocketShutdown.Both);
                }
            }
            catch (Exception ex)
            {
                logger?.LogTrace(ex, "Error shutting down accept socket during SAEA dispose");
            }
            finally
            {
                e.AcceptSocket.Close();
                e.AcceptSocket.Dispose();
            }
            DisposeImpl();
            e.Dispose();
            ReleaseBufferHold();
        }

        void RecvEventArgCompletedWithTLS(object sender, SocketAsyncEventArgs e) =>
            _ = HandleReceiveWithTLSAsync(sender, e);

        void RecvEventArgCompletedWithoutTLS(object sender, SocketAsyncEventArgs e) =>
            HandleReceiveWithoutTLS(sender, e);

        private void HandleReceiveWithoutTLS(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                if (TryReceiveLoopWithoutTLS(e, out var demand, out var pending))
                {
                    // A command in this batch suspended. Hand the rest of the loop to a continuation and
                    // release this IO thread. No receive is outstanding, so the connection stays idle until
                    // the batch finishes, which is what parks the session.
                    FireAndForgetMethodBuilder.UseBoxOf(resumeBox);
                    _ = ResumeReceiveWithoutTLSAsync(pending, demand, e);
                }
            }
            catch (Exception ex)
            {
                HandleReceiveFailure(ex, e);
            }
        }

        /// <summary>
        /// Drains whatever the socket delivers synchronously.
        /// </summary>
        /// <param name="e">Receive event args for this connection.</param>
        /// <param name="demand">
        /// Buffer occupancy of the unfinished pass, meaningful only when this returns true.
        /// </param>
        /// <param name="pending">
        /// The consume that must finish before the loop continues, meaningful only when this returns true.
        /// </param>
        /// <returns>
        /// False once the loop is finished -- the connection was torn down, or a receive is outstanding and
        /// its completion will re-enter here. True when a batch suspended and <paramref name="pending"/> must
        /// be awaited and completed first.
        /// </returns>
        private bool TryReceiveLoopWithoutTLS(SocketAsyncEventArgs e, out int demand, out ValueTask<int> pending)
        {
            demand = 0;
            pending = default;
            do
            {
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || serverHook.Disposed)
                {
                    // No more things to receive
                    Dispose(e);
                    return false;
                }

                var consume = BeginNetworkReceiveWithoutTLS(e.BytesTransferred, out demand);

                // Observe the consume exactly once, into a local, and let everything below follow from that
                // one observation. A resume on another thread can complete it the instant it is published, so
                // a second observation need not agree with this one, and a pass believed finished on the
                // strength of it would skip the completion half and leave this batch's bytes unconsumed for
                // the next pass to parse again.
                if (!consume.IsCompletedSuccessfully)
                {
                    pending = consume;
                    return true;
                }

                CompleteNetworkReceiveWithoutTLS(consume.GetAwaiter().GetResultGuarded(), demand);

                e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
            } while (!e.AcceptSocket.ReceiveAsync(e));

            return false;
        }

        /// <summary>
        /// Finishes a suspended batch and then resumes the synchronous receive loop, staying asynchronous only
        /// for as long as batches keep suspending.
        /// </summary>
        /// <remarks>
        /// One asynchronous frame covers the entire suspension path, so a park boxes a single state machine
        /// rather than one per network layer.
        /// </remarks>
        private async FireAndForget ResumeReceiveWithoutTLSAsync(ValueTask<int> pending, int demand, SocketAsyncEventArgs e)
        {
            try
            {
                while (true)
                {
                    var consumed = await pending.ConfigureAwait(false);
#if DEBUG
                    if (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Receive_Hold_Resumed_Pass))
                        await ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Receive_Hold_Resumed_Pass).ConfigureAwait(false);
#endif
                    CompleteNetworkReceiveWithoutTLS(consumed, demand);

                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                    if (e.AcceptSocket.ReceiveAsync(e))
                        return;

                    if (!TryReceiveLoopWithoutTLS(e, out demand, out pending))
                        return;
                }
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
                        await receiveTask.ConfigureAwait(false);
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
            else if (ex is OperationCanceledException && cancellationTokenSource is { IsCancellationRequested: true })
                logger?.LogTrace("The receive loop was woken by the connection closing");
            else if (!TryLogSessionTornDown(ex))
                logger?.LogError(ex, "An error occurred at RecvEventArg_Completed");
            Dispose(e);
        }

        unsafe void AllocateNetworkReceiveBuffer()
        {
            networkReceiveBufferEntry = networkPool.Get(BaseReceiveBufferSize, PoolEntryBufferType.NetworkReceiveBuffer);
            networkReceiveBuffer = networkReceiveBufferEntry.entry;
            networkReceiveBufferPtr = networkReceiveBufferEntry.entryPtr;
        }
    }
}