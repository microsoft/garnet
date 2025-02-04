// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Light remote client
    /// </summary>
    public class LightClient : ClientBase, IServerHook, IMessageConsumer
    {
        /// <summary>
        /// On response delegate function.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="bytesRead"></param>
        /// <param name="opType"></param>        
        /// <returns></returns>
        public unsafe delegate (int, int) OnResponseDelegateUnsafe(byte* buf, int bytesRead, int opType);
        readonly OnResponseDelegateUnsafe onResponseDelegateUnsafe = null;

        readonly int BufferSize;
        readonly SslClientAuthenticationOptions sslOptions;
        Socket socket;
        /// <summary>
        /// Operation type
        /// </summary>
        public int opType;

        LightClientTcpNetworkHandler networkHandler;
        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;
        readonly ILogger logger;

        /// <summary>
        /// Create client instance to connect to specfied destination
        /// </summary>
        /// <param name="endpoint">The server endpoint</param>
        /// <param name="opType">Op type</param>
        /// <param name="onResponseDelegateUnsafe">Callback that takes in a byte array and length, and returns the number of bytes read and the number of requests processed</param>
        /// <param name="BufferSize">Message buffer size.</param>
        /// <param name="sslOptions">SSL options</param>
        public unsafe LightClient(
            EndPoint endpoint,
            int opType,
            OnResponseDelegateUnsafe onResponseDelegateUnsafe = null,
            int BufferSize = 1 << 18,
            SslClientAuthenticationOptions sslOptions = null,
            ILogger logger = null)
            : base(endpoint, BufferSize)
        {
            this.networkBufferSettings = new NetworkBufferSettings(BufferSize, BufferSize);
            this.networkPool = networkBufferSettings.CreateBufferPool();
            this.onResponseDelegateUnsafe = onResponseDelegateUnsafe ?? new OnResponseDelegateUnsafe(DefaultLightReceiveUnsafe);
            this.opType = opType;
            this.BufferSize = BufferSize;
            this.sslOptions = sslOptions;
            this.logger = logger;
        }

        /// <summary>
        /// Authenticate
        /// </summary>
        /// <param name="auth">Auth string</param>
        public override void Authenticate(string auth)
        {
            if (auth != null)
            {
                int op = opType;
                opType = 9999; // auth
                var authCmd = Encoding.ASCII.GetBytes($"AUTH {auth}\r\n");
                Send(authCmd, authCmd.Length, 1);
                CompletePendingRequests();
                opType = op;
            }
        }

        /// <summary>
        /// Send readonly request to that node
        /// </summary>
        public void ReadOnly()
        {
            int op = opType;
            opType = 8888; // auth
            var readOnlyBytes = Encoding.ASCII.GetBytes($"*1\r\n$8\r\nREADONLY\r\n");
            Send(readOnlyBytes, readOnlyBytes.Length, 1);
            CompletePendingRequests();
            opType = op;
        }

        /// <summary>
        /// Response buffer used by client
        /// </summary>
        public byte[] ResponseBuffer => networkHandler.RawTransportBuffer;

        /// <inheritdoc />
        public bool Disposed => false;

        /// <summary>
        /// Connect
        /// </summary>
        public override void Connect()
        {
            socket = ConnectSendSocketAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            networkHandler = new LightClientTcpNetworkHandler(this, socket, networkBufferSettings, networkPool, sslOptions != null, this);
            networkHandler.StartAsync(sslOptions, endpoint.ToString()).ConfigureAwait(false).GetAwaiter().GetResult();
            networkSender = networkHandler.GetNetworkSender();
            networkSender.GetResponseObject();
        }

        /// <summary>
        /// Connect client send socket
        /// </summary>
        private async Task<Socket> ConnectSendSocketAsync(CancellationToken cancellationToken = default)
        {
            if (endpoint is DnsEndPoint dnsEndpoint)
            {
                var hostEntries = await Dns.GetHostEntryAsync(dnsEndpoint.Host, cancellationToken).ConfigureAwait(false);
                // Try all available DNS entries if a hostName is provided
                foreach (var addressEntry in hostEntries.AddressList)
                {
                    var endpoint = new IPEndPoint(addressEntry, dnsEndpoint.Port);
                    var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };

                    if (await TryConnectSocketAsync(socket, endpoint, cancellationToken))
                        return socket;
                }
            }
            else
            {
                var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
                if (endpoint is not UnixDomainSocketEndPoint)
                    socket.NoDelay = true;

                if (await TryConnectSocketAsync(socket, endpoint, cancellationToken))
                    return socket;
            }

            logger?.LogWarning("Failed to connect at {endpoint}", endpoint);
            throw new Exception($"Failed to connect at {endpoint}");
        }

        /// <summary>
        /// Try to establish connection for <paramref name="socket"/> using <paramref name="endpoint"/>
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="endpoint"></param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns></returns>
        private async Task<bool> TryConnectSocketAsync(Socket socket, EndPoint endpoint, CancellationToken cancellationToken = default)
        {
            try
            {
                await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
                if (socket.Connected)
                    return true;
            }
            catch
            {
                socket.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Send len bytes from given buffer
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="len"></param>
        /// <param name="numTokens">Number of symbols expected in the response</param>
        public override unsafe void Send(byte[] buf, int len, int numTokens = 1)
        {
            Interlocked.Add(ref numPendingRequests, numTokens);

            fixed (byte* ptr = buf)
            {
                Buffer.MemoryCopy(ptr, networkSender.GetResponseObjectHead(), len, len);
                networkSender.SendResponse(0, len);
                networkSender.GetResponseObject();
            }
        }

        /// <summary>
        /// Send len bytes from given buffer
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="len"></param>
        /// <param name="numTokens">Number of symbols expected in the response</param>
        public unsafe void Send(byte* ptr, int len, int numTokens = 1)
        {
            Interlocked.Add(ref numPendingRequests, numTokens);

            Buffer.MemoryCopy(ptr, networkSender.GetResponseObjectHead(), len, len);
            networkSender.SendResponse(0, len);
            networkSender.GetResponseObject();
        }

        /// <summary>
        /// Send len bytes from networkSender buffer.
        /// </summary>
        /// <param name="len"></param>
        /// <param name="numTokens"></param>
        public override void Send(int len, int numTokens = 1)
        {
            Interlocked.Add(ref numPendingRequests, numTokens);
            networkSender.SendResponse(0, len);
            networkSender.GetResponseObject();
        }

        public override bool CompletePendingRequests(int timeout = -1, CancellationToken token = default)
        {
            var deadline = timeout == -1 ? DateTime.MaxValue.Ticks : DateTime.Now.AddMilliseconds(timeout).Ticks;
            while (numPendingRequests > 0 && DateTime.Now.Ticks < deadline)
            {
                if (token.IsCancellationRequested) return false;
                if (!socket.Connected) throw new GarnetException("Disconnected");
                Thread.Yield();
            }

            // TODO: Re-enable to catch token counting errors.
            // Debug.Assert(numPendingRequests == 0, $"numPendingRequests cannot be nonzero, numPendingRequests = {numPendingRequests} | " +
            //    $"timeout = {timeout}, deadline: {deadline} > now: {DateTime.Now.Ticks}");
            return numPendingRequests == 0;
        }

        private static unsafe (int, int) DefaultLightReceiveUnsafe(byte* buf, int bytesRead, int opType) => (bytesRead, 1);

        /// <inheritdoc />
        public override void Dispose()
        {
            networkSender.ReturnResponseObject();
            networkHandler?.Dispose();
            networkPool?.Dispose();
        }

        /// <inheritdoc />
        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session)
            => throw new NotSupportedException();

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            // NO-OP as we are the message consumer
        }

        /// <inheritdoc />
        public unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead)
        {
            (int readHead, int count) = onResponseDelegateUnsafe(reqBuffer, bytesRead, opType);
            Interlocked.Add(ref numPendingRequests, -count);
            return readHead;
        }
    }
}