// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Garnet.networking;
using Garnet.server;

namespace Garnet.common
{

    /// <summary>
    /// Light remote client
    /// </summary>
    public unsafe class LightClient : ClientBase, IServerHook, IMessageConsumer
    {
        /// <summary>
        /// On response delegate function.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="bytesRead"></param>
        /// <param name="opType"></param>        
        /// <returns></returns>
        public delegate (int, int) OnResponseDelegateUnsafe(byte* buf, int bytesRead, int opType);
        readonly OnResponseDelegateUnsafe onResponseDelegateUnsafe = null;

        readonly int BufferSize;
        readonly SslClientAuthenticationOptions sslOptions;
        Socket socket;
        /// <summary>
        /// Operation type
        /// </summary>
        public int opType;

        LightClientTcpNetworkHandler networkHandler;

        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Create client instance to connect to specfied destination
        /// </summary>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="opType">Op type</param>
        /// <param name="onResponseDelegateUnsafe">Callback that takes in a byte array and length, and returns the number of bytes read and the number of requests processed</param>
        /// <param name="BufferSize">Message buffer size.</param>
        /// <param name="sslOptions">SSL options</param>
        public LightClient(
            string address,
            int port,
            int opType,
            OnResponseDelegateUnsafe onResponseDelegateUnsafe = null,
            int BufferSize = 1 << 18,
            SslClientAuthenticationOptions sslOptions = null)
            : base(address, port, BufferSize)
        {
            this.networkPool = new LimitedFixedBufferPool(BufferSize);
            this.onResponseDelegateUnsafe = onResponseDelegateUnsafe ?? new OnResponseDelegateUnsafe(DefaultLightReceiveUnsafe);
            this.opType = opType;
            this.BufferSize = BufferSize;
            this.sslOptions = sslOptions;
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
            socket = GetSendSocket(address, port);
            networkHandler = new LightClientTcpNetworkHandler(this, socket, networkPool, sslOptions != null, this);
            networkHandler.StartAsync(sslOptions, $"{address}:{port}").GetAwaiter().GetResult();
            networkSender = networkHandler.GetNetworkSender();
            networkSender.GetResponseObject();
        }

        /// <summary>
        /// Send len bytes from given buffer
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="len"></param>
        /// <param name="numTokens">Number of symbols expected in the response</param>
        public override void Send(byte[] buf, int len, int numTokens = 1)
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

        private Socket GetSendSocket(string address, int port, int millisecondsTimeout = -2)
        {
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            var socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            if (millisecondsTimeout != -2)
            {
                IAsyncResult result = socket.BeginConnect(endPoint, null, null);
                result.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
                if (socket.Connected)
                    socket.EndConnect(result);
                else
                {
                    socket.Close();
                    throw new Exception("Failed to connect server.");
                }
            }
            else
            {
                socket.Connect(endPoint);
            }

            return socket;
        }

        private static (int, int) DefaultLightReceiveUnsafe(byte* buf, int bytesRead, int opType) => (bytesRead, 1);

        /// <inheritdoc />
        public override void Dispose()
        {
            networkSender.ReturnResponseObject();
            networkHandler?.Dispose();
            networkPool.Dispose();
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
        public int TryConsumeMessages(byte* reqBuffer, int bytesRead)
        {
            (int readHead, int count) = onResponseDelegateUnsafe(reqBuffer, bytesRead, opType);
            Interlocked.Add(ref numPendingRequests, -count);
            return readHead;
        }
    }
}