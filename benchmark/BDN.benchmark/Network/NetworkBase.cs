// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using BenchmarkDotNet.Attributes;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.TLS;

namespace BDN.benchmark.Network
{
    public abstract class NetworkBase
    {
        private Socket _serverSocket;
        /// <summary>
        /// Base class for operations benchmarks
        /// </summary>

        protected byte[] _networkEchoCommandBuffer;

        private ConcurrentDictionary<INetworkHandler, byte> activeHandlers;
        private NetworkBufferSettings _networkBufferSettings;
        private LimitedFixedBufferPool _fixedBufferPool;
        protected RemoteCertificateValidationCallback certValidation = (a, b, c, d) => { return true; };
        private IGarnetServer garnetServer;
        private Dictionary<TcpClient, SslStream> _tcpClients;
        private IReadOnlyList<SslStream> sslStreams;

        [GlobalSetup]
        public virtual async Task GlobalSetup()
        {
            try
            {
                var tlsOptions = new GarnetTlsOptions(
                  certFileName: "testcert.pfx",
                  certPassword: "placeholder",
                  clientCertificateRequired: false,
                  certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                  issuerCertificatePath: "testcert.pfx",
                  null, 0, false, null);
                var serverBufferSize = BufferSizeUtils.ServerBufferSize(new MaxSizeSettings());
                garnetServer = new GarnetServerTcp("127.0.0.1", 3278, serverBufferSize, tlsOptions);
                garnetServer.Start();
                ThreadPool.SetMinThreads(4, 4);
                await SetupClientPool();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public IReadOnlyList<SslStream> GetStreams()
        {
            return sslStreams;
        }

        private async Task SetupClientPool()
        {
            _tcpClients = new Dictionary<TcpClient, SslStream>();
            for (int i = 0; i < 128; i++)
            {
                var client = new TcpClient();
                await client.ConnectAsync("127.0.0.1", 3278);
                var sslStream = new SslStream(client.GetStream(), false, certValidation, null);
                _tcpClients.Add(client, sslStream);
            }
            sslStreams = [.. _tcpClients.Values];
        }

        public void StartSocketAccept()
        {
            var endPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 3278);
            var acceptEventArg = new SocketAsyncEventArgs();
            _serverSocket.Bind(endPoint);
            _serverSocket.Listen(512);
            if (!_serverSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    var tlsOptions = new GarnetTlsOptions(
                           certFileName: "testcert.pfx",
                           certPassword: "placeholder",
                           clientCertificateRequired: false,
                           certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                           issuerCertificatePath: "testcert.pfx",
                           null, 0, false, null);
                    MockTcpNetworkHandler networkHandler = new MockTcpNetworkHandler(e.AcceptSocket, _networkBufferSettings, _fixedBufferPool, true);
                    if (activeHandlers.TryAdd(networkHandler, 0))
                    {
                        networkHandler.Start(tlsOptions.TlsServerOptions);
                    }

                    e.AcceptSocket = null;
                } while (!_serverSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
        }


        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            foreach (var tcpClient in _tcpClients)
            {
                tcpClient.Value.Dispose();
                tcpClient.Key.Dispose();
            }
            garnetServer.Dispose();
        }
    }
}