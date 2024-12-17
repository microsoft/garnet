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
        /// <summary>
        /// Base class for operations benchmarks
        /// </summary>
        protected RemoteCertificateValidationCallback CertValidationCallback = (a, b, c, d) => { return true; };
        private IGarnetServer GarnetServer;
        private Dictionary<TcpClient, SslStream> TcpClients;
        private IReadOnlyList<SslStream> SslStreams;
        protected const int MaxConnections = 128;

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
                GarnetServer = new GarnetServerTcp("127.0.0.1", 3278, serverBufferSize, tlsOptions);
                GarnetServer.Start();
                // This is done to compare sync vs async workloads since blocking patterns exhaust threadpool.
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
            return SslStreams;
        }

        private async Task SetupClientPool()
        {
            TcpClients = new Dictionary<TcpClient, SslStream>();
            for (int i = 0; i < MaxConnections; i++)
            {
                var client = new TcpClient();
                await client.ConnectAsync("127.0.0.1", 3278);
                var sslStream = new SslStream(client.GetStream(), false, CertValidationCallback, null);
                TcpClients.Add(client, sslStream);
            }
            SslStreams = [.. TcpClients.Values];
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            foreach (var tcpClient in TcpClients)
            {
                tcpClient.Value.Dispose();
                tcpClient.Key.Dispose();
            }
            GarnetServer.Dispose();
        }
    }
}