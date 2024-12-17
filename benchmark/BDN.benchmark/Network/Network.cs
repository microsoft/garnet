// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace BDN.benchmark.Network
{
    public class DummyServerHook : IServerHook
    {
        public bool Disposed => false;
        public void DisposeMessageConsumer(INetworkHandler session) { }
        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session) { session = null; return false; }
    }

    public class MockTcpNetworkHandler : TcpNetworkHandlerBase<IServerHook, GarnetTcpNetworkSender>
    {
        public MockTcpNetworkHandler(Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null) : base(new DummyServerHook(), new GarnetTcpNetworkSender(socket, networkBufferSettings, networkPool), socket, networkBufferSettings, networkPool, useTLS, messageConsumer, logger)
        {
        }
    }
    [MemoryDiagnoser]
    public class Network : NetworkBase
    {
       
        [Benchmark]
        public async Task TestNetworkTask()
        {
            var sslStreams = GetStreams();
            var sslStreamWrites = new List<Task>();
            var sslClientAuthOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = certValidation };
            foreach (var stream in sslStreams)
            {
                sslStreamWrites.Add(stream.AuthenticateAsClientAsync(sslClientAuthOptions));
            }
            await Task.WhenAll(sslStreamWrites);

        }
    }
}