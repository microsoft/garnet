// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.common;
using Garnet.networking;
using Garnet.server.TLS;
using Garnet.server;

namespace BDN.benchmark.Network
{
    [MemoryDiagnoser]
    public class Network : NetworkBase
    {


        [Benchmark]
        public void TestNetworkTask()
        {
            RemoteCertificateValidationCallback certValidation = (a, b, c, d) => { return true; };
            using (var client = new TcpClient())
            {
                client.ConnectAsync("127.0.0.1", 3280).ConfigureAwait(false).GetAwaiter().GetResult();
                using (var sslStream = new SslStream(client.GetStream(), false, certValidation, null))
                {
                    sslStream.AuthenticateAsClientAsync("127.0.0.1").Wait();
                    sslStream.Write(_networkEchoCommandBuffer);
                    sslStream.Flush();
                }
            }
        }   
    }
}
