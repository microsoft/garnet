// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Security;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Network
{
    [MemoryDiagnoser]
    public class Network : NetworkBase
    {
        [Benchmark]
        public async Task TestNetworkTask()
        {
            RemoteCertificateValidationCallback certValidation = (a, b, c, d) => { return true; };
            using (var client = new TcpClient())
            {
                await client.ConnectAsync("127.0.0.1", 3278);
                using (var sslStream = new SslStream(client.GetStream(), false, certValidation, null))
                {
                    await sslStream.AuthenticateAsClientAsync("127.0.0.1");
                }
            }
        }
    }
}