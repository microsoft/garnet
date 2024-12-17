// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Security;
using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Network
{
    [MemoryDiagnoser]
    public class Network : NetworkBase
    {
        [Benchmark]
        public async Task TestNetworkTask()
        {
            var sslStreams = GetStreams();
            var sslStreamWrites = new List<Task>(MaxConnections);
            var sslClientAuthOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = CertValidationCallback };
            foreach (var stream in sslStreams)
            {
                sslStreamWrites.Add(stream.AuthenticateAsClientAsync(sslClientAuthOptions));
            }
            await Task.WhenAll(sslStreamWrites);
        }
    }
}