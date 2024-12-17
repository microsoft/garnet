// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Security.Cryptography.X509Certificates;
using BenchmarkDotNet.Attributes;
using Garnet.server;
using Garnet.server.TLS;

namespace BDN.benchmark.Network
{
    public abstract class NetworkBase
    {
        private IGarnetServer _server;
        /// <summary>
        /// Base class for operations benchmarks
        /// </summary>

        protected byte[] _networkEchoCommandBuffer;

        [GlobalSetup]
        public virtual void GlobalSetup()
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

                var server = new GarnetServerTcp("127.0.0.1", 3278, 0, tlsOptions);
                _server = server;
                _server.Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            _server.Dispose();
        }
    }
}