// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BDN.benchmark.Operations;
using BenchmarkDotNet.Attributes;
using Garnet.server.Auth.Settings;
using Garnet.server;
using System.Runtime.CompilerServices;
using Garnet.server.TLS;
using System.Security.Cryptography.X509Certificates;
using System.Text;

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
            var tlsOptions = new GarnetTlsOptions(
                       certFileName: "C:\\Users\\padgupta\\GarnetPublic\\garnet\\benchmark\\BDN.benchmark\\bin\\Release\\net8.0\\ssl-cert.pfx",
                       certPassword: "",
                       clientCertificateRequired: false,
                       certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                       issuerCertificatePath: "C:\\Users\\padgupta\\GarnetPublic\\garnet\\benchmark\\BDN.benchmark\\bin\\Release\\net8.0\\ssl-cert.pfx",
                       null, 0, false, null);

            var server = new GarnetServerTcp("127.0.0.1", 3280, 0, tlsOptions);
            _server = server;
            _networkEchoCommandBuffer = Encoding.ASCII.GetBytes("\r\n\"*1\\r\\n$1\\r\\nECHO\\r\\n\"");

            _server.Start();
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