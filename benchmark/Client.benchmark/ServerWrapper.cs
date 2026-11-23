// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Garnet;
using Garnet.server;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Client.benchmark
{
    /// <summary>
    /// Hosts a single real <see cref="GarnetServer"/> in-process, bound to a loopback
    /// endpoint on an ephemeral port, configured for a pure in-memory workload (no
    /// storage tier, no AOF). The benchmark client connects to <see cref="EndPoint"/>
    /// over a real TCP socket, so the full network and server stack participates in the
    /// measurement.
    /// </summary>
    internal sealed class ServerWrapper : IDisposable
    {
        readonly GarnetServer server;
        readonly X509Certificate2 certificate;

        /// <summary>
        /// Client TLS options trusting only this run's certificate, or null for plaintext.
        /// </summary>
        public SslClientAuthenticationOptions TlsClientOptions { get; }

        /// <summary>
        /// The loopback endpoint the server is listening on.
        /// </summary>
        public IPEndPoint EndPoint { get; }

        /// <summary>
        /// The loopback host the server is listening on.
        /// </summary>
        public string Host => EndPoint.Address.ToString();

        /// <summary>
        /// The ephemeral port the server is listening on.
        /// </summary>
        public int Port => EndPoint.Port;

        /// <summary>
        /// Creates and starts an in-process server on a free loopback port.
        /// </summary>
        /// <param name="disableObjects">Disable the object (collection) types; the benchmark drives raw-string ops.</param>
        /// <param name="loggerFactory">Optional logger factory for server diagnostics.</param>
        /// <param name="enableTLS">Enable TLS with a temporary self-signed loopback certificate.</param>
        public ServerWrapper(bool disableObjects = true, ILoggerFactory loggerFactory = null, bool enableTLS = false)
        {
            EndPoint = new IPEndPoint(IPAddress.Loopback, GetFreeLoopbackPort());

            var opts = new GarnetServerOptions(loggerFactory?.CreateLogger("ServerWrapper"))
            {
                EndPoints = [EndPoint],
                EnableStorageTier = false,
                IndexMemorySize = "1g",
                DisableObjects = disableObjects,
                QuietMode = true,
                DeviceFactoryCreator = new LocalStorageNamedDeviceFactoryCreator(logger: loggerFactory?.CreateLogger("ServerWrapper")),
            };

            try
            {
                if (enableTLS)
                {
                    certificate = CreateCertificate();
                    opts.TlsOptions = new GarnetTlsOptions(
                        certFileName: null, certPassword: null,
                        clientCertificateRequired: false, certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                        issuerCertificatePath: null, certSubjectName: null, certificateRefreshFrequency: 0,
                        enableCluster: false, clientTargetHost: "localhost",
                        tlsServerOptionsOverride: new SslServerAuthenticationOptions
                        {
                            ServerCertificate = certificate,
                        });
                    TlsClientOptions = new SslClientAuthenticationOptions
                    {
                        TargetHost = "localhost",
                        CertificateChainPolicy = new X509ChainPolicy
                        {
                            TrustMode = X509ChainTrustMode.CustomRootTrust,
                            CustomTrustStore = { certificate },
                            RevocationMode = X509RevocationMode.NoCheck,
                        },
                    };
                }

                server = new GarnetServer(opts, loggerFactory);
                server.Start();
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        static X509Certificate2 CreateCertificate()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var names = new SubjectAlternativeNameBuilder();
            names.AddDnsName("localhost");
            names.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(names.Build());
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
            using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
            // Schannel requires a PKCS#12-imported private key rather than the ephemeral generated key.
#if NET9_0_OR_GREATER
            return X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), password: null);
#else
            return new X509Certificate2(generated.Export(X509ContentType.Pkcs12));
#endif
        }

        /// <summary>
        /// Reserves a free TCP port on the loopback interface.
        /// </summary>
        static int GetFreeLoopbackPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        public void Dispose()
        {
            try
            {
                server?.Dispose();
            }
            finally
            {
                certificate?.Dispose();
            }
        }
    }
}