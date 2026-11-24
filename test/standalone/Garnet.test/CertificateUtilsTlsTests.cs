// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server.TLS;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class CertificateUtilsTlsTests : TestBase
    {
        const string Password = "certificate-test-password";
        string certFile;
        string keyFile;
        string pfxFile;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            certFile = Path.Combine(TestUtils.MethodTestDir, "server.pem");
            keyFile = Path.Combine(TestUtils.MethodTestDir, "server.key");
            // Deliberately use a PEM extension for PFX contents to exercise format detection.
            pfxFile = Path.Combine(TestUtils.MethodTestDir, "server-pfx.pem");
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var names = new SubjectAlternativeNameBuilder();
            names.AddDnsName("localhost");
            request.CertificateExtensions.Add(names.Build());
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], true));
            using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
            File.WriteAllText(certFile, certificate.ExportCertificatePem());
            File.WriteAllText(keyFile, key.ExportPkcs8PrivateKeyPem());
            File.WriteAllBytes(pfxFile, certificate.Export(X509ContentType.Pkcs12, Password));
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown(waitForDelete: true);

        [TestCase(false)]
        [TestCase(true)]
        public async Task LoadedCertificateSupportsTlsAsync(bool pem)
        {
            using var certificate = LoadCertificate(pem);
            await AssertTlsHandshakeAsync(certificate);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task MachineKeySetRetrySupportsTlsOnWindowsAsync(bool pem)
        {
            if (!OperatingSystem.IsWindows())
                Assert.Ignore("Requires Windows machine key storage and SslStream.");

            // Inject only the initial failure; the retry imports a real key and completes a real handshake.
            using var pemCertificate = pem ? X509Certificate2.CreateFromPemFile(certFile, keyFile) : null;
            var pkcs12 = pemCertificate?.Export(X509ContentType.Pkcs12);
            var calls = new List<X509KeyStorageFlags>();
            using var certificate = CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                if (calls.Count == 1)
                    throw new CryptographicException(unchecked((int)0x80070002));
                return pem
                    ? X509CertificateLoader.LoadPkcs12(pkcs12, password: null, flags)
                    : X509CertificateLoader.LoadPkcs12FromFile(pfxFile, Password, flags);
            }, OperatingSystem.IsWindows());

            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet, X509KeyStorageFlags.MachineKeySet]));
            await AssertTlsHandshakeAsync(certificate);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void LoadedCertificateRejectsWrongTlsHostname(bool pem)
        {
            using var certificate = LoadCertificate(pem);
            Assert.ThrowsAsync<AuthenticationException>(() => AssertTlsHandshakeAsync(certificate, "wrong-hostname"));
        }

        [Test]
        public void WrongPfxPasswordIsRejected()
        {
            Assert.Throws<CryptographicException>(() =>
            {
                using var certificate = CertificateUtils.GetMachineCertificateByFile(pfxFile, "wrong-password");
            });
        }

        [TestCase(false)]
        [TestCase(true)]
        public void MalformedCertificateIsRejected(bool pem)
        {
            var path = pem ? certFile : pfxFile;
            File.WriteAllText(path, pem ? "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----" : "invalid-pfx");
            Assert.Throws<CryptographicException>(() =>
            {
                using var certificate = LoadCertificate(pem);
            });
        }

        [Test]
        public void MismatchedPemPrivateKeyIsRejected()
        {
            using var otherKey = RSA.Create(2048);
            File.WriteAllText(keyFile, otherKey.ExportPkcs8PrivateKeyPem());
            Assert.Throws<CryptographicException>(() =>
            {
                using var certificate = LoadCertificate(pem: true);
            });
        }

        [Test]
        public void MissingPemPrivateKeyIsRejected()
        {
            File.Delete(keyFile);
            Assert.Throws<FileNotFoundException>(() =>
            {
                using var certificate = LoadCertificate(pem: true);
            });
        }

        X509Certificate2 LoadCertificate(bool pem) => CertificateUtils.GetMachineCertificateByFile(
            pem ? certFile : pfxFile, pem ? keyFile : Password);

        // Uses explicit trust for this test certificate while retaining chain and hostname validation.
        internal static async Task AssertTlsHandshakeAsync(X509Certificate2 certificate, string targetHost = "localhost")
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var token = deadline.Token;
            using var trustedCertificate = X509CertificateLoader.LoadCertificate(certificate.RawData);
            var endpoint = (IPEndPoint)TestUtils.EndPoint;
            using var listener = new TcpListener(endpoint);
            listener.Start();
            using var client = new TcpClient();
            await client.ConnectAsync(endpoint, token);
            using var peer = await listener.AcceptTcpClientAsync(token);
            using var serverStream = new SslStream(peer.GetStream());
            using var clientStream = new SslStream(client.GetStream());
            var serverHandshake = serverStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = certificate,
                EnabledSslProtocols = SslProtocols.Tls12,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck
            }, token);
            var clientHandshake = clientStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = targetHost,
                EnabledSslProtocols = SslProtocols.Tls12,
                CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust,
                    CustomTrustStore = { trustedCertificate },
                    RevocationMode = X509RevocationMode.NoCheck
                }
            }, token);
            // Observe both tasks, including when certificate validation fails.
            await Task.WhenAll(clientHandshake, serverHandshake);
            Assert.That(clientStream.IsAuthenticated && serverStream.IsAuthenticated, Is.True);
            await clientStream.WriteAsync(new byte[] { 42 }, token);
            var received = new byte[1];
            await serverStream.ReadExactlyAsync(received, token);
            Assert.That(received[0], Is.EqualTo(42));
        }
    }
}