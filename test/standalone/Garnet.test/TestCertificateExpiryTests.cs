// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Guards the checked-in TLS test certificates against expiry.
    ///
    /// An expired certificate does not surface as a TLS test failure. Under TLS 1.3 the client completes its
    /// half of the handshake before the server validates the client certificate, so <c>ConnectAsync</c>
    /// succeeds and the first request is the one that dies. The server then closes without a RESP error --
    /// deliberately, because writing one mid-handshake would be a protocol violation -- leaving the client
    /// awaiting a reply that never arrives. The test host wedges until the hang detector kills it, taking the
    /// whole run with it and reporting a crash rather than naming the cause.
    ///
    /// These tests turn that into an immediate, self-explaining failure.
    /// <para>
    /// The certificates carry a one-year validity, so this is a recurring deadline rather than a one-off.
    /// </para>
    /// </summary>
    [TestFixture]
    public class TestCertificateExpiryTests : TestBase
    {
        /// <summary>
        /// Lead time demanded before expiry. Renewing a test certificate means regenerating it, committing it
        /// and merging, so the warning has to arrive far enough ahead to be actionable rather than on the day
        /// the suite starts hanging.
        /// </summary>
        const int RenewalLeadTimeDays = 30;

        const string RegenerateHint =
            "Regenerate the test certificates following test/testcerts/README.md, then commit testcert.pfx, " +
            "testcert.pem, testcert.key.pem, garnet-ca.crt, garnet-ca.key, garnet-cert.crt and " +
            "garnet-cert.key together so the pfx and its companion PEM artifacts stay consistent.";

        static X509Certificate2 LoadPfx() => TestUtils.GetClientCertificate();

        static X509Certificate2 LoadPem() => X509Certificate2.CreateFromPem(File.ReadAllText(TestUtils.pemCertFile));

        [Test]
        public void TlsTestCertificatesAreCurrentlyValid()
        {
            var now = DateTime.Now;

            using var pem = LoadPem();
            var pfx = LoadPfx();

            foreach (var (name, cert) in new[] { (TestUtils.certFile, pfx), (TestUtils.pemCertFile, pem) })
            {
                Assert.That(now, Is.LessThan(cert.NotAfter),
                    $"TLS test certificate '{name}' expired on {cert.NotAfter:u}. TLS tests will hang rather than " +
                    $"fail, wedging the test host. {RegenerateHint}");

                Assert.That(now, Is.GreaterThanOrEqualTo(cert.NotBefore),
                    $"TLS test certificate '{name}' is not valid until {cert.NotBefore:u}, which is in the future. " +
                    $"Check the machine clock before regenerating. {RegenerateHint}");
            }
        }

        [Test]
        public void TlsTestCertificatesHaveRenewalLeadTime()
        {
            var now = DateTime.Now;

            using var pem = LoadPem();
            var pfx = LoadPfx();

            foreach (var (name, cert) in new[] { (TestUtils.certFile, pfx), (TestUtils.pemCertFile, pem) })
            {
                var remaining = cert.NotAfter - now;
                Assert.That(remaining, Is.GreaterThan(TimeSpan.FromDays(RenewalLeadTimeDays)),
                    $"TLS test certificate '{name}' expires on {cert.NotAfter:u}, in {remaining.Days} day(s). " +
                    $"Renew it before it lapses: once expired, TLS tests hang instead of failing. {RegenerateHint}");
            }
        }
    }
}
