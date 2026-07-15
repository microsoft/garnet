// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.server.TLS;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class CertificateUtilsTests
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void GetMachineCertificateByFileLoadsPfxCertificate()
        {
            var cert = CertificateUtils.GetMachineCertificateByFile(TestUtils.certFile, TestUtils.certPassword);

            ClassicAssert.IsNotNull(cert);
            ClassicAssert.IsTrue(cert.HasPrivateKey);
        }

        [Test]
        public void GetMachineCertificateByFileLoadsPemCertificateWithSeparateKeyFile()
        {
            var cert = CertificateUtils.GetMachineCertificateByFile(TestUtils.pemCertFile, TestUtils.pemCertKeyFile);

            ClassicAssert.IsNotNull(cert);
            ClassicAssert.IsTrue(cert.HasPrivateKey);
            StringAssert.Contains("CN=Garnet", cert.Subject);
        }

        [Test]
        public void GetMachineCertificateByFileLoadsPemCertificateWithEmbeddedKey()
        {
            // A single PEM file containing both the certificate and the private key should load
            // without a separate key file being specified.
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var combinedPemFile = Path.Combine(TestUtils.MethodTestDir, "combined.pem");
            File.WriteAllText(combinedPemFile, File.ReadAllText(TestUtils.pemCertFile) + File.ReadAllText(TestUtils.pemCertKeyFile));

            var cert = CertificateUtils.GetMachineCertificateByFile(combinedPemFile, null);

            ClassicAssert.IsNotNull(cert);
            ClassicAssert.IsTrue(cert.HasPrivateKey);
        }

        [Test]
        public void GetMachineCertificateByFileDetectsPemContentsRegardlessOfExtension()
        {
            // Format detection should be based on file contents, not the file extension.
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var renamedCertFile = Path.Combine(TestUtils.MethodTestDir, "testcert.pfx");
            File.Copy(TestUtils.pemCertFile, renamedCertFile, overwrite: true);

            var cert = CertificateUtils.GetMachineCertificateByFile(renamedCertFile, TestUtils.pemCertKeyFile);

            ClassicAssert.IsNotNull(cert);
            ClassicAssert.IsTrue(cert.HasPrivateKey);
        }
    }
}