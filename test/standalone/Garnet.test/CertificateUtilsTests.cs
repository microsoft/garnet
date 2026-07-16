// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.server.TLS;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class CertificateUtilsTests : TestBase
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown(waitForDelete: true);
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
            var certContents = File.ReadAllText(TestUtils.pemCertFile);
            if (!certContents.EndsWith('\n'))
                certContents += Environment.NewLine;
            File.WriteAllText(combinedPemFile, certContents + File.ReadAllText(TestUtils.pemCertKeyFile));

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

        [Test]
        public void GetMachineCertificateByFileDetectsPemWithLeadingBomAndBlankLines()
        {
            // Some editors save PEM files with a UTF-8 BOM and/or a blank line or two before the
            // "-----BEGIN" marker. Detection should tolerate that instead of falling through to the
            // PKCS#12 path and failing.
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var bomPemFile = Path.Combine(TestUtils.MethodTestDir, "bom.pem");
            var contents = "\r\n\n" + File.ReadAllText(TestUtils.pemCertFile);
            File.WriteAllText(bomPemFile, contents, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

            var cert = CertificateUtils.GetMachineCertificateByFile(bomPemFile, TestUtils.pemCertKeyFile);

            ClassicAssert.IsNotNull(cert);
            ClassicAssert.IsTrue(cert.HasPrivateKey);
        }
    }
}