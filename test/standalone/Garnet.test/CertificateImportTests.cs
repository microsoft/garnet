// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Garnet.server.TLS;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class CertificateImportTests : TestBase
    {
        const int MissingKeyStore = unchecked((int)0x80070002);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown(waitForDelete: true);

        [TestCase(false)]
        [TestCase(true)]
        public void DefaultImportSuccessDoesNotRetry(bool isWindows)
        {
            using var expected = X509CertificateLoader.LoadCertificateFromFile(TestUtils.pemCertFile);
            var calls = new List<X509KeyStorageFlags>();
            var actual = CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                return expected;
            }, isWindows);

            Assert.That(actual, Is.SameAs(expected));
            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet]));
        }

        [Test]
        public void MissingWindowsKeyStoreRetriesWithMachineKeySetOnce()
        {
            using var expected = X509CertificateLoader.LoadCertificateFromFile(TestUtils.pemCertFile);
            var calls = new List<X509KeyStorageFlags>();
            var actual = CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                if (calls.Count == 1)
                    throw new CryptographicException(MissingKeyStore);
                return expected;
            }, isWindows: true);

            Assert.That(actual, Is.SameAs(expected));
            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet, X509KeyStorageFlags.MachineKeySet]));
        }

        [TestCase(false, MissingKeyStore)]
        [TestCase(true, unchecked((int)0x80070056))] // Invalid password
        [TestCase(true, unchecked((int)0x8007000D))] // Invalid data
        [TestCase(true, unchecked((int)0x80090005))] // NTE_BAD_DATA
        [TestCase(true, unchecked((int)0x80070005))] // Access denied
        [TestCase(true, unchecked((int)0x80090016))] // NTE_BAD_KEYSET is not the reported HRESULT
        public void OtherImportFailuresPropagateWithoutRetry(bool isWindows, int hresult)
        {
            var expected = new CryptographicException(hresult);
            var calls = new List<X509KeyStorageFlags>();
            var actual = Assert.Throws<CryptographicException>(() => CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                throw expected;
            }, isWindows));

            Assert.That(actual, Is.SameAs(expected));
            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet]));
        }

        [Test]
        public void MissingFileDoesNotRetry()
        {
            var expected = new FileNotFoundException();
            Assert.That(expected.HResult, Is.EqualTo(MissingKeyStore));
            var calls = new List<X509KeyStorageFlags>();
            var actual = Assert.Throws<FileNotFoundException>(() => CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                throw expected;
            }, isWindows: true));

            Assert.That(actual, Is.SameAs(expected));
            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet]));
        }

        [TestCase(MissingKeyStore)]
        [TestCase(unchecked((int)0x80070005))]
        public void MachineKeySetFailurePropagatesWithoutFurtherRetry(int hresult)
        {
            var expected = new CryptographicException(hresult);
            var calls = new List<X509KeyStorageFlags>();
            var actual = Assert.Throws<CryptographicException>(() => CertificateUtils.LoadPkcs12WithFallback(flags =>
            {
                calls.Add(flags);
                throw calls.Count == 1 ? new CryptographicException(MissingKeyStore) : expected;
            }, isWindows: true));

            Assert.That(actual, Is.SameAs(expected));
            Assert.That(calls, Is.EqualTo([X509KeyStorageFlags.DefaultKeySet, X509KeyStorageFlags.MachineKeySet]));
        }
    }
}