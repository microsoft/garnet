// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Garnet.server.TLS
{
    /// <summary>
    /// CertificateUtils
    /// </summary>
    public static class CertificateUtils
    {
        /// <summary>
        /// Marker that PEM-encoded files (certificates and keys alike) start with, used to
        /// distinguish them from PKCS#12/PFX files regardless of the file's extension.
        /// </summary>
        static ReadOnlySpan<byte> PemMarker => "-----BEGIN"u8;

        /// <summary>
        /// Gets machine certificate by subject name
        /// </summary>
        /// <param name="subjectName"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static X509Certificate2 GetMachineCertificateBySubjectName(string subjectName)
        {
            X509Store store = null;
            X509Certificate2 certificate;

            try
            {
                store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
                store.Open(OpenFlags.ReadOnly);

                var certificateCollection = store.Certificates
                    .Find(X509FindType.FindBySubjectName, subjectName, false);

                if (certificateCollection.Count <= 0)
                {
                    throw new ArgumentException(
                        $"Unable to load certificate with subject name {subjectName}");
                }

                var latestMatchingCert = certificateCollection.OfType<X509Certificate2>().OrderByDescending(cert => cert.NotAfter).First();
                certificate = new X509Certificate2(latestMatchingCert);
            }
            finally
            {
                store?.Close();
            }

            return certificate;
        }


        /// <summary>
        /// Gets machine certificate by file name. The certificate format (PKCS#12/PFX or PEM) is
        /// detected from the file's contents rather than trusted from its extension.
        /// </summary>
        /// <param name="fileName">Path to the certificate file.</param>
        /// <param name="password">
        /// For a PKCS#12/PFX file, the password protecting it. For a PEM-encoded certificate, this is
        /// instead treated as the path to a separate PEM-encoded private key file, or null/empty if
        /// the private key is already included in <paramref name="fileName"/>.
        /// </param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static X509Certificate2 GetMachineCertificateByFile(string fileName, string password)
        {
            if (IsPemFile(fileName))
                return GetCertificateFromPemFile(fileName, password);

#if NET9_0_OR_GREATER
            return X509CertificateLoader.LoadPkcs12FromFile(fileName, password);
#else
            return new X509Certificate2(fileName, password);
#endif
        }

        /// <summary>
        /// Loads a certificate and its private key from PEM-encoded file(s).
        /// </summary>
        /// <param name="certFileName">Path to the PEM-encoded certificate file.</param>
        /// <param name="keyFileName">Path to the PEM-encoded private key file, or null/empty if the private key is included in <paramref name="certFileName"/>.</param>
        static X509Certificate2 GetCertificateFromPemFile(string certFileName, string keyFileName)
        {
            using var pemCertificate = X509Certificate2.CreateFromPemFile(certFileName, string.IsNullOrEmpty(keyFileName) ? null : keyFileName);

            // The certificate above carries an ephemeral key set, which isn't supported by SslStream on
            // all platforms. Re-import it via PKCS#12 to get a certificate with a persisted key set.
#if NET9_0_OR_GREATER
            return X509CertificateLoader.LoadPkcs12(pemCertificate.Export(X509ContentType.Pkcs12), password: null);
#else
#pragma warning disable SYSLIB0057 // Type or member is obsolete
            return new X509Certificate2(pemCertificate.Export(X509ContentType.Pkcs12));
#pragma warning restore SYSLIB0057
#endif
        }

        /// <summary>
        /// UTF-8 byte order mark, tolerated before the PEM marker since some editors save PEM files
        /// with one even though the format doesn't call for it.
        /// </summary>
        static ReadOnlySpan<byte> Utf8Bom => [0xEF, 0xBB, 0xBF];

        /// <summary>
        /// Determines whether a certificate file is PEM-encoded by inspecting its leading bytes for
        /// the "-----BEGIN" marker, rather than relying on the file's extension. Tolerates a leading
        /// UTF-8 BOM and leading blank lines/whitespace ahead of the marker.
        /// </summary>
        static bool IsPemFile(string fileName)
        {
            // Sized generously so a BOM plus a few blank lines still leave room for the marker itself.
            Span<byte> buffer = stackalloc byte[64];

            using var stream = File.OpenRead(fileName);
            var totalRead = 0;
            int bytesRead;
            while (totalRead < buffer.Length && (bytesRead = stream.Read(buffer[totalRead..])) > 0)
                totalRead += bytesRead;

            ReadOnlySpan<byte> prefix = buffer[..totalRead];

            if (prefix.StartsWith(Utf8Bom))
                prefix = prefix[Utf8Bom.Length..];

            prefix = prefix.TrimStart(" \t\r\n"u8);

            return prefix.StartsWith(PemMarker);
        }
    }
}