// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// Gets machine certificate by file name
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static X509Certificate2 GetMachineCertificateByFile(string fileName, string password)
        {
            return new X509Certificate2(fileName, password);
        }
    }
}