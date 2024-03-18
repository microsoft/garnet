// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server.TLS
{
    /// <summary>
    /// A sample Garnet TLS options class provided for default validations.
    /// NOTE: Do not use in production without verifying the implementation yourself. This class
    /// can be replaced with your own implementation when instantiating GarnetServerOptions.
    /// </summary>
    public class GarnetTlsOptions : IGarnetTlsOptions
    {
        /// <summary>
        /// TLS Server Options
        /// </summary>
        public SslServerAuthenticationOptions TlsServerOptions { get; private set; }

        /// <summary>
        /// TLS Client Options
        /// </summary>
        public SslClientAuthenticationOptions TlsClientOptions { get; private set; }

        string CertFileName, CertPassword;

        readonly string CertSubjectName;
        readonly int CertificateRefreshFrequency;

        readonly bool ClientCertificateRequired;
        readonly X509RevocationMode CertificateRevocationCheckMode;

        string ClusterTlsClientTargetHost;
        string IssuerCertificatePath = string.Empty;

        ServerCertificateSelector serverCertificateSelector;

        ILogger logger = null;

        /// <summary>
        /// Constructor
        /// </summary>
        public GarnetTlsOptions(
            string certFileName, string certPassword,
            bool clientCertificateRequired, X509RevocationMode certificateRevocationCheckMode, string issuerCertificatePath,
            string certSubjectName, int certificateRefreshFrequency,
            bool enableCluster,
            string clusterTlsClientTargetHost,
            SslServerAuthenticationOptions tlsServerOptionsOverride = null,
            SslClientAuthenticationOptions clusterTlsClientOptionsOverride = null,
            ILogger logger = null)
        {
            this.CertFileName = certFileName;
            this.CertPassword = certPassword;
            this.ClientCertificateRequired = clientCertificateRequired;
            this.CertificateRevocationCheckMode = certificateRevocationCheckMode;
            this.CertSubjectName = certSubjectName;
            this.CertificateRefreshFrequency = certificateRefreshFrequency;

            this.ClusterTlsClientTargetHost = clusterTlsClientTargetHost;
            this.IssuerCertificatePath = issuerCertificatePath;

            this.logger = logger;

            this.TlsServerOptions = tlsServerOptionsOverride;
            if (TlsServerOptions == null) TlsServerOptions = GetSslServerAuthenticationOptions();

            this.TlsClientOptions = clusterTlsClientOptionsOverride;
            if (TlsClientOptions == null && enableCluster) TlsClientOptions = GetSslClientAuthenticationOptions();
        }

        /// <summary>
        /// Update certificate file name
        /// </summary>
        /// <param name="certFileName"></param>
        /// <param name="certPassword"></param>
        /// <param name="errorMsg"></param>
        public bool UpdateCertFile(string certFileName, string certPassword, out string errorMsg)
        {
            if (CertSubjectName != null)
            {
                errorMsg = "Cannot use cert-file-name with cert-subject-name. Provide only one of them.";
                logger?.LogError("Cannot use cert-file-name with cert-subject-name. Provide only one of them.");
                return false;
            }
            if (certFileName == null)
            {
                errorMsg = "Cannot provide null cert-file-name.";
                logger?.LogError("Cannot provide null cert-file-name.");
                return false;
            }

            errorMsg = null;
            CertFileName = certFileName;
            CertPassword = certPassword;
            TlsServerOptions = GetSslServerAuthenticationOptions();
            return true;
        }

        SslServerAuthenticationOptions GetSslServerAuthenticationOptions()
        {
            if (CertFileName == null && CertSubjectName == null)
            {
                logger?.LogError("CertFileName and CertSubjectName cannot both be null.");
                throw new GarnetException("CertFileName and CertSubjectName cannot both be null.");
            }

            if (CertFileName != null && CertSubjectName != null)
            {
                logger?.LogError("Cannot use CertFileName with CertSubjectName. Provide only one of them.");
                throw new GarnetException("Cannot use CertFileName with CertSubjectName. Provide only one of them.");
            }

            // We support CertSubjectName only on Windows
            if (CertSubjectName != null && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logger?.LogError("CertSubjectName is supported only on Windows.");
                throw new GarnetException("CertSubjectName is supported only on Windows.");
            }

            if (CertificateRefreshFrequency < 0)
            {
                logger?.LogError("CertificateRefreshFrequency should not be less than 0.");
                throw new Exception("CertificateRefreshFrequency should not be less than 0.");
            }

            // End timer associated with old certificate selector, if any
            serverCertificateSelector?.EndTimer();

            // Create new certificate selector
            if (CertSubjectName == null)
                serverCertificateSelector = new ServerCertificateSelector(CertFileName, CertPassword, CertificateRefreshFrequency, logger);
            else
                serverCertificateSelector = new ServerCertificateSelector(CertSubjectName, CertificateRefreshFrequency, logger);

            return new SslServerAuthenticationOptions
            {
                ClientCertificateRequired = ClientCertificateRequired,
                CertificateRevocationCheckMode = CertificateRevocationCheckMode,
                RemoteCertificateValidationCallback = ValidateCertificateCallback(IssuerCertificatePath),
                ServerCertificateSelectionCallback = (sender, hostName) =>
                {
                    return serverCertificateSelector.GetSslServerCertificate();
                }
            };
        }

        SslClientAuthenticationOptions GetSslClientAuthenticationOptions()
        {
            return new SslClientAuthenticationOptions
            {
                TargetHost = ClusterTlsClientTargetHost,
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = ValidateCertificateCallback(IssuerCertificatePath),
                // We use the same server certificate selector for the server's own client as well
                LocalCertificateSelectionCallback = (object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers) =>
                {
                    return serverCertificateSelector.GetSslServerCertificate();
                }
            };
        }

        /// <summary>
        /// Callback to verify the TLS certificate
        /// </summary>
        /// <param name="issuerCertificatePath"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        RemoteCertificateValidationCallback ValidateCertificateCallback(string issuerCertificatePath)
        {
            if (!ClientCertificateRequired)
            {
                logger?.LogWarning("ClientCertificateRequired is false. Remote certificate validation will always succeed.");
                return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                    => true;
            }
            X509Certificate2 issuer = null;
            if (!string.IsNullOrEmpty(issuerCertificatePath))
            {
                try
                {
                    issuer = new X509Certificate2(issuerCertificatePath);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error loading issuer certificate");
                    throw;
                }
            }
            else
                logger?.LogWarning("ClientCertificateRequired is true and IssuerCertificatePath is not provided. The remote certificate chain will not be validated against issuer.");

            return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                => (sslPolicyErrors == SslPolicyErrors.None) || (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors
                    && certificate is X509Certificate2 certificate2
                    && ValidateCertificateIssuer(certificate2, issuer));
        }

        /// <summary>
        /// Check the chain certificate and determine if the certificate is valid. NOTE: This is prototype code based on
        /// https://stackoverflow.com/questions/6497040/how-do-i-validate-that-a-certificate-was-created-by-a-particular-certification-a
        /// Make sure to validate for your requirements before using in production.
        /// </summary>
        /// <param name="certificateToValidate"></param>
        /// <param name="authority"></param>
        /// <returns></returns>
        bool ValidateCertificateIssuer(X509Certificate2 certificateToValidate, X509Certificate2 authority)
        {
            using X509Chain chain = new();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.RevocationFlag = X509RevocationFlag.ExcludeRoot;
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            chain.ChainPolicy.VerificationTime = DateTime.Now;
            chain.ChainPolicy.UrlRetrievalTimeout = new TimeSpan(0, 0, 0);

            if (authority != null) chain.ChainPolicy.ExtraStore.Add(authority);

            try
            {
                var chainBuilt = chain.Build(certificateToValidate);
                if (!chainBuilt)
                {
                    string[] errors = chain.ChainStatus
                        .Select(x => String.Format("{0} ({1})", x.StatusInformation.Trim(), x.Status))
                        .ToArray();
                    string certificateErrorsString = "Unknown errors.";
                    if (errors != null && errors.Length > 0)
                        certificateErrorsString = String.Join(", ", errors);
                    throw new Exception("Trust chain did not complete to the known authority anchor. Errors: " + certificateErrorsString);
                }

                if (authority != null)
                {
                    // This piece makes sure it actually matches your known root
                    var valid = chain.ChainElements
                        .Cast<X509ChainElement>()
                        .Any(x => x.Certificate.Thumbprint == authority.Thumbprint);

                    if (!valid)
                        throw new Exception("Trust chain did not complete to the known authority anchor. Thumbprints did not match.");
                }
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error validating certificate issuer");
                return false;
            }
        }
    }
}