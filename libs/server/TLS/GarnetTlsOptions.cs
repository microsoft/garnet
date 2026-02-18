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

        /// <summary>
        /// Whether server requires a valid client certificate
        /// </summary>
        readonly bool ClientCertificateRequired;

        /// <summary>
        /// Whether client requires a valid server certificate
        /// </summary>
        readonly bool ServerCertificateRequired;

        /// <summary>
        /// Certificate revocation mode (shared by client and server)
        /// </summary>
        readonly X509RevocationMode CertificateRevocationCheckMode;

        /// <summary>
        /// Target (server) host name used by the client
        /// </summary>
        string ClientTargetHost;

        /// <summary>
        /// Issuer certificate path
        /// </summary>
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
            string clientTargetHost,
            bool serverCertificateRequired = false,
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

            this.ServerCertificateRequired = serverCertificateRequired;
            this.ClientTargetHost = clientTargetHost;
            this.IssuerCertificatePath = issuerCertificatePath;

            this.logger = logger;

            this.TlsServerOptions = tlsServerOptionsOverride;
            if (TlsServerOptions == null) TlsServerOptions = GetSslServerAuthenticationOptions();

            this.TlsClientOptions = clusterTlsClientOptionsOverride;
            if (TlsClientOptions == null && enableCluster) TlsClientOptions = GetSslClientAuthenticationOptions();
        }

        /// <inheritdoc />
        public bool UpdateCertFile(string certFileName, string certPassword, out string errorMessage)
        {
            if (CertSubjectName != null)
            {
                errorMessage = "Cannot use cert-file-name with cert-subject-name. Provide only one of them.";
                logger?.LogError("Cannot use cert-file-name with cert-subject-name. Provide only one of them.");
                return false;
            }
            if (certFileName == null)
            {
                errorMessage = "Cannot provide null cert-file-name.";
                logger?.LogError("Cannot provide null cert-file-name.");
                return false;
            }

            errorMessage = null;
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
                throw new GarnetException("CertificateRefreshFrequency should not be less than 0.");
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
                RemoteCertificateValidationCallback = ValidateClientCertificateCallback(IssuerCertificatePath),
                ServerCertificateSelectionCallback = (sender, hostName) =>
                {
                    return serverCertificateSelector.GetSslServerCertificate();
                }
            };
        }

        SslClientAuthenticationOptions GetSslClientAuthenticationOptions()
        {
            if (ServerCertificateRequired && string.IsNullOrEmpty(ClientTargetHost))
            {
                logger?.LogError("ClientTargetHost should be provided when ServerCertificateRequired is enabled");
                throw new GarnetException("ClientTargetHost should be provided when ServerCertificateRequired is enabled");
            }
            return new SslClientAuthenticationOptions
            {
                TargetHost = ClientTargetHost,
                AllowRenegotiation = false,
                CertificateRevocationCheckMode = CertificateRevocationCheckMode,
                RemoteCertificateValidationCallback = ValidateServerCertificateCallback(ClientTargetHost, IssuerCertificatePath),
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
        /// <param name="issuerCertificatePath">The path to issuer certificate file. </param>
        /// <returns></returns>
        RemoteCertificateValidationCallback ValidateServerCertificateCallback(string targetHostName, string issuerCertificatePath)
        {
            if (!ServerCertificateRequired)
            {
                logger?.LogWarning("ServerCertificateRequired is false. Remote certificate validation will always succeed.");
                return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                    => true;
            }
            var issuer = GetCertificateIssuer(issuerCertificatePath);
            return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                => (sslPolicyErrors == SslPolicyErrors.None) || (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors
                   && certificate is X509Certificate2 certificate2
                   && ValidateCertificateName(certificate2, targetHostName)
                   && ValidateCertificateIssuer(certificate2, issuer));
        }

        /// <summary>
        /// Validates certificate subject name by looking into DNS name property (preferred), if missing it falls back to
        /// legacy SimpleName. The input certificate subject should match the expected host name provided in server config.
        /// </summary>
        /// <param name="certificate2">The remote certificate to validate.</param>
        /// <param name="targetHostName">The expected target host name. </param>
        private bool ValidateCertificateName(X509Certificate2 certificate2, string targetHostName)
        {
            var subjectName = certificate2.GetNameInfo(X509NameType.DnsName, false);
            if (string.IsNullOrWhiteSpace(subjectName))
            {
                subjectName = certificate2.GetNameInfo(X509NameType.SimpleName, false);
            }

            return subjectName.Equals(targetHostName, StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary>
        /// Callback to verify the TLS certificate
        /// </summary>
        /// <param name="issuerCertificatePath">The path to issuer certificate file.</param>
        /// <returns>The RemoteCertificateValidationCallback delegate to invoke.</returns>
        RemoteCertificateValidationCallback ValidateClientCertificateCallback(string issuerCertificatePath)
        {
            if (!ClientCertificateRequired)
            {
                logger?.LogWarning("ClientCertificateRequired is false. Remote certificate validation will always succeed.");
                return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                    => true;
            }
            var issuer = GetCertificateIssuer(issuerCertificatePath);
            return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyErrors)
                => (sslPolicyErrors == SslPolicyErrors.None) || (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors
                    && certificate is X509Certificate2 certificate2
                    && ValidateCertificateIssuer(certificate2, issuer));
        }

        /// <summary>
        /// Loads an issuer X.509 certificate using its file name.
        /// </summary>
        /// <param name="issuerCertificatePath">The path to issuer certificate file.</param>
        X509Certificate2 GetCertificateIssuer(string issuerCertificatePath)
        {
            X509Certificate2 issuer = null;
            if (!string.IsNullOrEmpty(issuerCertificatePath))
            {
                try
                {
#if NET9_0_OR_GREATER
                    issuer = X509CertificateLoader.LoadCertificateFromFile(issuerCertificatePath);
#else
                    issuer = new X509Certificate2(issuerCertificatePath);
#endif
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error loading issuer certificate");
                    throw;
                }
            }
            else
                logger?.LogWarning("ClientCertificateRequired is true and IssuerCertificatePath is not provided. The remote certificate chain will not be validated against issuer.");

            return issuer;
        }

        /// <summary>
        /// Check the chain certificate and determine if the certificate is valid. NOTE: This is prototype code based on
        /// https://stackoverflow.com/questions/6497040/how-do-i-validate-that-a-certificate-was-created-by-a-particular-certification-a
        /// Make sure to validate for your requirements before using in production.
        /// </summary>
        /// <param name="certificateToValidate">X509Certificate2 certificate to be validated.</param>
        /// <param name="authority">X509Certificate2 representing the root cert.</param>
        /// <returns>A boolean indicating whether the certificate has a valid issuer.</returns>
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
                    string[] errors = [.. chain.ChainStatus.Select(x => String.Format("{0} ({1})", x.StatusInformation.Trim(), x.Status))];
                    string certificateErrorsString = "Unknown errors.";
                    if (errors != null && errors.Length > 0)
                        certificateErrorsString = String.Join(", ", errors);
                    throw new GarnetException("Trust chain did not complete to the known authority anchor. Errors: " + certificateErrorsString);
                }

                if (authority != null)
                {
                    // This piece makes sure it actually matches your known root
                    var valid = chain.ChainElements
                        .Cast<X509ChainElement>()
                        .Any(x => x.Certificate.Thumbprint == authority.Thumbprint);

                    if (!valid)
                        throw new GarnetException("Trust chain did not complete to the known authority anchor. Thumbprints did not match.");
                }
                return true;
            }
            catch (GarnetException ex)
            {
                logger?.LogError(ex, "Error validating certificate issuer");
                return false;
            }
        }
    }
}