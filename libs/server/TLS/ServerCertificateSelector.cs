// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server.TLS
{
    /// <summary>
    /// Ssl certificate selection to handle certificate refresh
    /// </summary>
    public sealed class ServerCertificateSelector
    {
        /// <summary>
        /// Ssl certificate retry duration in case of failures.
        /// </summary>
        readonly TimeSpan certificateRefreshRetryInterval = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Ssl certificate subject name.
        /// </summary>
        readonly string sslCertificateSubjectName;

        /// <summary>
        /// Ssl certificate file name.
        /// </summary>
        readonly string sslCertificateFileName;

        /// <summary>
        /// Ssl certificate file password
        /// </summary>
        readonly string sslCertificatePassword;

        readonly Timer _refreshTimer;
        readonly ILogger _logger;

        /// <summary>
        /// Ssl certificate retry duration
        /// </summary>
        readonly TimeSpan certRefreshFrequency;

        /// <summary>
        /// Ssl server certificate.
        /// </summary>
        X509Certificate2 sslServerCertificate;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerCertificateSelector"/> class.
        /// </summary>
        public ServerCertificateSelector(string subjectName, int certRefreshFrequencyInSeconds = 0, ILogger logger = null)
        {
            this._logger = logger;
            this.sslCertificateSubjectName = subjectName;

            // First get certificate synchronously on current call
            this.certRefreshFrequency = TimeSpan.Zero;
            GetServerCertificate(null);

            // Set up future timer, if needed
            // If the sync call failed to fetch certificate, we schedule the first call earlier (after certificateRefreshRetryInterval)
            this.certRefreshFrequency = TimeSpan.FromSeconds(certRefreshFrequencyInSeconds);
            if (certRefreshFrequency > TimeSpan.Zero)
                _refreshTimer = new Timer(GetServerCertificate, null, sslServerCertificate == null ? certificateRefreshRetryInterval : certRefreshFrequency, certRefreshFrequency);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerCertificateSelector"/> class.
        /// </summary>changed th
        public ServerCertificateSelector(string fileName, string filePassword, int certRefreshFrequencyInSeconds = 0, ILogger logger = null)
        {
            this._logger = logger;
            this.sslCertificateFileName = fileName;
            this.sslCertificatePassword = filePassword;

            // First get certificate synchronously on current call
            this.certRefreshFrequency = TimeSpan.Zero;
            GetServerCertificate(null);

            // Set up future timer, if needed
            // If the sync call failed to fetch certificate, we schedule the first call earlier (after certificateRefreshRetryInterval)
            this.certRefreshFrequency = TimeSpan.FromSeconds(certRefreshFrequencyInSeconds);
            if (certRefreshFrequency > TimeSpan.Zero)
                _refreshTimer = new Timer(GetServerCertificate, null, sslServerCertificate == null ? certificateRefreshRetryInterval : certRefreshFrequency, certRefreshFrequency);
        }

        /// <summary>
        /// End refresh timer
        /// </summary>
        public void EndTimer()
        {
            _refreshTimer?.Dispose();
        }

        /// <summary>
        /// Looks up the server certificate for authenticating an HTTPS connection.
        /// </summary>
        /// <returns>The X.509 certificate to use for server authentication.</returns>
        public X509Certificate2 GetSslServerCertificate()
        {
            return this.sslServerCertificate;
        }

        void GetServerCertificate(object _)
        {
            try
            {
                if (sslCertificateSubjectName != null)
                {
                    this.sslServerCertificate =
                        CertificateUtils.GetMachineCertificateBySubjectName(
                            this.sslCertificateSubjectName);
                }
                else
                {
                    this.sslServerCertificate =
                        CertificateUtils.GetMachineCertificateByFile(
                            this.sslCertificateFileName, this.sslCertificatePassword);
                }
            }
            catch (Exception ex)
            {
                if (certRefreshFrequency > TimeSpan.Zero)
                {
                    this._logger?.LogError(ex, $"Unable to fetch certificate. It will be retried after {certificateRefreshRetryInterval}");
                    try
                    {
                        _refreshTimer?.Change(certificateRefreshRetryInterval, certRefreshFrequency);
                    }
                    catch (ObjectDisposedException)
                    {
                        // Timer is disposed
                    }
                }
                else
                {
                    // This is not a background timer based call
                    this._logger?.LogError(ex, "Unable to fetch certificate using the provided filename and password. Make sure you specify a correct CertFileName and CertPassword.");
                }
            }
        }
    }
}