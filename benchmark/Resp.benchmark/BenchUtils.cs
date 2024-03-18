// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Garnet.server;
using StackExchange.Redis;

namespace Resp.benchmark
{
    public class BenchUtils
    {
        /// <summary>
        /// Get TLS options. NOTE: These are just test options, not for production use.
        /// </summary>
        /// <param name="tlsHost"></param>
        /// <param name="certFile"></param>
        /// <param name="certPassword"></param>
        /// <returns></returns>
        public static SslClientAuthenticationOptions GetTlsOptions(string tlsHost, string certFile, string certPassword)
        {
            return new SslClientAuthenticationOptions
            {
                ClientCertificates = [new X509Certificate2(certFile, certPassword)],
                TargetHost = tlsHost,
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true,
            };
        }

        public static ConfigurationOptions GetConfig(string address, int port = default, bool allowAdmin = false, bool useTLS = false, string tlsHost = null)
        {
            var configOptions = new ConfigurationOptions
            {
                EndPoints = { { address, port }, },
                CommandMap = CommandMap.Create(RespInfo.GetCommands()),
                ConnectTimeout = 100_000,
                SyncTimeout = 100_000,
                AllowAdmin = allowAdmin,
                Ssl = useTLS,
                SslHost = tlsHost,
            };

            if (useTLS)
            {
                configOptions.CertificateValidation += (sender, cert, chain, errors) =>
                {
                    Debug.WriteLine("Certificate validation errors: " + errors);
                    return true;
                };
            }

            return configOptions;
        }
    }
}