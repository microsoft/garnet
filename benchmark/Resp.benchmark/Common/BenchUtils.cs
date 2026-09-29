// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Garnet.common;
using Garnet.server;
using Garnet.server.TLS;
using StackExchange.Redis;

namespace Resp.benchmark
{
    public class BenchUtils
    {
        private static string issuerCertificatePath;

        /// <summary>
        /// SHA IDs for set and get scripts
        /// </summary>
        public static string sha1SetScript;
        public static string sha1GetScript;
        public static string sha1RetKeyScript;

        /// <summary>
        /// Loads a Set and Get script in memory
        /// </summary>
        /// <param name="client"></param>
        /// <param name="sha1SetScript"></param>
        /// <param name="sha1GetScript"></param>
        public static void LoadSetGetScripts(LightClient client, out string sha1SetScript, out string sha1GetScript)
        {
            // load set script in the server
            string script = "return redis.call('set', KEYS[1], ARGV[1])";
            var stringCmd = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${script.Length}\r\n{script}\r\n";
            client.Send(Encoding.ASCII.GetBytes(stringCmd), stringCmd.Length, 1);
            client.CompletePendingRequests();
            sha1SetScript = Encoding.ASCII.GetString(client.ResponseBuffer)[..45];

            // load get script in the server
            script = "return redis.call('get', KEYS[1])";
            stringCmd = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${script.Length}\r\n{script}\r\n";
            client.Send(Encoding.ASCII.GetBytes(stringCmd), stringCmd.Length, 1);
            client.CompletePendingRequests();
            sha1GetScript = Encoding.ASCII.GetString(client.ResponseBuffer)[..45];

            // load retkey script in the server
            script = "return KEYS[1]";
            stringCmd = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${script.Length}\r\n{script}\r\n";
            client.Send(Encoding.ASCII.GetBytes(stringCmd), stringCmd.Length, 1);
            client.CompletePendingRequests();
            sha1RetKeyScript = Encoding.ASCII.GetString(client.ResponseBuffer)[..45];
        }

        /// <summary>
        /// Get TLS options. NOTE: These are just test options, not for production use.
        /// </summary>
        /// <param name="tlsHost"></param>
        /// <param name="certFile"></param>
        /// <param name="certPassword"></param>
        /// <returns></returns>
        public static SslClientAuthenticationOptions GetTlsOptions(string tlsHost, string certFile, string certPassword, string issuerCertificatePath = null)
        {
            issuerCertificatePath ??= BenchUtils.issuerCertificatePath;
            return new SslClientAuthenticationOptions
            {
                ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                TargetHost = tlsHost,
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = (_, certificate, _, sslPolicyErrors)
                    => ValidateServerCertificate(certificate, sslPolicyErrors, issuerCertificatePath),
            };
        }

        public static ConfigurationOptions GetConfig(string address, int port = default, bool allowAdmin = false, bool useTLS = false, string tlsHost = null, string issuerCertificatePath = null)
        {
            issuerCertificatePath ??= BenchUtils.issuerCertificatePath;
            var commands = RespCommandsInfo.TryGetRespCommandNames(out var cmds)
                ? new HashSet<string>(cmds)
                : new HashSet<string>();

            var configOptions = new ConfigurationOptions
            {
                EndPoints = { { address, port }, },
                CommandMap = CommandMap.Create(commands),
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
                    return ValidateServerCertificate(cert, errors, issuerCertificatePath);
                };
            }

            return configOptions;
        }

        public static void ConfigureTlsValidation(string issuerCertificatePath)
            => BenchUtils.issuerCertificatePath = issuerCertificatePath;

        private static bool ValidateServerCertificate(X509Certificate certificate, SslPolicyErrors sslPolicyErrors, string issuerCertificatePath)
        {
            // Preserve the benchmark's existing test-oriented behavior unless a caller
            // explicitly supplies a CA. AzureBench always supplies one for TLS runs.
            if (string.IsNullOrEmpty(issuerCertificatePath))
                return true;

            if (certificate == null
                || sslPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable)
                || sslPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch))
                return false;

#pragma warning disable SYSLIB0057 // Required while Resp.benchmark still targets net8.0.
            using var serverCertificate = new X509Certificate2(certificate);
            using var issuerCertificate = new X509Certificate2(issuerCertificatePath);
#pragma warning restore SYSLIB0057
            using var validationChain = new X509Chain();
            validationChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            validationChain.ChainPolicy.CustomTrustStore.Add(issuerCertificate);
            validationChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            return validationChain.Build(serverCertificate);
        }
    }
}