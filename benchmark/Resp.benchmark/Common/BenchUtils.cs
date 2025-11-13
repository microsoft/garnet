// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net.Security;
using System.Text;
using Garnet.common;
using Garnet.server;
using Garnet.server.TLS;
using StackExchange.Redis;

namespace Resp.benchmark
{
    public class BenchUtils
    {
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
        public static SslClientAuthenticationOptions GetTlsOptions(string tlsHost, string certFile, string certPassword)
        {
            return new SslClientAuthenticationOptions
            {
                ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                TargetHost = tlsHost,
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true,
            };
        }

        public static ConfigurationOptions GetConfig(string address, int port = default, bool allowAdmin = false, bool useTLS = false, string tlsHost = null)
        {
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
                    return true;
                };
            }

            return configOptions;
        }
    }
}