// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Security;
using StackExchange.Redis;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Builds StackExchange.Redis connection settings so that every connection the simulator opens
    /// (workload clients and the key space loader) negotiates the transport the same way.
    /// </summary>
    public static class SeRedisConfig
    {
        /// <summary>
        /// Creates connection settings for the configured target, applying TLS when requested.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <param name="timeoutMs">Sync and async operation timeout in milliseconds.</param>
        /// <returns>Configuration options for a new multiplexer.</returns>
        public static ConfigurationOptions Build(Options opts, int timeoutMs)
        {
            var config = new ConfigurationOptions
            {
                EndPoints = { { opts.Host, opts.Port } },
                AbortOnConnectFail = true,
                ConnectTimeout = 15000,
                SyncTimeout = timeoutMs,
                AsyncTimeout = timeoutMs,
                ConnectRetry = 3,
            };

            if (opts.Tls)
            {
                config.Ssl = true;
                config.SslHost = opts.TlsHost;
                config.SslClientAuthenticationOptions = host => new SslClientAuthenticationOptions
                {
                    TargetHost = opts.TlsHost,
                    AllowRenegotiation = false,
                    // The simulator uses a self-signed certificate, so validation is bypassed to keep
                    // the measurement on transport cost rather than on PKI setup.
                    RemoteCertificateValidationCallback = static (_, _, _, _) => true,
                };
            }

            return config;
        }
    }
}