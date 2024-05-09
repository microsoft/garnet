// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using Garnet.server;
using StackExchange.Redis;

namespace MetricsMonitor
{
    public enum Metric : byte
    {
        LATENCY,
        INFO,
    }

    public class Configuration
    {
        public static ConfigurationOptions GetConfig(string address, int port = default, bool allowAdmin = false, bool useTLS = false, string tlsHost = null)
        {
            var commands = RespCommandsInfo.TryGetRespCommandNames(out var names)
                ? new HashSet<string>(names)
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