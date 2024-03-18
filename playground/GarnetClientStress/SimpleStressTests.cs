// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.client;

namespace GarnetClientStress
{
    public class SimpleStressTests
    {
        public static void RunPingDispose(Options opts)
        {
            var timeout = TimeSpan.FromSeconds(1);
            GarnetClient[] clients = new GarnetClient[opts.PartitionCount];
            var progress = new ProgressBar("RunPingDispose");
            for (int i = 0; i < clients.Length; i++)
            {
                clients[i] = new GarnetClient(
                    opts.Address,
                    opts.Port,
                    opts.EnableTLS ? StressTestUtils.GetTlsOptions("GarnetTest") : null,
                    timeoutMilliseconds: (int)timeout.TotalMilliseconds,
                    maxOutstandingTasks: opts.ClientMaxOutstandingTasks,
                    recordLatency: true);
                clients[i].Connect();
                var result = clients[i].PingAsync().GetAwaiter().GetResult();
                if (result != "PONG")
                    throw new Exception($"Unexpected response to PING > {result}");
                progress.Report((double)i / clients.Length);
                clients[i].Dispose();
            }
        }
    }
}