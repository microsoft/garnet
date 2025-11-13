// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Resp.benchmark
{
    class PeriodicCheckpointer
    {
        readonly int periodMs;
        readonly ILogger logger;
        readonly CancellationTokenSource cts = new();

        public PeriodicCheckpointer(int periodMs, ILogger logger = null)
        {
            this.periodMs = periodMs;
            this.logger = logger;
        }

        public void Start(string address, int port)
        {
            using var redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(address, port, allowAdmin: true));
            var server = redis.GetServer(address, port);
            while (true)
            {
                if (cts.IsCancellationRequested)
                    break;
                Thread.Sleep(periodMs);
                try
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    server.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618 // Type or member is obsolete
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Save threw exception");
                }
            }
        }

        public void Stop()
        {
            cts.Cancel();
        }
    }
}