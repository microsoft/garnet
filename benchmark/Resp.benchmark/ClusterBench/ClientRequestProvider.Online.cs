// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using Garnet.client;
using Garnet.common;

namespace Resp.benchmark
{
    public unsafe partial class ClientRequestProvider
    {
        /// <summary>
        /// Run online benchmark: generate and send requests on-the-fly.
        /// </summary>
        public void RunOnline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var endpoint = new IPEndPoint(IPAddress.Parse(shard.Address), shard.Port);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    RunOnlineLightClient(endpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClientSession:
                    RunOnlineGarnetClientSession(endpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClient:
                    RunOnlineGarnetClient(endpoint, startSignal, runTime);
                    break;
                default:
                    throw new NotSupportedException($"Client type {opts.Client} not supported in cluster bench mode.");
            }
        }

        private void RunOnlineLightClient(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)opts.Op,
                onResponse,
                1 << 17, // Buffer size in bytes
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            var dbSizePerShard = opts.DbSize;

            while (!done && sw.Elapsed < runTime)
            {
                var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                var request = FormatRequest(opts.Op, key);

                var opStart = Stopwatch.GetTimestamp();

                fixed (byte* bufPtr = request)
                {
                    client.Send(bufPtr, request.Length, 1);
                    client.CompletePendingRequests();
                }

                var elapsed = Stopwatch.GetTimestamp() - opStart;

                if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                    histogram.RecordValue(elapsed);

                Interlocked.Increment(ref opsCompleted);
            }
        }

        private void RunOnlineGarnetClientSession(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            using var client = new GarnetClientSession(
                endpoint,
                new(),
                tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();

            if (opts.Auth != null)
            {
                client.Execute("AUTH", opts.Auth);
                client.CompletePending();
            }

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            var dbSizePerShard = opts.DbSize;
            int itp = opts.IntraThreadParallelism;

            while (!done && sw.Elapsed < runTime)
            {
                var opStart = Stopwatch.GetTimestamp();

                for (int p = 0; p < itp; p++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                    if (opts.Op == OpType.GET)
                        client.Execute("GET", key);
                    else if (opts.Op == OpType.SET)
                        client.Execute("SET", key, GenerateValue());
                    else
                        client.Execute("GET", key);
                }

                client.CompletePending();

                var elapsed = Stopwatch.GetTimestamp() - opStart;

                if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                    histogram.RecordValue(elapsed);

                Interlocked.Add(ref opsCompleted, itp);
            }
        }

        private void RunOnlineGarnetClient(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var client = new GarnetClient(
                endpoint,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                recordLatency: opts.ClientHistogram);

            client.Connect();

            if (opts.Auth != null)
                client.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            var dbSizePerShard = opts.DbSize;
            int itp = opts.IntraThreadParallelism;

            try
            {
                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var tasks = new Task[itp];

                    for (int p = 0; p < itp; p++)
                    {
                        var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                        if (opts.Op == OpType.GET)
                            tasks[p] = client.StringGetAsMemoryAsync(key);
                        else if (opts.Op == OpType.SET)
                            tasks[p] = client.StringSetAsync(key, GenerateValue());
                        else
                            tasks[p] = client.StringGetAsMemoryAsync(key);
                    }

                    Task.WaitAll(tasks);

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, itp);
                }
            }
            finally
            {
                client.Dispose();
            }
        }
    }
}
