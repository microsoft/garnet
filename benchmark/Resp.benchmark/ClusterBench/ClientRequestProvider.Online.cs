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
            var primaryEndpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            IPEndPoint replicaEndpoint = hasReplica ? new IPEndPoint(IPAddress.Parse(replicaAddress), replicaPort) : null;

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    RunOnlineLightClient(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClientSession:
                    RunOnlineGarnetClientSession(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClient:
                    RunOnlineGarnetClient(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                default:
                    throw new NotSupportedException($"Client type {opts.Client} not supported in cluster bench mode.");
            }
        }

        private void RunOnlineLightClient(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var primaryClient = new LightClient(
                primaryEndpoint,
                (int)opts.Op,
                onResponse,
                1 << 17, // Buffer size in bytes
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            primaryClient.Connect();
            primaryClient.Authenticate(opts.Auth);

            // Create replica client if assigned
            LightClient replicaClient = null;
            if (replicaEndpoint != null)
            {
                replicaClient = new LightClient(
                    replicaEndpoint,
                    (int)opts.Op,
                    onResponse,
                    1 << 17,
                    opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

                replicaClient.Connect();
                replicaClient.Authenticate(opts.Auth);
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var dbSizePerShard = opts.DbSize;

                while (!done && sw.Elapsed < runTime)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    var request = FormatRequest(opts.Op, key);
                    var opStart = Stopwatch.GetTimestamp();

                    // Route based on operation type
                    var useReplica = ShouldUseReplica(opts.Op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    fixed (byte* bufPtr = request)
                    {
                        client.Send(bufPtr, request.Length, 1);
                        client.CompletePendingRequests();
                    }

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Increment(ref opsCompleted);

                    // Track per-endpoint metrics
                    if (useReplica && replicaClient != null)
                        Interlocked.Increment(ref replicaOps);
                    else
                        Interlocked.Increment(ref primaryOps);
                }
            }
            finally
            {
                replicaClient?.Dispose();
            }
        }

        private void RunOnlineGarnetClientSession(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            using var primaryClient = new GarnetClientSession(
                primaryEndpoint,
                new(),
                tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            primaryClient.Connect();

            if (opts.Auth != null)
            {
                primaryClient.Execute("AUTH", opts.Auth);
                primaryClient.CompletePending();
            }

            // Create replica client if assigned
            GarnetClientSession replicaClient = null;
            if (replicaEndpoint != null)
            {
                replicaClient = new GarnetClientSession(
                    replicaEndpoint,
                    new(),
                    tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

                replicaClient.Connect();

                if (opts.Auth != null)
                {
                    replicaClient.Execute("AUTH", opts.Auth);
                    replicaClient.CompletePending();
                }
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var dbSizePerShard = opts.DbSize;
                int itp = opts.IntraThreadParallelism;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    int primaryCount = 0;
                    int replicaCount = 0;

                    for (int p = 0; p < itp; p++)
                    {
                        var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                        // Route based on operation type
                        var useReplica = ShouldUseReplica(opts.Op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                        if (opts.Op == OpType.GET)
                            client.Execute("GET", key);
                        else if (opts.Op == OpType.SET)
                            client.Execute("SET", key, GenerateValue());
                        else
                            client.Execute("GET", key);

                        // Track routing
                        if (useReplica && replicaClient != null)
                            replicaCount++;
                        else
                            primaryCount++;
                    }

                    primaryClient.CompletePending();
                    replicaClient?.CompletePending();

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, itp);
                    Interlocked.Add(ref primaryOps, primaryCount);
                    Interlocked.Add(ref replicaOps, replicaCount);
                }
            }
            finally
            {
                replicaClient?.Dispose();
            }
        }

        private void RunOnlineGarnetClient(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var primaryClient = new GarnetClient(
                primaryEndpoint,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                recordLatency: opts.ClientHistogram);

            primaryClient.Connect();

            if (opts.Auth != null)
                primaryClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

            // Create replica client if assigned
            GarnetClient replicaClient = null;
            if (replicaEndpoint != null)
            {
                replicaClient = new GarnetClient(
                    replicaEndpoint,
                    opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                    recordLatency: opts.ClientHistogram);

                replicaClient.Connect();

                if (opts.Auth != null)
                    replicaClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var dbSizePerShard = opts.DbSize;
                int itp = opts.IntraThreadParallelism;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var tasks = new Task[itp];
                    int primaryCount = 0;
                    int replicaCount = 0;

                    for (int p = 0; p < itp; p++)
                    {
                        var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                        // Route based on operation type
                        var useReplica = ShouldUseReplica(opts.Op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                        if (opts.Op == OpType.GET)
                            tasks[p] = client.StringGetAsMemoryAsync(key);
                        else if (opts.Op == OpType.SET)
                            tasks[p] = client.StringSetAsync(key, GenerateValue());
                        else
                            tasks[p] = client.StringGetAsMemoryAsync(key);

                        // Track routing
                        if (useReplica && replicaClient != null)
                            replicaCount++;
                        else
                            primaryCount++;
                    }

                    Task.WaitAll(tasks);

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, itp);
                    Interlocked.Add(ref primaryOps, primaryCount);
                    Interlocked.Add(ref replicaOps, replicaCount);
                }
            }
            finally
            {
                primaryClient.Dispose();
                replicaClient?.Dispose();
            }
        }
    }
}