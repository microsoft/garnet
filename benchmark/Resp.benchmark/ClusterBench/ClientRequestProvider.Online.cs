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
            var replicaEndpoint = hasReplica ? new IPEndPoint(IPAddress.Parse(replicaAddress), replicaPort) : null;

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
                var batchSize = opts.BatchSize.First();
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
                var numCommands = isMCommand ? 1 : 1;  // MGET/MSET: 1 command, others: 1 command

                while (!done && sw.Elapsed < runTime)
                {
                    byte[] request;

                    if (isMCommand)
                    {
                        request = FormatMRequest(opts.Op, batchSize, rng, dbSizePerShard);
                    }
                    else
                    {
                        var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                        request = FormatRequest(opts.Op, key);
                    }

                    var opStart = Stopwatch.GetTimestamp();

                    // Route based on operation type
                    var useReplica = ShouldUseReplica(opts.Op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    fixed (byte* bufPtr = request)
                    {
                        client.Send(bufPtr, request.Length, numCommands);
                        _ = client.CompletePendingRequests();
                    }

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    _ = Interlocked.Increment(ref opsCompleted);

                    // Track per-endpoint metrics
                    if (useReplica && replicaClient != null)
                        _ = Interlocked.Increment(ref replicaOps);
                    else
                        _ = Interlocked.Increment(ref primaryOps);
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
                var itp = opts.IntraThreadParallelism;
                var batchSize = opts.BatchSize.First();
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var primaryCount = 0;
                    var replicaCount = 0;

                    for (var p = 0; p < itp; p++)
                    {
                        // Route based on operation type
                        var useReplica = ShouldUseReplica(opts.Op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                        if (isMCommand)
                        {
                            // Generate multiple keys for MGET/MSET
                            var args = new List<string>();
                            if (opts.Op == OpType.MGET)
                            {
                                args.Add("MGET");
                                for (var i = 0; i < batchSize; i++)
                                    args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                            }
                            else if (opts.Op == OpType.MSET)
                            {
                                args.Add("MSET");
                                for (var i = 0; i < batchSize; i++)
                                {
                                    args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                                    args.Add(GenerateValue());
                                }
                            }

                            client.Execute(args.ToArray());
                        }
                        else
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                            if (opts.Op == OpType.GET)
                                client.Execute("GET", key);
                            else if (opts.Op == OpType.SET)
                                client.Execute("SET", key, GenerateValue());
                            else
                                client.Execute("GET", key);
                        }

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

                    _ = Interlocked.Add(ref opsCompleted, itp);
                    _ = Interlocked.Add(ref primaryOps, primaryCount);
                    _ = Interlocked.Add(ref replicaOps, replicaCount);
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
                _ = primaryClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

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
                    _ = replicaClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var dbSizePerShard = opts.DbSize;
                var itp = opts.IntraThreadParallelism;
                var batchSize = opts.BatchSize.First();
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var tasks = new Task[itp];
                    var primaryCount = 0;
                    var replicaCount = 0;

                    for (var p = 0; p < itp; p++)
                    {
                        // Route based on operation type
                        var useReplica = ShouldUseReplica(opts.Op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                        if (isMCommand)
                        {
                            // Generate multiple keys for MGET/MSET
                            if (opts.Op == OpType.MGET)
                            {
                                var keys = new string[batchSize];
                                for (var i = 0; i < batchSize; i++)
                                    keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                                tasks[p] = client.StringGetAsync(keys);
                            }
                            else if (opts.Op == OpType.MSET)
                            {
                                var args = new List<string>();
                                for (var i = 0; i < batchSize; i++)
                                {
                                    args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                                    args.Add(GenerateValue());
                                }
                                tasks[p] = client.ExecuteForStringResultAsync("MSET", args.ToArray());
                            }
                        }
                        else
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));

                            if (opts.Op == OpType.GET)
                                tasks[p] = client.StringGetAsMemoryAsync(key);
                            else if (opts.Op == OpType.SET)
                                tasks[p] = client.StringSetAsync(key, GenerateValue());
                            else
                                tasks[p] = client.StringGetAsMemoryAsync(key);
                        }

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

                    _ = Interlocked.Add(ref opsCompleted, itp);
                    _ = Interlocked.Add(ref primaryOps, primaryCount);
                    _ = Interlocked.Add(ref replicaOps, replicaCount);
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