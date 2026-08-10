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

            // Online mode issues single-key commands whose in-flight count is driven by --itp (not --batchsize).
            // The op mix is driven by --op-workload / --op-percent. Multi-key batching (MGET/MSET) belongs to offline mode.
            if (opWorkload != null && opWorkload.Any(o => o is OpType.MGET or OpType.MSET))
                throw new NotSupportedException("MGET/MSET are not supported in --online cluster-bench mode; use single-key ops in --op-workload. In-flight parallelism is driven by --itp.");

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    RunOnlineLightClient(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClientSession:
                    if (opts.IntraThreadParallelism > 1)
                        RunOnlineGarnetClientSessionParallel(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    else
                        RunOnlineGarnetClientSession(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClient:
                    if (opts.IntraThreadParallelism > 1)
                        RunOnlineGarnetClientParallel(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    else
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
                (int)OpType.NONE, // Mixed workload: use the generic response-counting branch in OnResponse.
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
                    (int)OpType.NONE, // Mixed workload: use the generic response-counting branch in OnResponse.
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
                var itp = opts.IntraThreadParallelism;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var primaryCount = 0;
                    var replicaCount = 0;

                    // Issue itp single-key commands (pipelined), routing each independently, then drain.
                    for (var p = 0; p < itp; p++)
                    {
                        var op = SelectOpType();
                        var key = GenerateKey(dbSizePerShard);
                        var request = FormatRequest(op, key);

                        var useReplica = ShouldUseReplica(op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                        fixed (byte* bufPtr = request)
                        {
                            client.Send(bufPtr, request.Length, 1);
                        }

                        if (useReplica && replicaClient != null)
                            replicaCount++;
                        else
                            primaryCount++;
                    }

                    _ = primaryClient.CompletePendingRequests();
                    _ = replicaClient?.CompletePendingRequests();

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

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var op = SelectOpType();
                    var useReplica = ShouldUseReplica(op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;
                    var key = GenerateKey(dbSizePerShard);

                    switch (op)
                    {
                        case OpType.SET:
                            client.Execute("SET", key, GenerateValue());
                            break;
                        case OpType.INCR:
                            client.Execute("INCR", key);
                            break;
                        case OpType.DEL:
                            client.Execute("DEL", key);
                            break;
                        default:
                            client.Execute("GET", key);
                            break;
                    }

                    client.CompletePending();

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    _ = Interlocked.Increment(ref opsCompleted);
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

        private void RunOnlineGarnetClientSessionParallel(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var bufferSize = Math.Max(131072, opts.IntraThreadParallelism * opts.ValueLength);
            using var primaryClient = new GarnetClientSession(
                primaryEndpoint,
                new NetworkBufferSettings(bufferSize),
                tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            primaryClient.Connect();

            if (opts.Auth != null)
            {
                primaryClient.Execute("AUTH", opts.Auth);
                primaryClient.CompletePending();
            }

            GarnetClientSession replicaClient = null;
            if (replicaEndpoint != null)
            {
                replicaClient = new GarnetClientSession(
                    replicaEndpoint,
                    new NetworkBufferSettings(bufferSize),
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

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var primaryCount = 0;
                    var replicaCount = 0;

                    for (var p = 0; p < itp; p++)
                    {
                        var op = SelectOpType();
                        var useReplica = ShouldUseReplica(op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;
                        var key = GenerateKey(dbSizePerShard);

                        switch (op)
                        {
                            case OpType.SET:
                                client.ExecuteBatch(["SET", key, GenerateValue()]);
                                break;
                            case OpType.INCR:
                                client.ExecuteBatch(["INCR", key]);
                                break;
                            case OpType.DEL:
                                client.ExecuteBatch(["DEL", key]);
                                break;
                            default:
                                client.ExecuteBatch(["GET", key]);
                                break;
                        }

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

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var op = SelectOpType();
                    var useReplica = ShouldUseReplica(op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;
                    var key = GenerateKey(dbSizePerShard);

                    Task task = op switch
                    {
                        OpType.SET => client.StringSetAsync(key, GenerateValue()),
                        OpType.INCR => client.ExecuteForStringResultAsync("INCR", [key]),
                        OpType.DEL => client.ExecuteForStringResultAsync("DEL", [key]),
                        _ => client.StringGetAsMemoryAsync(key),
                    };

                    task.GetAwaiter().GetResult();

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    _ = Interlocked.Increment(ref opsCompleted);
                    if (useReplica && replicaClient != null)
                        _ = Interlocked.Increment(ref replicaOps);
                    else
                        _ = Interlocked.Increment(ref primaryOps);
                }
            }
            finally
            {
                primaryClient.Dispose();
                replicaClient?.Dispose();
            }
        }

        private void RunOnlineGarnetClientParallel(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var primaryClient = new GarnetClient(
                primaryEndpoint,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                recordLatency: opts.ClientHistogram);

            primaryClient.Connect();

            if (opts.Auth != null)
                _ = primaryClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

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

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var tasks = new Task[itp];
                    var primaryCount = 0;
                    var replicaCount = 0;

                    for (var p = 0; p < itp; p++)
                    {
                        var op = SelectOpType();
                        var useReplica = ShouldUseReplica(op);
                        var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;
                        var key = GenerateKey(dbSizePerShard);

                        tasks[p] = op switch
                        {
                            OpType.SET => client.StringSetAsync(key, GenerateValue()),
                            OpType.INCR => client.ExecuteForStringResultAsync("INCR", [key]),
                            OpType.DEL => client.ExecuteForStringResultAsync("DEL", [key]),
                            _ => client.StringGetAsMemoryAsync(key),
                        };

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