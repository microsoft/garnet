// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using Garnet.client;
using Garnet.common;

namespace Resp.benchmark
{
    public unsafe partial class ClientRequestProvider
    {
        // Connection state for worker pool mode
        private LightClient primaryLightClient;
        private LightClient replicaLightClient;
        private GarnetClientSession primaryGarnetSession;
        private GarnetClientSession replicaGarnetSession;
        private GarnetClient primaryGarnetClient;
        private GarnetClient replicaGarnetClient;
        private bool connectionsInitialized = false;

        // Batch counter for offline mode (cycles through pre-generated batches)
        private long batchCounter = -1;

        /// <summary>
        /// Initialize connections for worker pool mode.
        /// Called once before first operation execution.
        /// </summary>
        private void InitializeConnections()
        {
            if (connectionsInitialized)
                return;

            var primaryEndpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var replicaEndpoint = hasReplica ? new IPEndPoint(IPAddress.Parse(replicaAddress), replicaPort) : null;

            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    primaryLightClient = new LightClient(
                        primaryEndpoint,
                        (int)opts.Op,
                        onResponse,
                        1 << 17,
                        opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    primaryLightClient.Connect();
                    primaryLightClient.Authenticate(opts.Auth);

                    if (replicaEndpoint != null)
                    {
                        replicaLightClient = new LightClient(
                            replicaEndpoint,
                            (int)opts.Op,
                            onResponse,
                            1 << 17,
                            opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                        replicaLightClient.Connect();
                        replicaLightClient.Authenticate(opts.Auth);
                    }
                    break;

                case ClientType.GarnetClientSession:
                    primaryGarnetSession = new GarnetClientSession(
                        primaryEndpoint,
                        new(),
                        tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    primaryGarnetSession.Connect();
                    if (!string.IsNullOrEmpty(opts.Auth))
                    {
                        primaryGarnetSession.Execute("AUTH", opts.Auth);
                        primaryGarnetSession.CompletePending();
                    }

                    if (replicaEndpoint != null)
                    {
                        replicaGarnetSession = new GarnetClientSession(
                            replicaEndpoint,
                            new(),
                            tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                        replicaGarnetSession.Connect();
                        if (!string.IsNullOrEmpty(opts.Auth))
                        {
                            replicaGarnetSession.Execute("AUTH", opts.Auth);
                            replicaGarnetSession.CompletePending();
                        }
                    }
                    break;

                case ClientType.GarnetClient:
                    primaryGarnetClient = new GarnetClient(
                        primaryEndpoint,
                        opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                        recordLatency: opts.ClientHistogram);
                    primaryGarnetClient.Connect();
                    if (!string.IsNullOrEmpty(opts.Auth))
                        primaryGarnetClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

                    if (replicaEndpoint != null)
                    {
                        replicaGarnetClient = new GarnetClient(
                            replicaEndpoint,
                            opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                            recordLatency: opts.ClientHistogram);
                        replicaGarnetClient.Connect();
                        if (!string.IsNullOrEmpty(opts.Auth))
                            replicaGarnetClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();
                    }
                    break;

                default:
                    throw new NotSupportedException($"Client type {opts.Client} not supported in worker pool mode.");
            }

            connectionsInitialized = true;
        }

        /// <summary>
        /// Execute a single operation for worker pool mode.
        /// Worker calls this repeatedly, selecting providers randomly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteSingleOnlineOperation()
        {
            // Lazy initialization of connections
            if (!connectionsInitialized)
                InitializeConnections();

            var dbSizePerShard = opts.DbSize;
            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
            var opStart = Stopwatch.GetTimestamp();

            // Route based on operation type
            var useReplica = ShouldUseReplica(opts.Op);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    ExecuteSingleLightClient(key, useReplica);
                    break;
                case ClientType.GarnetClientSession:
                    ExecuteSingleGarnetSession(key, useReplica);
                    break;
                case ClientType.GarnetClient:
                    ExecuteSingleGarnetClient(key, useReplica);
                    break;
            }

            var elapsed = Stopwatch.GetTimestamp() - opStart;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            _ = Interlocked.Increment(ref opsCompleted);

            // Track per-endpoint metrics
            if (useReplica && hasReplica)
                _ = Interlocked.Increment(ref replicaOps);
            else
                _ = Interlocked.Increment(ref primaryOps);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteSingleLightClient(string key, bool useReplica)
        {
            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
            var request = FormatRequest(opts.Op, key);

            fixed (byte* bufPtr = request)
            {
                client.Send(bufPtr, request.Length, 1);
                client.CompletePendingRequests();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteSingleGarnetSession(string key, bool useReplica)
        {
            var session = (useReplica && replicaGarnetSession != null) ? replicaGarnetSession : primaryGarnetSession;

            if (opts.Op == OpType.GET)
                session.Execute("GET", key);
            else if (opts.Op == OpType.SET)
                session.Execute("SET", key, GenerateValue());
            else
                session.Execute("GET", key);

            session.CompletePending();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteSingleGarnetClient(string key, bool useReplica)
        {
            var client = (useReplica && replicaGarnetClient != null) ? replicaGarnetClient : primaryGarnetClient;

            if (opts.Op == OpType.GET)
                client.ExecuteForStringResultAsync("GET", [key]).GetAwaiter().GetResult();
            else if (opts.Op == OpType.SET)
                client.ExecuteForStringResultAsync("SET", [key, GenerateValue()]).GetAwaiter().GetResult();
            else
                client.ExecuteForStringResultAsync("GET", [key]).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Execute a single batch of pre-generated operations (offline mode).
        /// Routes the entire batch to either primary or replica based on operation type.
        /// In offline mode, all operations in a batch are of the same type.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteSingleOfflineBatch()
        {
            if (requestBuffers == null)
                throw new InvalidOperationException("Must call PrepareBuffers() before ExecuteSingleOfflineBatch()");

            // Ensure connections are initialized
            if (primaryLightClient == null)
                InitializeConnections();

            // Select batch index (cycle through pre-generated batches)
            var batchIdx = (int)(Interlocked.Increment(ref batchCounter) % batchCount);
            var buffer = requestBuffers[batchIdx];
            var len = requestLengths[batchIdx];
            var batchSize = opts.BatchSize.First();

            // Start timing
            var opStart = Stopwatch.GetTimestamp();

            // Route entire batch based on operation type
            // For offline mode, we route per-batch (all ops in batch are same type)
            var useReplica = ShouldUseReplica(opts.Op);
            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;

            // Execute batch
            unsafe
            {
                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, len, batchSize);
                    client.CompletePendingRequests();
                }
            }

            // Record latency
            var elapsed = Stopwatch.GetTimestamp() - opStart;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            // Update counters
            Interlocked.Add(ref opsCompleted, batchSize);
            Interlocked.Add(ref bytesSent, len);

            // Track per-endpoint metrics
            if (useReplica && replicaLightClient != null)
                Interlocked.Add(ref replicaOps, batchSize);
            else
                Interlocked.Add(ref primaryOps, batchSize);
        }

        /// <summary>
        /// Dispose connections created in worker pool mode.
        /// </summary>
        private void DisposeWorkerPoolConnections()
        {
            primaryLightClient?.Dispose();
            replicaLightClient?.Dispose();
            primaryGarnetSession?.Dispose();
            replicaGarnetSession?.Dispose();
            primaryGarnetClient?.Dispose();
            replicaGarnetClient?.Dispose();
        }
    }
}