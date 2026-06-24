// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using Garnet.client;
using Garnet.common;
using StackExchange.Redis;

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
        private ConnectionMultiplexer primarySERedis;
        private ConnectionMultiplexer replicaSERedis;
        private IDatabase primarySERedisDb;
        private IDatabase replicaSERedisDb;
        private bool connectionsInitialized = false;

        // Batch counter for offline mode (cycles through pre-generated batches)
        private long batchCounter = -1;

        // Pipeline state: tracks pending request metadata per provider
        private long pendingStartTimestamp;
        private int pendingBatchSize;
        private long pendingBytesSent;
        private bool pendingUsedReplica;
        private Task pendingGarnetClientTask;

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

            // Size buffer to fit the largest pre-generated request batch
            var bufferSize = 1 << 17; // 128KB default
            if (requestLengths != null)
            {
                var maxLen = requestLengths.Max();
                if (maxLen > bufferSize)
                    bufferSize = (int)BitOperations.RoundUpToPowerOf2((uint)maxLen);
            }

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    primaryLightClient = new LightClient(
                        primaryEndpoint,
                        (int)opts.Op,
                        onResponse,
                        bufferSize,
                        opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    primaryLightClient.Connect();
                    primaryLightClient.Authenticate(opts.Auth);

                    if (replicaEndpoint != null)
                    {
                        replicaLightClient = new LightClient(
                            replicaEndpoint,
                            (int)opts.Op,
                            onResponse,
                            bufferSize,
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

                case ClientType.SERedis:
                    var primaryConfig = BenchUtils.GetConfig(primaryAddress, primaryPort, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost);
                    if (!string.IsNullOrEmpty(opts.Auth))
                        primaryConfig.Password = opts.Auth;
                    primarySERedis = ConnectionMultiplexer.Connect(primaryConfig);
                    primarySERedisDb = primarySERedis.GetDatabase(0);

                    if (replicaEndpoint != null)
                    {
                        var replicaConfig = BenchUtils.GetConfig(replicaAddress, replicaPort, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost);
                        if (!string.IsNullOrEmpty(opts.Auth))
                            replicaConfig.Password = opts.Auth;
                        replicaSERedis = ConnectionMultiplexer.Connect(replicaConfig);
                        replicaSERedisDb = replicaSERedis.GetDatabase(0);
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
        private void SendSingleLightClient(string key, bool useReplica)
        {
            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
            var request = FormatRequest(opts.Op, key);

            fixed (byte* bufPtr = request)
            {
                client.Send(bufPtr, request.Length, 1);
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
        private void SendSingleGarnetSession(string key, bool useReplica)
        {
            var session = (useReplica && replicaGarnetSession != null) ? replicaGarnetSession : primaryGarnetSession;

            if (opts.Op == OpType.GET)
                session.Execute("GET", key);
            else if (opts.Op == OpType.SET)
                session.Execute("SET", key, GenerateValue());
            else
                session.Execute("GET", key);
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
            if (requestBuffers == null && opts.Client == ClientType.LightClient)
                throw new InvalidOperationException("Must call PrepareBuffers() before ExecuteSingleOfflineBatch()");

            // Ensure connections are initialized
            if (!connectionsInitialized)
                InitializeConnections();

            var opStart = Stopwatch.GetTimestamp();
            var useReplica = ShouldUseReplica(opts.Op);
            var batchSize = opts.BatchSize.First();
            var dbSizePerShard = opts.DbSize;
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
            var numCommands = isMCommand ? 1 : batchSize;

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    ExecuteOfflineBatchLightClient(useReplica, batchSize, numCommands);
                    break;
                case ClientType.GarnetClientSession:
                    ExecuteOfflineBatchGarnetSession(useReplica, batchSize, dbSizePerShard, isMCommand);
                    break;
                case ClientType.GarnetClient:
                    ExecuteOfflineBatchGarnetClient(useReplica, batchSize, dbSizePerShard, isMCommand);
                    break;
                case ClientType.SERedis:
                    ExecuteOfflineBatchSERedis(useReplica, batchSize, dbSizePerShard, isMCommand);
                    break;
            }

            // Record latency
            var elapsed = Stopwatch.GetTimestamp() - opStart;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            // Update counters
            Interlocked.Add(ref opsCompleted, numCommands);

            // Track per-endpoint metrics
            if (useReplica && hasReplica)
                Interlocked.Add(ref replicaOps, numCommands);
            else
                Interlocked.Add(ref primaryOps, numCommands);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteOfflineBatchLightClient(bool useReplica, int batchSize, int numCommands)
        {
            // Select batch index (cycle through pre-generated batches)
            var batchIdx = (int)(Interlocked.Increment(ref batchCounter) % batchCount);
            var buffer = requestBuffers[batchIdx];
            var len = requestLengths[batchIdx];

            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;

            unsafe
            {
                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, len, numCommands);
                    client.CompletePendingRequests();
                }
            }

            var bytesRcvd = client.TotalBytesReceived;
            Interlocked.Exchange(ref bytesReceived, bytesRcvd);
            Interlocked.Add(ref bytesSent, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteOfflineBatchGarnetSession(bool useReplica, int batchSize, int dbSizePerShard, bool isMCommand)
        {
            var session = (useReplica && replicaGarnetSession != null) ? replicaGarnetSession : primaryGarnetSession;

            if (isMCommand)
            {
                // Execute MGET/MSET with batchSize keys
                var args = new List<string>();
                if (opts.Op == OpType.MGET)
                {
                    args.Add("MGET");
                    for (int i = 0; i < batchSize; i++)
                        args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                }
                else if (opts.Op == OpType.MSET)
                {
                    args.Add("MSET");
                    for (int i = 0; i < batchSize; i++)
                    {
                        args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                        args.Add(GenerateValue());
                    }
                }
                session.Execute(args.ToArray());
            }
            else
            {
                // Execute batch of individual commands
                for (int i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    if (opts.Op == OpType.GET)
                        session.Execute("GET", key);
                    else if (opts.Op == OpType.SET)
                        session.Execute("SET", key, GenerateValue());
                    else
                        session.Execute("GET", key);
                }
            }

            session.CompletePending();

            // Track bytes (approximate RESP protocol overhead)
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

            Interlocked.Add(ref bytesSent, sentBytes);
            Interlocked.Add(ref bytesReceived, rcvdBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteOfflineBatchGarnetClient(bool useReplica, int batchSize, int dbSizePerShard, bool isMCommand)
        {
            var client = (useReplica && replicaGarnetClient != null) ? replicaGarnetClient : primaryGarnetClient;

            if (isMCommand)
            {
                // Execute MGET/MSET with batchSize keys
                if (opts.Op == OpType.MGET)
                {
                    var keys = new string[batchSize];
                    for (int i = 0; i < batchSize; i++)
                        keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    client.StringGetAsync(keys).GetAwaiter().GetResult();
                }
                else if (opts.Op == OpType.MSET)
                {
                    var args = new List<string>();
                    for (int i = 0; i < batchSize; i++)
                    {
                        args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                        args.Add(GenerateValue());
                    }
                    client.ExecuteForStringResultAsync("MSET", args.ToArray()).GetAwaiter().GetResult();
                }
            }
            else
            {
                // Execute batch of individual commands
                var tasks = new Task[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    if (opts.Op == OpType.GET)
                        tasks[i] = client.StringGetAsMemoryAsync(key);
                    else if (opts.Op == OpType.SET)
                        tasks[i] = client.StringSetAsync(key, GenerateValue());
                    else
                        tasks[i] = client.StringGetAsMemoryAsync(key);
                }
                Task.WaitAll(tasks);
            }

            // Track bytes (approximate RESP protocol overhead)
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

            Interlocked.Add(ref bytesSent, sentBytes);
            Interlocked.Add(ref bytesReceived, rcvdBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteOfflineBatchSERedis(bool useReplica, int batchSize, int dbSizePerShard, bool isMCommand)
        {
            var db = (useReplica && replicaSERedisDb != null) ? replicaSERedisDb : primarySERedisDb;

            if (isMCommand)
            {
                // Execute MGET/MSET with batchSize keys
                if (opts.Op == OpType.MGET)
                {
                    var keys = new RedisKey[batchSize];
                    for (int i = 0; i < batchSize; i++)
                        keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    db.StringGet(keys);
                }
                else if (opts.Op == OpType.MSET)
                {
                    var pairs = new KeyValuePair<RedisKey, RedisValue>[batchSize];
                    for (int i = 0; i < batchSize; i++)
                    {
                        var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                        var value = GenerateValue();
                        pairs[i] = new KeyValuePair<RedisKey, RedisValue>(key, value);
                    }
                    db.StringSet(pairs);
                }
            }
            else
            {
                // Execute batch of individual commands
                for (int i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    if (opts.Op == OpType.GET)
                        db.StringGet(key);
                    else if (opts.Op == OpType.SET)
                        db.StringSet(key, GenerateValue());
                    else
                        db.StringGet(key);
                }
            }

            // Track bytes (approximate RESP protocol overhead)
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

            Interlocked.Add(ref bytesSent, sentBytes);
            Interlocked.Add(ref bytesReceived, rcvdBytes);
        }

        /// <summary>
        /// Send-only: issue a single offline batch without waiting for the response.
        /// Called during the broadcast send phase of pipeline mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SendSingleOfflineBatch()
        {
            if (requestBuffers == null)
                throw new InvalidOperationException("Must call PrepareBuffers() before SendSingleOfflineBatch()");

            if (primaryLightClient == null)
                InitializeConnections();

            // Select batch
            var batchIdx = (int)(Interlocked.Increment(ref batchCounter) % batchCount);
            var buffer = requestBuffers[batchIdx];
            var len = requestLengths[batchIdx];
            var numCommands = requestCounts[batchIdx];  // 1 for MGET/MSET, batchSize for others

            // Route batch
            var useReplica = ShouldUseReplica(opts.Op);
            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;

            // Send without waiting for response
            unsafe
            {
                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, len, numCommands);
                }
            }

            // Record pending state for completion phase
            pendingStartTimestamp = Stopwatch.GetTimestamp();
            pendingBatchSize = numCommands;
            pendingBytesSent = len;
            pendingUsedReplica = useReplica;
        }

        /// <summary>
        /// Complete pending for a previously sent offline batch and record metrics.
        /// Called during the broadcast complete phase of pipeline mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompletePendingAndRecordOfflineMetrics()
        {
            // Complete pending on the client that was used
            var client = (pendingUsedReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
            client.CompletePendingRequests();
            var completedTimestamp = Stopwatch.GetTimestamp();

            // Record latency
            var elapsed = completedTimestamp - pendingStartTimestamp;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            // Update counters
            Interlocked.Add(ref opsCompleted, pendingBatchSize);
            Interlocked.Add(ref bytesSent, pendingBytesSent);

            if (pendingUsedReplica && hasReplica)
                Interlocked.Add(ref replicaOps, pendingBatchSize);
            else
                Interlocked.Add(ref primaryOps, pendingBatchSize);
        }

        /// <summary>
        /// Send-only: issue a single online operation without waiting for the response.
        /// Called during the broadcast send phase of pipeline mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SendSingleOnlineOperation()
        {
            if (!connectionsInitialized)
                InitializeConnections();

            var dbSizePerShard = opts.DbSize;
            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
            var useReplica = ShouldUseReplica(opts.Op);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    SendSingleLightClient(key, useReplica);
                    break;
                case ClientType.GarnetClientSession:
                    SendSingleGarnetSession(key, useReplica);
                    break;
                case ClientType.GarnetClient:
                    // GarnetClient is async-based; send without awaiting
                    SendSingleGarnetClient(key, useReplica);
                    break;
            }

            // Record pending state for completion phase
            pendingStartTimestamp = Stopwatch.GetTimestamp();
            pendingBatchSize = 1;
            pendingBytesSent = 0;
            pendingUsedReplica = useReplica;
        }

        /// <summary>
        /// Complete pending for a previously sent online operation and record metrics.
        /// Called during the broadcast complete phase of pipeline mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompletePendingAndRecordOnlineMetrics()
        {
            long completedTimestamp;
            switch (opts.Client)
            {
                case ClientType.LightClient:
                    var lc = (pendingUsedReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
                    lc.CompletePendingRequests();
                    completedTimestamp = Stopwatch.GetTimestamp();
                    break;
                case ClientType.GarnetClientSession:
                    var gs = (pendingUsedReplica && replicaGarnetSession != null) ? replicaGarnetSession : primaryGarnetSession;
                    gs.CompletePending();
                    completedTimestamp = Stopwatch.GetTimestamp();
                    break;
                case ClientType.GarnetClient:
                    CompleteSingleGarnetClient(pendingUsedReplica);
                    completedTimestamp = Stopwatch.GetTimestamp();
                    break;
                default:
                    completedTimestamp = Stopwatch.GetTimestamp();
                    break;
            }

            // Record latency
            var elapsed = completedTimestamp - pendingStartTimestamp;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            // Update counters
            _ = Interlocked.Increment(ref opsCompleted);

            if (pendingUsedReplica && hasReplica)
                _ = Interlocked.Increment(ref replicaOps);
            else
                _ = Interlocked.Increment(ref primaryOps);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendSingleGarnetClient(string key, bool useReplica)
        {
            var client = (useReplica && replicaGarnetClient != null) ? replicaGarnetClient : primaryGarnetClient;

            if (opts.Op == OpType.GET)
                pendingGarnetClientTask = client.StringGetAsMemoryAsync(key);
            else if (opts.Op == OpType.SET)
                pendingGarnetClientTask = client.StringSetAsync(key, GenerateValue());
            else
                pendingGarnetClientTask = client.StringGetAsMemoryAsync(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteSingleGarnetClient(bool useReplica)
        {
            pendingGarnetClientTask?.GetAwaiter().GetResult();
            pendingGarnetClientTask = null;
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
            primarySERedis?.Dispose();
            replicaSERedis?.Dispose();
        }
    }
}