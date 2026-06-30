// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
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
        private long pendingBytesReceived;  // For non-LightClient tracking
        private bool pendingUsedReplica;
        private Task pendingGarnetClientTask;
        private Task[] pendingGarnetClientTasks;  // For batched async operations
        private Task[] pendingSERedisTasks;  // For SERedis async operations

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
            var bufferSize = offlineBufferSize > 0 ? offlineBufferSize : (1 << 17);

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
                        // For mixed workload (write op + replicas), replica receives read responses
                        var replicaOpType = workload.ReplicaRequests != null
                            ? (int)GetReadOperationType()
                            : (int)opts.Op;

                        replicaLightClient = new LightClient(
                            replicaEndpoint,
                            replicaOpType,
                            onResponse,
                            bufferSize,
                            opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                        replicaLightClient.Connect();
                        replicaLightClient.Authenticate(opts.Auth);
                        replicaLightClient.ReadOnly();
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
                        replicaGarnetSession.Execute("READONLY");
                        replicaGarnetSession.CompletePending();
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
                        replicaGarnetClient.ExecuteForStringResultAsync("READONLY").GetAwaiter().GetResult();
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
            if (workload.PrimaryRequests == null && opts.Client == ClientType.LightClient)
                throw new InvalidOperationException("Must call PrepareBuffers() before ExecuteSingleOfflineBatch()");

            // Ensure connections are initialized
            if (!connectionsInitialized)
                InitializeConnections();

            var opStart = Stopwatch.GetTimestamp();
            var batchSize = opts.BatchSize.First();
            var dbSizePerShard = opts.DbSize;
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
            var numCommands = isMCommand ? 1 : batchSize;

            // Determine routing: for mixed workload (write op + replicas), use pre-computed routing
            var batchIdx = (int)(Interlocked.Increment(ref batchCounter) % batchCount);
            var useReplica = workload.ReplicaRequests != null && workload.ReadUseReplica[batchIdx];

            // For non-mixed workload, fall back to standard routing
            if (workload.ReplicaRequests == null)
                useReplica = ShouldUseReplica(opts.Op);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    ExecuteOfflineBatchLightClient(useReplica, batchIdx);
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

            // Get the actual command count from the request that was sent
            var request = useReplica ? workload.ReplicaRequests[batchIdx] : workload.PrimaryRequests[batchIdx];
            Interlocked.Add(ref opsCompleted, request.CommandCount);

            // Track per-endpoint metrics
            if (useReplica && hasReplica)
                Interlocked.Add(ref replicaOps, request.CommandCount);
            else
                Interlocked.Add(ref primaryOps, request.CommandCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteOfflineBatchLightClient(bool useReplica, int batchIdx)
        {
            // Select the appropriate request based on routing decision
            ref var request = ref (useReplica && workload.ReplicaRequests != null
                ? ref workload.ReplicaRequests[batchIdx]
                : ref workload.PrimaryRequests[batchIdx]);

            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;

            unsafe
            {
                fixed (byte* bufPtr = request.RespData)
                {
                    client.Send(bufPtr, request.ByteCount, request.CommandCount);
                    client.CompletePendingRequests();
                }
            }

            var bytesRcvd = client.TotalBytesReceived;
            Interlocked.Exchange(ref bytesReceived, bytesRcvd);
            Interlocked.Add(ref bytesSent, request.ByteCount);
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
            if (workload.PrimaryRequests == null && opts.Client == ClientType.LightClient)
                throw new InvalidOperationException("Must call PrepareBuffers() before SendSingleOfflineBatch()");

            if (!connectionsInitialized)
                InitializeConnections();

            var batchSize = opts.BatchSize.First();
            var dbSizePerShard = opts.DbSize;
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

            // Determine routing: for mixed workload, use pre-computed routing
            var batchIdx = (int)(Interlocked.Increment(ref batchCounter) % batchCount);
            var useReplica = workload.ReplicaRequests != null && workload.ReadUseReplica[batchIdx];

            if (workload.ReplicaRequests == null)
                useReplica = ShouldUseReplica(opts.Op);

            var request = useReplica ? workload.ReplicaRequests[batchIdx] : workload.PrimaryRequests[batchIdx];

            // Record start timestamp for all clients
            pendingStartTimestamp = Stopwatch.GetTimestamp();
            pendingBatchSize = request.CommandCount;
            pendingUsedReplica = useReplica;

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    SendOfflineBatchLightClient(ref request, useReplica);
                    break;
                case ClientType.GarnetClientSession:
                    SendOfflineBatchGarnetSession(batchSize, dbSizePerShard, isMCommand, request.CommandCount, useReplica);
                    break;
                case ClientType.GarnetClient:
                    SendOfflineBatchGarnetClient(batchSize, dbSizePerShard, isMCommand, useReplica);
                    break;
                case ClientType.SERedis:
                    SendOfflineBatchSERedis(batchSize, dbSizePerShard, isMCommand, useReplica);
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendOfflineBatchLightClient(ref Request request, bool useReplica)
        {
            var client = (useReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;

            // Send without waiting for response
            unsafe
            {
                fixed (byte* bufPtr = request.RespData)
                {
                    client.Send(bufPtr, request.ByteCount, request.CommandCount);
                }
            }

            pendingBytesSent = request.ByteCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendOfflineBatchGarnetSession(int batchSize, int dbSizePerShard, bool isMCommand, int numCommands, bool useReplica)
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
                session.Execute(args.ToArray());  // Queue command, don't wait
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

            // Track bytes for completion phase
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;

            pendingBytesSent = sentBytes;
            pendingBytesReceived = rcvdBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendOfflineBatchGarnetClient(int batchSize, int dbSizePerShard, bool isMCommand, bool useReplica)
        {
            var client = (useReplica && replicaGarnetClient != null) ? replicaGarnetClient : primaryGarnetClient;

            if (isMCommand)
            {
                // Execute MGET/MSET with batchSize keys - start task but don't await
                if (opts.Op == OpType.MGET)
                {
                    var keys = new string[batchSize];
                    for (int i = 0; i < batchSize; i++)
                        keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    pendingGarnetClientTask = client.StringGetAsync(keys);
                }
                else if (opts.Op == OpType.MSET)
                {
                    var args = new List<string>();
                    for (int i = 0; i < batchSize; i++)
                    {
                        args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                        args.Add(GenerateValue());
                    }
                    pendingGarnetClientTask = client.ExecuteForStringResultAsync("MSET", args.ToArray());
                }
            }
            else
            {
                // Execute batch of individual commands - start all tasks
                pendingGarnetClientTasks = new Task[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    if (opts.Op == OpType.GET)
                        pendingGarnetClientTasks[i] = client.StringGetAsMemoryAsync(key);
                    else if (opts.Op == OpType.SET)
                        pendingGarnetClientTasks[i] = client.StringSetAsync(key, GenerateValue());
                    else
                        pendingGarnetClientTasks[i] = client.StringGetAsMemoryAsync(key);
                }
            }

            // Track bytes for completion phase
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;

            pendingBytesSent = sentBytes;
            pendingBytesReceived = rcvdBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendOfflineBatchSERedis(int batchSize, int dbSizePerShard, bool isMCommand, bool useReplica)
        {
            var db = (useReplica && replicaSERedisDb != null) ? replicaSERedisDb : primarySERedisDb;

            if (isMCommand)
            {
                // Execute MGET/MSET with batchSize keys - start task but don't await
                if (opts.Op == OpType.MGET)
                {
                    var keys = new RedisKey[batchSize];
                    for (int i = 0; i < batchSize; i++)
                        keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    pendingGarnetClientTask = db.StringGetAsync(keys);
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
                    pendingGarnetClientTask = db.StringSetAsync(pairs);
                }
            }
            else
            {
                // Execute batch of individual commands - start all tasks
                pendingSERedisTasks = new Task[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                    if (opts.Op == OpType.GET)
                        pendingSERedisTasks[i] = db.StringGetAsync(key);
                    else if (opts.Op == OpType.SET)
                        pendingSERedisTasks[i] = db.StringSetAsync(key, GenerateValue());
                    else
                        pendingSERedisTasks[i] = db.StringGetAsync(key);
                }
            }

            // Track bytes for completion phase
            var sentBytes = isMCommand
                ? CalculateRespSentBytes(opts.Op, batchSize)
                : CalculateRespSentBytes(opts.Op, 1) * batchSize;
            var rcvdBytes = isMCommand
                ? CalculateRespReceivedBytes(opts.Op, batchSize)
                : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;

            pendingBytesSent = sentBytes;
            pendingBytesReceived = rcvdBytes;
        }

        /// <summary>
        /// Complete pending for a previously sent offline batch and record metrics.
        /// Called during the broadcast complete phase of pipeline mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompletePendingAndRecordOfflineMetrics()
        {
            switch (opts.Client)
            {
                case ClientType.LightClient:
                    CompletePendingLightClient();
                    break;
                case ClientType.GarnetClientSession:
                    CompletePendingGarnetSession();
                    break;
                case ClientType.GarnetClient:
                    CompletePendingGarnetClient();
                    break;
                case ClientType.SERedis:
                    CompletePendingSERedis();
                    break;
            }

            // Record latency (common for all clients)
            var completedTimestamp = Stopwatch.GetTimestamp();
            var elapsed = completedTimestamp - pendingStartTimestamp;
            if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                histogram.RecordValue(elapsed);

            // Update counters
            Interlocked.Add(ref opsCompleted, pendingBatchSize);
            Interlocked.Add(ref bytesSent, pendingBytesSent);

            // LightClient uses TotalBytesReceived, others track via calculation
            if (opts.Client == ClientType.LightClient)
            {
                var client = (pendingUsedReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
                var bytesRcvd = client.TotalBytesReceived;
                Interlocked.Exchange(ref bytesReceived, bytesRcvd);
            }
            else
            {
                Interlocked.Add(ref bytesReceived, pendingBytesReceived);
            }

            if (pendingUsedReplica && hasReplica)
                Interlocked.Add(ref replicaOps, pendingBatchSize);
            else
                Interlocked.Add(ref primaryOps, pendingBatchSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompletePendingLightClient()
        {
            var client = (pendingUsedReplica && replicaLightClient != null) ? replicaLightClient : primaryLightClient;
            client.CompletePendingRequests();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompletePendingGarnetSession()
        {
            var session = (pendingUsedReplica && replicaGarnetSession != null) ? replicaGarnetSession : primaryGarnetSession;
            session.CompletePending();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompletePendingGarnetClient()
        {
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

            if (isMCommand)
            {
                // Wait for the single MGET/MSET task
                pendingGarnetClientTask.GetAwaiter().GetResult();
            }
            else
            {
                // Wait for all individual command tasks
                Task.WaitAll(pendingGarnetClientTasks);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompletePendingSERedis()
        {
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

            if (isMCommand)
            {
                // Wait for the single MGET/MSET task
                pendingGarnetClientTask.GetAwaiter().GetResult();
            }
            else
            {
                // Wait for all individual command tasks
                Task.WaitAll(pendingSERedisTasks);
            }
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