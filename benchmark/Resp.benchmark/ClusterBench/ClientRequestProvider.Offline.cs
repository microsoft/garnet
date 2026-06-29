// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Numerics;
using System.Text;
using Garnet.client;
using Garnet.common;
using StackExchange.Redis;

namespace Resp.benchmark
{
    public unsafe partial class ClientRequestProvider
    {
        // Pre-generated request batches for offline mode
        ClusterWorkload workload;   // Holds primary requests (always) + replica requests (conditional)
        int batchCount;
        int offlineBufferSize;

        /// <summary>
        /// Pre-generate request buffers for offline mode.
        ///
        /// Single-operation mode (normal):
        ///   - Generate PrimaryRequests[] array with write OR read operations
        ///
        /// Mixed workload mode (when replicas exist + write op + --replica-ops-percent > 0):
        ///   - Generate PrimaryRequests[] with write operations (SET, MSET, etc.)
        ///   - Generate ReplicaRequests[] with corresponding reads (GET, MGET) FOR THE SAME KEYS
        ///   - Pre-compute ReadUseReplica[] routing decisions based on --replica-ops-percent
        ///
        /// Execution model: Choose ONE request per iteration:
        ///   - ReadUseReplica[i] = true  → execute ReplicaRequests[i] to replica
        ///   - ReadUseReplica[i] = false → execute PrimaryRequests[i] to primary
        ///
        /// For GET/SET/etc: Each request contains batchSize commands.
        /// For MGET/MSET: Each request contains 1 command with batchSize keys.
        /// </summary>
        public void PrepareBuffers()
        {
            var batchSize = opts.BatchSize.First();
            var dbSizePerShard = opts.DbSize;
            batchCount = Math.Max(1, dbSizePerShard / batchSize);

            // Always fill primary requests (write ops in mixed mode, or single op in normal mode)
            workload.PrimaryRequests = new Request[batchCount];

            for (var b = 0; b < batchCount; b++)
            {
                var buffer = GenerateRandomBatch(batchSize, dbSizePerShard, b);
                workload.PrimaryRequests[b] = new Request
                {
                    RespData = buffer,
                    ByteCount = buffer.Length,
                    CommandCount = opts.Op is OpType.MGET or OpType.MSET ? 1 : batchSize
                };
            }

            // If replicas exist and we have a write op with read mapping, generate replica-eligible requests
            if (ShouldGenerateMixedWorkload())
            {
                var readOp = GetReadOperationType();

                workload.ReplicaRequests = new Request[batchCount];
                workload.ReadUseReplica = new bool[batchCount];

                for (var b = 0; b < batchCount; b++)
                {
                    // Generate corresponding read request (GET/MGET) for THE SAME KEYS as primary
                    var readBuffer = GenerateReadBatchForKeys(batchSize, dbSizePerShard, b, readOp);
                    workload.ReplicaRequests[b] = new Request
                    {
                        RespData = readBuffer,
                        ByteCount = readBuffer.Length,
                        CommandCount = readOp is OpType.MGET ? 1 : batchSize
                    };

                    // Pre-compute routing decision: should this read go to replica?
                    workload.ReadUseReplica[b] = rng.Next(100) < opts.ReplicaOpsPercent;
                }
            }

            ComputeOfflineBufferSize();
        }

        /// <summary>
        /// Compute the LightClient buffer size needed to hold the largest pre-generated request.
        /// </summary>
        private void ComputeOfflineBufferSize()
        {
            var maxLen = workload.PrimaryRequests.Max(r => r.ByteCount);

            if (workload.ReplicaRequests != null)
            {
                var maxReplicaLen = workload.ReplicaRequests.Max(r => r.ByteCount);
                maxLen = Math.Max(maxLen, maxReplicaLen);
            }

            offlineBufferSize = (int)BitOperations.RoundUpToPowerOf2((uint)maxLen);
            offlineBufferSize = Math.Max(offlineBufferSize, 1 << 17); // At least 128KB
        }

        /// <summary>
        /// Generate a batch of random keys for benchmarking.
        /// For GET/SET/etc: batchSize = number of commands, each with 1 key.
        /// For MGET/MSET: batchSize = number of keys per single MGET/MSET command.
        /// </summary>
        /// <param name="bufferIndex">Seed for deterministic key generation (ensures reads match writes)</param>
        private byte[] GenerateRandomBatch(int batchSize, int dbSize, int bufferIndex)
        {
            var keyLen = Math.Max(opts.KeyLength, 8);
            var valLen = Math.Max(opts.ValueLength, 8);

            // Estimate capacity based on op type to avoid StringBuilder reallocations
            var estimatedCapacity = opts.Op switch
            {
                // GET: *2\r\n$3\r\nGET\r\n$<keyLenDigits>\r\n<key>\r\n ≈ 20 + keyLen per command
                OpType.GET => batchSize * (20 + keyLen),
                // SET: *3\r\n$3\r\nSET\r\n$<kld>\r\n<key>\r\n$<vld>\r\n<val>\r\n ≈ 30 + keyLen + valLen
                OpType.SET => batchSize * (30 + keyLen + valLen),
                // INCR/DEL: similar to GET
                OpType.INCR or OpType.DEL => batchSize * (22 + keyLen),
                // MGET: header + N × ($<kld>\r\n<key>\r\n) ≈ 20 + N × (6 + keyLen)
                OpType.MGET => 20 + batchSize * (6 + keyLen),
                // MSET: header + N × ($<kld>\r\n<key>\r\n$<vld>\r\n<val>\r\n) ≈ 20 + N × (12 + keyLen + valLen)
                OpType.MSET => 20 + batchSize * (12 + keyLen + valLen),
                _ => batchSize * (30 + keyLen + valLen)
            };

            var sb = new StringBuilder(estimatedCapacity);
            var isMCommand = opts.Op is OpType.MGET or OpType.MSET;

            // Create deterministic RNG with buffer index for reproducible key generation
            var deterministicRng = new Random(31337 + (threadIndex * 1000) + shard.Port + bufferIndex);

            if (isMCommand)
            {
                // MGET/MSET: one command with batchSize keys
                AppendCommand(sb, opts.Op, batchSize, deterministicRng, dbSize);
            }
            else
            {
                // Single-key ops: batchSize commands, each with 1 key
                for (var i = 0; i < batchSize; i++)
                    AppendCommand(sb, opts.Op, 1, deterministicRng, dbSize);
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Generate a batch of read commands for the same keys as a write batch.
        /// Uses deterministic key generation (same buffer index) to ensure reads target the same keys as writes.
        /// For GET: batchSize individual GET commands (one per key).
        /// For MGET: One MGET command with batchSize keys.
        /// </summary>
        private byte[] GenerateReadBatchForKeys(int batchSize, int dbSize, int bufferIndex, OpType readOp)
        {
            var keyLen = Math.Max(opts.KeyLength, 8);

            // Estimate capacity based on read op type
            var estimatedCapacity = readOp switch
            {
                // GET: *2\r\n$3\r\nGET\r\n$<keyLenDigits>\r\n<key>\r\n ≈ 20 + keyLen per command
                OpType.GET => batchSize * (20 + keyLen),
                // MGET: header + N × ($<kld>\r\n<key>\r\n) ≈ 20 + N × (6 + keyLen)
                OpType.MGET => 20 + batchSize * (6 + keyLen),
                // GETBIT: *3\r\n$6\r\nGETBIT\r\n$<kld>\r\n<key>\r\n$1\r\n0\r\n ≈ 30 + keyLen
                OpType.GETBIT => batchSize * (30 + keyLen),
                // PFCOUNT: *2\r\n$7\r\nPFCOUNT\r\n$<kld>\r\n<key>\r\n ≈ 22 + keyLen
                OpType.PFCOUNT => batchSize * (22 + keyLen),
                // ZCARD: *2\r\n$5\r\nZCARD\r\n$<kld>\r\n<key>\r\n ≈ 20 + keyLen
                OpType.ZCARD => batchSize * (20 + keyLen),
                _ => batchSize * (25 + keyLen)
            };

            var sb = new StringBuilder(estimatedCapacity);
            var isMCommand = readOp is OpType.MGET;

            // Create deterministic RNG with same seed to generate same keys as write buffer
            var deterministicRng = new Random(31337 + (threadIndex * 1000) + shard.Port + bufferIndex);

            if (isMCommand)
            {
                // MGET: one command with batchSize keys
                AppendCommand(sb, readOp, batchSize, deterministicRng, dbSize);
            }
            else
            {
                // Single-key reads: batchSize commands, each with 1 key
                for (var i = 0; i < batchSize; i++)
                    AppendCommand(sb, readOp, 1, deterministicRng, dbSize);
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Load data into the shard for this provider's key space.
        /// Always uses primary connection only - replicas are read-only and cannot accept writes.
        ///
        /// In sharded mode: Each provider loads a slice of the key space based on shardThreadIndex.
        /// In worker pool mode: Each provider loads the entire DbSize (only worker[0]'s providers are used).
        /// </summary>
        public void LoadData()
        {
            var threadsPerShard = opts.NumThreads.First();

            // In worker pool mode (--pool), load entire DbSize per shard
            // In sharded mode, partition DbSize across threads-per-shard
            int keysToLoad;
            int keyOffset;

            if (opts.Pool)
            {
                // Worker pool mode: Load entire DbSize
                keysToLoad = opts.DbSize;
                keyOffset = 0;
            }
            else
            {
                // Sharded mode: Partition key space across threads
                // Each thread loads a distinct slice of the key space:
                // thread 0 loads [0, dbSizePerThread), thread 1 loads [dbSizePerThread, 2*dbSizePerThread), etc.
                keysToLoad = opts.DbSize / threadsPerShard;
                keyOffset = shardThreadIndex * keysToLoad;
            }

            // Compute exact max RESP bytes for one SET command:
            // *3\r\n$3\r\nSET\r\n$<keyLenDigits>\r\n<key>\r\n$<valLenDigits>\r\n<val>\r\n
            var keyLen = Math.Max(opts.KeyLength, 8);
            var valLen = Math.Max(opts.ValueLength, 8);
            var bytesPerSet = 4 + 9 + 1 + keyLen.ToString().Length + 2 + keyLen + 2
                                      + 1 + valLen.ToString().Length + 2 + valLen + 2;
            var maxBatchSize = Math.Max(1, (120 * 1024) / bytesPerSet);  // Stay under 120KB (safety margin)
            var batchSize = (int)Math.Min(maxBatchSize, Math.Min(256, keysToLoad));

            // Size the LightClient buffer to fit the largest load batch
            var loadBufferSize = (int)BitOperations.RoundUpToPowerOf2((uint)(batchSize * bytesPerSet));
            loadBufferSize = Math.Max(loadBufferSize, 1 << 17); // At least 128KB

            // IMPORTANT: Always use primary endpoint for loading (replicas are read-only)
            var endpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)OpType.SET,
                onResponse,
                loadBufferSize,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            var loaded = 0;
            while (loaded < keysToLoad)
            {
                var thisBatch = Math.Min(batchSize, keysToLoad - loaded);
                var buffer = GenerateLoadBatch(thisBatch, keyOffset + loaded);

                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, buffer.Length, thisBatch);
                    client.CompletePendingRequests();
                }
                loaded += thisBatch;
            }

            Interlocked.Add(ref keysLoaded, loaded);
        }

        /// <summary>
        /// Generate a batch of sequential keys for loading data.
        /// Always generates SET commands (one key-value pair per operation).
        /// </summary>
        private byte[] GenerateLoadBatch(int batchSize, int startKeyIndex)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < batchSize; i++)
            {
                var key = keyGen.GenerateKey(rng, startKeyIndex + i);
                var value = GenerateValue();
                _ = sb.Append($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n");
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Run offline benchmark: send pre-generated batches and measure throughput/latency.
        /// Supports dual-connection routing for replica reads.
        /// </summary>
        public void RunOffline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            if (workload.PrimaryRequests == null && opts.Client == ClientType.LightClient)
                throw new InvalidOperationException("Must call PrepareBuffers() before RunOffline()");

            var primaryEndpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var replicaEndpoint = hasReplica ? new IPEndPoint(IPAddress.Parse(replicaAddress), replicaPort) : null;

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    RunOfflineLightClient(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClientSession:
                    RunOfflineGarnetClientSession(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClient:
                    RunOfflineGarnetClient(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                case ClientType.SERedis:
                    RunOfflineSERedis(primaryEndpoint, replicaEndpoint, startSignal, runTime);
                    break;
                default:
                    throw new NotSupportedException($"Client type {opts.Client} not supported in offline mode.");
            }
        }

        private void RunOfflineLightClient(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            // Determine operation types for LightClient initialization
            var primaryOpType = (int)opts.Op;
            var replicaOpType = primaryOpType;  // Default to same operation
            
            // If mixed workload, replica client should be configured for read operations
            if (workload.ReplicaRequests != null)
            {
                var readOp = GetReadOperationType();
                replicaOpType = (int)readOp;
            }

            using var primaryClient = new LightClient(
                primaryEndpoint,
                primaryOpType,
                onResponse,
                offlineBufferSize,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            primaryClient.Connect();
            primaryClient.Authenticate(opts.Auth);

            // Create replica client if assigned
            LightClient replicaClient = null;
            if (replicaEndpoint != null)
            {
                replicaClient = new LightClient(
                    replicaEndpoint,
                    replicaOpType,  // Use read operation type for mixed workload
                    onResponse,
                    offlineBufferSize,
                    opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

                replicaClient.Connect();
                replicaClient.Authenticate(opts.Auth);
                replicaClient.ReadOnly();
            }

            try
            {
                // Wait for start signal
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var batchIdx = 0;

                while (!done && sw.Elapsed < runTime)
                {
                    var bufIdx = batchIdx % batchCount;
                    Request request;
                    bool useReplica;

                    // Model B: Choose ONE request per iteration based on ReadUseReplica flag
                    if (workload.ReplicaRequests != null && workload.ReadUseReplica[bufIdx])
                    {
                        // Execute read request to replica
                        request = workload.ReplicaRequests[bufIdx];
                        useReplica = true;
                    }
                    else
                    {
                        // Execute primary request (write or fallback read)
                        request = workload.PrimaryRequests[bufIdx];
                        useReplica = false;
                    }

                    var opStart = Stopwatch.GetTimestamp();
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    fixed (byte* bufPtr = request.RespData)
                    {
                        client.Send(bufPtr, request.ByteCount, request.CommandCount);
                        client.CompletePendingRequests();
                    }

                    // Track bytes received
                    var bytesRcvd = client.TotalBytesReceived;
                    Interlocked.Exchange(ref bytesReceived, bytesRcvd);

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, request.CommandCount);
                    Interlocked.Add(ref bytesSent, request.ByteCount);

                    // Track per-endpoint metrics
                    if (useReplica && replicaClient != null)
                        Interlocked.Add(ref replicaOps, request.CommandCount);
                    else
                        Interlocked.Add(ref primaryOps, request.CommandCount);

                    batchIdx++;
                }
            }
            finally
            {
                replicaClient?.Dispose();
            }
        }

        private void RunOfflineGarnetClientSession(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
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
                replicaClient.Execute("READONLY");
                replicaClient.CompletePending();
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var batchSize = opts.BatchSize.First();
                var dbSizePerShard = opts.DbSize;
                var batchIdx = 0;

                // Determine operation types for mixed workload
                var primaryOp = opts.Op;
                OpType? replicaOp = null;
                if (workload.ReplicaRequests != null)
                    replicaOp = GetReadOperationType();

                while (!done && sw.Elapsed < runTime)
                {
                    var bufIdx = batchIdx % batchCount;
                    bool useReplica;
                    OpType currentOp;
                    int numCommands;

                    // Model B: Choose ONE operation per iteration based on ReadUseReplica flag
                    if (workload.ReplicaRequests != null && workload.ReadUseReplica[bufIdx])
                    {
                        useReplica = true;
                        currentOp = replicaOp.Value;
                    }
                    else
                    {
                        useReplica = false;
                        currentOp = primaryOp;
                    }

                    var isMCommand = currentOp is OpType.MGET or OpType.MSET;
                    numCommands = isMCommand ? 1 : batchSize;
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    var opStart = Stopwatch.GetTimestamp();

                    if (isMCommand)
                    {
                        var args = new List<string>();
                        if (currentOp == OpType.MGET)
                        {
                            args.Add("MGET");
                            for (int i = 0; i < batchSize; i++)
                                args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                        }
                        else if (currentOp == OpType.MSET)
                        {
                            args.Add("MSET");
                            for (int i = 0; i < batchSize; i++)
                            {
                                args.Add(keyGen.GenerateKey(rng, rng.Next(dbSizePerShard)));
                                args.Add(GenerateValue());
                            }
                        }
                        client.Execute(args.ToArray());
                    }
                    else
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            if (currentOp == OpType.GET)
                                client.Execute("GET", key);
                            else if (currentOp == OpType.SET)
                                client.Execute("SET", key, GenerateValue());
                            else
                                client.Execute("GET", key);
                        }
                    }

                    client.CompletePending();

                    var elapsed = Stopwatch.GetTimestamp() - opStart;
                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);

                    // Track bytes (approximate RESP protocol overhead)
                    var keysInBatch = isMCommand ? batchSize : batchSize;  // MGET/MSET has batchSize keys, GET/SET has batchSize commands (1 key each)
                    var sentBytes = isMCommand
                        ? CalculateRespSentBytes(currentOp, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(currentOp, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(currentOp, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(currentOp, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaClient != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);

                    batchIdx++;
                }
            }
            finally
            {
                replicaClient?.Dispose();
            }
        }

        private void RunOfflineGarnetClient(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var primaryClient = new GarnetClient(
                primaryEndpoint,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                recordLatency: opts.ClientHistogram);

            primaryClient.Connect();
            if (opts.Auth != null)
                primaryClient.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

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
                replicaClient.ExecuteForStringResultAsync("READONLY").GetAwaiter().GetResult();
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var batchSize = opts.BatchSize.First();
                var dbSizePerShard = opts.DbSize;
                var batchIdx = 0;

                // Determine operation types for mixed workload
                var primaryOp = opts.Op;
                OpType? replicaOp = null;
                if (workload.ReplicaRequests != null)
                    replicaOp = GetReadOperationType();

                while (!done && sw.Elapsed < runTime)
                {
                    var bufIdx = batchIdx % batchCount;
                    bool useReplica;
                    OpType currentOp;
                    int numCommands;

                    // Model B: Choose ONE operation per iteration based on ReadUseReplica flag
                    if (workload.ReplicaRequests != null && workload.ReadUseReplica[bufIdx])
                    {
                        useReplica = true;
                        currentOp = replicaOp.Value;
                    }
                    else
                    {
                        useReplica = false;
                        currentOp = primaryOp;
                    }

                    var isMCommand = currentOp is OpType.MGET or OpType.MSET;
                    numCommands = isMCommand ? 1 : batchSize;
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    var opStart = Stopwatch.GetTimestamp();

                    if (isMCommand)
                    {
                        if (currentOp == OpType.MGET)
                        {
                            var keys = new string[batchSize];
                            for (int i = 0; i < batchSize; i++)
                                keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            client.StringGetAsync(keys).GetAwaiter().GetResult();
                        }
                        else if (currentOp == OpType.MSET)
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
                        var tasks = new Task[batchSize];
                        for (int i = 0; i < batchSize; i++)
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            if (currentOp == OpType.GET)
                                tasks[i] = client.StringGetAsMemoryAsync(key);
                            else if (currentOp == OpType.SET)
                                tasks[i] = client.StringSetAsync(key, GenerateValue());
                            else
                                tasks[i] = client.StringGetAsMemoryAsync(key);
                        }
                        Task.WaitAll(tasks);
                    }

                    var elapsed = Stopwatch.GetTimestamp() - opStart;
                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);

                    // Track bytes (approximate RESP protocol overhead)
                    var sentBytes = isMCommand
                        ? CalculateRespSentBytes(currentOp, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(currentOp, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(currentOp, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(currentOp, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaClient != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);

                    batchIdx++;
                }
            }
            finally
            {
                primaryClient.Dispose();
                replicaClient?.Dispose();
            }
        }

        private void RunOfflineSERedis(IPEndPoint primaryEndpoint, IPEndPoint replicaEndpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var primaryConfig = BenchUtils.GetConfig(primaryEndpoint.Address.ToString(), primaryEndpoint.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost);
            if (!string.IsNullOrEmpty(opts.Auth))
                primaryConfig.Password = opts.Auth;
            var primaryRedis = ConnectionMultiplexer.Connect(primaryConfig);
            var primaryDb = primaryRedis.GetDatabase(0);

            ConnectionMultiplexer replicaRedis = null;
            IDatabase replicaDb = null;
            if (replicaEndpoint != null)
            {
                var replicaConfig = BenchUtils.GetConfig(replicaEndpoint.Address.ToString(), replicaEndpoint.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost);
                if (!string.IsNullOrEmpty(opts.Auth))
                    replicaConfig.Password = opts.Auth;
                replicaRedis = ConnectionMultiplexer.Connect(replicaConfig);
                replicaDb = replicaRedis.GetDatabase(0);
            }

            try
            {
                startSignal.Wait();

                var sw = Stopwatch.StartNew();
                var batchSize = opts.BatchSize.First();
                var dbSizePerShard = opts.DbSize;
                var batchIdx = 0;

                // Determine operation types for mixed workload
                var primaryOp = opts.Op;
                OpType? replicaOp = null;
                if (workload.ReplicaRequests != null)
                    replicaOp = GetReadOperationType();

                while (!done && sw.Elapsed < runTime)
                {
                    var bufIdx = batchIdx % batchCount;
                    bool useReplica;
                    OpType currentOp;
                    int numCommands;

                    // Model B: Choose ONE operation per iteration based on ReadUseReplica flag
                    if (workload.ReplicaRequests != null && workload.ReadUseReplica[bufIdx])
                    {
                        useReplica = true;
                        currentOp = replicaOp.Value;
                    }
                    else
                    {
                        useReplica = false;
                        currentOp = primaryOp;
                    }

                    var isMCommand = currentOp is OpType.MGET or OpType.MSET;
                    numCommands = isMCommand ? 1 : batchSize;
                    var db = (useReplica && replicaDb != null) ? replicaDb : primaryDb;

                    var opStart = Stopwatch.GetTimestamp();

                    if (isMCommand)
                    {
                        if (currentOp == OpType.MGET)
                        {
                            var keys = new RedisKey[batchSize];
                            for (int i = 0; i < batchSize; i++)
                                keys[i] = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            db.StringGet(keys);
                        }
                        else if (currentOp == OpType.MSET)
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
                        for (int i = 0; i < batchSize; i++)
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            if (currentOp == OpType.GET)
                                db.StringGet(key);
                            else if (currentOp == OpType.SET)
                                db.StringSet(key, GenerateValue());
                            else
                                db.StringGet(key);
                        }
                    }

                    var elapsed = Stopwatch.GetTimestamp() - opStart;
                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);

                    // Track bytes (approximate RESP protocol overhead)
                    var sentBytes = isMCommand
                        ? CalculateRespSentBytes(currentOp, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(currentOp, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(currentOp, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(currentOp, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaDb != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);

                    batchIdx++;
                }
            }
            finally
            {
                primaryRedis?.Dispose();
                replicaRedis?.Dispose();
            }
        }
    }
}