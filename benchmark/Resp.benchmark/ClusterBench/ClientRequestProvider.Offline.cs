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
        // Pre-generated buffers for offline mode
        byte[][] requestBuffers;
        int[] requestLengths;
        int[] requestCounts;  // Number of commands in each buffer (1 for MGET/MSET, batchSize for others)
        int batchCount;

        /// <summary>
        /// Pre-generate request buffers for offline mode.
        /// For GET/SET/etc: Each buffer contains batchSize commands.
        /// For MGET/MSET: Each buffer contains 1 command with batchSize keys.
        /// </summary>
        public void PrepareBuffers()
        {
            var batchSize = opts.BatchSize.First();
            var dbSizePerShard = opts.DbSize;

            // For LightClient, ensure batch size doesn't exceed 128KB buffer limit
            // Calculate max batch size based on value size (use 120KB for safety margin)
            var keyLen = Math.Max(opts.KeyLength, 8);
            var valLen = Math.Max(opts.ValueLength, 8);

            int effectiveBatchSize;
            if (opts.Op is OpType.MGET or OpType.MSET)
            {
                // For MGET/MSET: estimate bytes for single multi-key command
                // MSET: *N*2+1\r\n$4\r\nMSET\r\n + N × ($KLen\r\nK\r\n$VLen\r\nV\r\n)
                var bytesPerPair = keyLen + valLen + keyLen.ToString().Length + valLen.ToString().Length + 10;  // 10 for delimiters
                var maxKeysPerCommand = Math.Max(1, (120 * 1024) / bytesPerPair);  // 120KB safety margin
                effectiveBatchSize = (int)Math.Min(batchSize, maxKeysPerCommand);
            }
            else
            {
                // For GET/SET: estimate bytes for batch of individual commands
                var bytesPerCommand = 20 + keyLen + valLen + keyLen.ToString().Length + valLen.ToString().Length;
                var maxCommandsPerBatch = Math.Max(1, (120 * 1024) / bytesPerCommand);  // 120KB safety margin
                effectiveBatchSize = (int)Math.Min(batchSize, maxCommandsPerBatch);
            }

            // For MGET/MSET, batchSize is keys per command, so adjust batch count
            if (opts.Op is OpType.MGET or OpType.MSET)
            {
                // Generate enough MGET/MSET commands to cover the DB
                // Each MGET/MSET covers effectiveBatchSize keys, so we need dbSize/effectiveBatchSize commands
                batchCount = Math.Max(1, dbSizePerShard / effectiveBatchSize);
            }
            else
            {
                // For regular ops, effectiveBatchSize is number of commands
                batchCount = Math.Max(1, dbSizePerShard / effectiveBatchSize);
            }

            requestBuffers = new byte[batchCount][];
            requestLengths = new int[batchCount];
            requestCounts = new int[batchCount];

            for (var b = 0; b < batchCount; b++)
            {
                var buffer = GenerateRandomBatch(effectiveBatchSize, dbSizePerShard);
                requestBuffers[b] = buffer;
                requestLengths[b] = buffer.Length;

                // Track number of commands (responses to expect)
                requestCounts[b] = opts.Op is OpType.MGET or OpType.MSET ? 1 : effectiveBatchSize;
            }
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

            // Calculate max batch size to stay under LightClient's 128KB buffer limit
            // Each SET command: *3\r\n$3\r\nSET\r\n$KLen\r\nK\r\n$VLen\r\nV\r\n (~20 + keyLen + valueLen bytes)
            var keyLen = Math.Max(opts.KeyLength, 8);
            var valLen = Math.Max(opts.ValueLength, 8);
            var bytesPerSet = 20 + keyLen + valLen + keyLen.ToString().Length + valLen.ToString().Length;
            var maxBatchSize = Math.Max(1, (120 * 1024) / bytesPerSet);  // Stay under 120KB (safety margin)
            var batchSize = (int)Math.Min(maxBatchSize, Math.Min(256, keysToLoad));

            // IMPORTANT: Always use primary endpoint for loading (replicas are read-only)
            var endpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)OpType.SET,
                onResponse,
                1 << 17, // Buffer size in bytes for the network buffer (128KB)
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
        /// Run offline benchmark: send pre-generated batches and measure throughput/latency.
        /// Supports dual-connection routing for replica reads.
        /// </summary>
        public void RunOffline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            if (requestBuffers == null && opts.Client == ClientType.LightClient)
                throw new InvalidOperationException("Must call PrepareOfflineBuffers() before RunOffline()");

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

            // Buffer size must be large enough to hold the largest pre-generated request buffer
            var bufferSize = 1 << 17; // 128KB default
            if (requestBuffers != null)
            {
                var maxLen = requestLengths.Max();
                if (maxLen > bufferSize)
                    bufferSize = (int)BitOperations.RoundUpToPowerOf2((uint)maxLen);
            }

            using var primaryClient = new LightClient(
                primaryEndpoint,
                (int)opts.Op,
                onResponse,
                bufferSize,
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
                    bufferSize,
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
                var batchSize = opts.BatchSize.First();

                while (!done && sw.Elapsed < runTime)
                {
                    var buffer = requestBuffers[batchIdx % batchCount];
                    var len = requestLengths[batchIdx % batchCount];
                    var numCommands = requestCounts[batchIdx % batchCount];
                    var opStart = Stopwatch.GetTimestamp();

                    // Route entire batch based on operation type
                    // For offline mode, we route per-batch (all ops in batch are same type)
                    var useReplica = ShouldUseReplica(opts.Op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    fixed (byte* bufPtr = buffer)
                    {
                        client.Send(bufPtr, len, numCommands);
                        client.CompletePendingRequests();
                    }

                    // Track bytes received
                    var bytesRcvd = client.TotalBytesReceived;
                    Interlocked.Exchange(ref bytesReceived, bytesRcvd);

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);
                    Interlocked.Add(ref bytesSent, len);

                    // Track per-endpoint metrics
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
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
                var numCommands = isMCommand ? 1 : batchSize;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var useReplica = ShouldUseReplica(opts.Op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    if (isMCommand)
                    {
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
                        client.Execute(args.ToArray());
                    }
                    else
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            var key = keyGen.GenerateKey(rng, rng.Next(dbSizePerShard));
                            if (opts.Op == OpType.GET)
                                client.Execute("GET", key);
                            else if (opts.Op == OpType.SET)
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
                        ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaClient != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);
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
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
                var numCommands = isMCommand ? 1 : batchSize;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var useReplica = ShouldUseReplica(opts.Op);
                    var client = (useReplica && replicaClient != null) ? replicaClient : primaryClient;

                    if (isMCommand)
                    {
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

                    var elapsed = Stopwatch.GetTimestamp() - opStart;
                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);

                    // Track bytes (approximate RESP protocol overhead)
                    var sentBytes = isMCommand
                        ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaClient != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);
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
                var isMCommand = opts.Op is OpType.MGET or OpType.MSET;
                var numCommands = isMCommand ? 1 : batchSize;

                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var useReplica = ShouldUseReplica(opts.Op);
                    var db = (useReplica && replicaDb != null) ? replicaDb : primaryDb;

                    if (isMCommand)
                    {
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

                    var elapsed = Stopwatch.GetTimestamp() - opStart;
                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, numCommands);

                    // Track bytes (approximate RESP protocol overhead)
                    var sentBytes = isMCommand
                        ? CalculateRespSentBytes(opts.Op, batchSize)  // Single MGET/MSET command with batchSize keys
                        : CalculateRespSentBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET commands (1 key each)
                    var rcvdBytes = isMCommand
                        ? CalculateRespReceivedBytes(opts.Op, batchSize)  // Single MGET/MSET response with batchSize values
                        : CalculateRespReceivedBytes(opts.Op, 1) * batchSize;  // batchSize GET/SET responses

                    Interlocked.Add(ref bytesSent, sentBytes);
                    Interlocked.Add(ref bytesReceived, rcvdBytes);

                    if (useReplica && replicaDb != null)
                        Interlocked.Add(ref replicaOps, numCommands);
                    else
                        Interlocked.Add(ref primaryOps, numCommands);
                }
            }
            finally
            {
                primaryRedis?.Dispose();
                replicaRedis?.Dispose();
            }
        }

        private byte[] GenerateBatch(int batchSize, int startKeyIndex)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < batchSize; i++)
            {
                var key = keyGen.GenerateKey(rng, startKeyIndex + i);
                AppendCommand(sb, opts.Op, key);
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Generate a batch of random keys for benchmarking.
        /// For GET/SET/etc: batchSize = number of commands, each with 1 key.
        /// For MGET/MSET: batchSize = number of keys per single MGET/MSET command.
        /// </summary>
        private byte[] GenerateRandomBatch(int batchSize, int dbSize)
        {
            var sb = new StringBuilder();

            if (opts.Op is OpType.MGET or OpType.MSET)
            {
                // For MGET/MSET: generate ONE command with batchSize keys
                var keys = new string[batchSize];
                for (var k = 0; k < batchSize; k++)
                {
                    keys[k] = keyGen.GenerateKey(rng, rng.Next(dbSize));
                }
                AppendMCommand(sb, opts.Op, keys);
            }
            else
            {
                // For other ops: generate batchSize commands, each with 1 key
                for (var i = 0; i < batchSize; i++)
                {
                    var key = keyGen.GenerateKey(rng, rng.Next(dbSize));
                    AppendCommand(sb, opts.Op, key);
                }
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
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
                sb.Append($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n");
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }
    }
}