// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Numerics;
using System.Text;
using Garnet.common;

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

            // For MGET/MSET, batchSize is keys per command, so adjust batch count
            if (opts.Op is OpType.MGET or OpType.MSET)
            {
                // Generate enough MGET/MSET commands to cover the DB
                // Each MGET/MSET covers batchSize keys, so we need dbSize/batchSize commands
                batchCount = Math.Max(1, dbSizePerShard / batchSize);
            }
            else
            {
                // For regular ops, batchSize is number of commands
                batchCount = Math.Max(1, dbSizePerShard / batchSize);
            }

            requestBuffers = new byte[batchCount][];
            requestLengths = new int[batchCount];
            requestCounts = new int[batchCount];

            for (var b = 0; b < batchCount; b++)
            {
                var buffer = GenerateRandomBatch(batchSize, dbSizePerShard);
                requestBuffers[b] = buffer;
                requestLengths[b] = buffer.Length;

                // Track number of commands (responses to expect)
                requestCounts[b] = opts.Op is OpType.MGET or OpType.MSET ? 1 : batchSize;
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

            var batchSize = Math.Min(256, keysToLoad);

            // IMPORTANT: Always use primary endpoint for loading (replicas are read-only)
            var endpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)OpType.SET,
                onResponse,
                1 << 17, // Buffer size in bytes for the network buffer
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
            if (requestBuffers == null)
                throw new InvalidOperationException("Must call PrepareOfflineBuffers() before RunOffline()");

            var primaryEndpoint = new IPEndPoint(IPAddress.Parse(primaryAddress), primaryPort);
            var replicaEndpoint = hasReplica ? new IPEndPoint(IPAddress.Parse(replicaAddress), replicaPort) : null;
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