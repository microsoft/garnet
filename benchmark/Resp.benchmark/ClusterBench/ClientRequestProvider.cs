// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
using HdrHistogram;

namespace Resp.benchmark
{
    /// <summary>
    /// A single-threaded worker that owns connections to a primary shard (and optionally a replica),
    /// generates keys restricted to that shard's assigned slots,
    /// and measures its own throughput and latency.
    /// Supports probabilistic routing of read operations to replicas when --allow-replica-reads is enabled.
    /// </summary>
    public unsafe partial class ClientRequestProvider : IDisposable
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        readonly PrimaryInfo shard;
        readonly Options opts;
        readonly int threadIndex;
        readonly SlotKeyGenerator keyGen;
        readonly Random rng;

        // Dual-connection endpoints (replica can be null)
        readonly string primaryAddress;
        readonly int primaryPort;
        readonly string replicaAddress;
        readonly int replicaPort;
        readonly bool hasReplica;

        // Metrics
        readonly LongHistogram histogram;
        long opsCompleted;
        long bytesSent;
        long bytesReceived;
        long keysLoaded;
        long primaryOps;  // Operations executed on primary
        long replicaOps;  // Operations executed on replica

        volatile bool done;

        public long OpsCompleted => Interlocked.Read(ref opsCompleted);
        public long BytesSent => Interlocked.Read(ref bytesSent);
        public long BytesReceived => Interlocked.Read(ref bytesReceived);
        public long KeysLoaded => Interlocked.Read(ref keysLoaded);
        public long PrimaryOps => Interlocked.Read(ref primaryOps);
        public long ReplicaOps => Interlocked.Read(ref replicaOps);
        public LongHistogram Histogram => histogram;
        public PrimaryInfo Shard => shard;
        public string KeyPrefix => keyGen.KeyPrefix;
        public bool HasReplica => hasReplica;
        public string ReplicaEndpoint => hasReplica ? $"{replicaAddress}:{replicaPort}" : null;

        /// <summary>
        /// Index of this thread within its shard (0-based). Used to partition key space during loading.
        /// </summary>
        private readonly int shardThreadIndex;

        public ClientRequestProvider(PrimaryInfo shard, ReplicaInfo assignedReplica, Options opts, int threadIndex, int shardThreadIndex)
        {
            this.shard = shard;
            this.opts = opts;
            this.threadIndex = threadIndex;
            this.shardThreadIndex = shardThreadIndex;
            this.rng = new Random(31337 + (threadIndex * 1000) + shard.Port);
            this.keyGen = new SlotKeyGenerator(shard, opts.KeyLength);
            this.histogram = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);

            // Primary endpoint (always set)
            this.primaryAddress = shard.Address;
            this.primaryPort = shard.Port;

            // Replica endpoint (optional)
            if (assignedReplica != null)
            {
                this.replicaAddress = assignedReplica.Address;
                this.replicaPort = assignedReplica.Port;
                this.hasReplica = true;
            }
            else
            {
                this.hasReplica = false;
            }
        }

        /// <summary>
        /// Determines which endpoint to use for a given operation based on operation type and --replica-read-percent setting.
        ///
        /// Routing logic:
        ///   - Write operations → always primary (replicas are read-only)
        ///   - Read operations → probabilistic based on --replica-read-percent
        ///   - If replicas exist, they always serve reads (percentage controls distribution)
        ///
        /// Statistical distribution:
        ///   - Each client makes independent probabilistic routing decisions per read operation
        ///   - Across all clients for a shard, approximately X% of reads go to replicas
        ///   - Example: --replica-read-percent=50 with 6 clients doing 1000 reads each
        ///     → ~3000 reads to replicas, ~3000 reads to primary (50% distribution per shard)
        /// </summary>
        /// <param name="op">The operation type to execute</param>
        /// <returns>True if replica endpoint should be used, false if primary endpoint should be used</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ShouldUseReplica(OpType op)
        {
            // If no replica is assigned to this worker, always use primary
            if (!hasReplica)
                return false;

            // If replica reads are set to 0%, always use primary
            if (opts.ReplicaOpsPercent == 0)
                return false;

            // Write operations always go to primary (replicas are read-only)
            if (OperationClassifier.IsWriteOperation(op))
                return false;

            // Read operations: probabilistic routing based on percentage
            if (OperationClassifier.IsReadOperation(op))
            {
                // If percentage is 100, always use replica
                if (opts.ReplicaOpsPercent >= 100)
                    return true;

                // Probabilistic: generate random number 0-99, compare with percentage
                // This ensures approximately X% of reads go to replicas across all clients
                var randomValue = rng.Next(100);
                return randomValue < opts.ReplicaOpsPercent;
            }

            // Unknown operation type: default to primary (safe default)
            return false;
        }

        /// <summary>
        /// Determines if mixed write/read workload should be generated for offline mode.
        /// Mixed workload is enabled when:
        /// - Replica read percentage > 0
        /// - This provider has a replica assigned
        /// - The primary operation is a write operation
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ShouldGenerateMixedWorkload()
        {
            return opts.ReplicaOpsPercent > 0
                && hasReplica
                && OperationClassifier.IsWriteOperation(opts.Op);
        }

        /// <summary>
        /// Gets the corresponding read operation for the configured write operation.
        /// Throws if no read mapping exists.
        /// </summary>
        private OpType GetReadOperationType()
        {
            var readOp = OperationClassifier.GetCorrespondingReadOperation(opts.Op);
            if (readOp == null)
            {
                throw new Exception($"No read operation mapping for {opts.Op}. " +
                    $"Cannot generate mixed workload with --replica-read-percent > 0. " +
                    $"Either set --replica-read-percent 0 or use a write operation with read mapping (SET, MSET, SETBIT, etc.)");
            }
            return readOp.Value;
        }

        /// <summary>
        /// Signal this provider to stop.
        /// </summary>
        public void Stop() => done = true;

        public void Dispose()
        {
            workload.PrimaryRequests = null;
            workload.ReplicaRequests = null;
            workload.ReadUseReplica = null;
            DisposeWorkerPoolConnections();
        }

        private byte[] FormatRequest(OpType op, string key)
        {
            return op switch
            {
                OpType.GET => Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n"),
                OpType.SET => Encoding.ASCII.GetBytes($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${opts.ValueLength}\r\n{GenerateValue()}\r\n"),
                OpType.INCR => Encoding.ASCII.GetBytes($"*2\r\n$4\r\nINCR\r\n${key.Length}\r\n{key}\r\n"),
                OpType.DEL => Encoding.ASCII.GetBytes($"*2\r\n$3\r\nDEL\r\n${key.Length}\r\n{key}\r\n"),
                OpType.MGET or OpType.MSET => throw new InvalidOperationException($"{op} requires multiple keys - use FormatMRequest"),
                _ => Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n"),
            };
        }

        /// <summary>
        /// Format MGET or MSET request with multiple keys generated inline.
        /// </summary>
        private byte[] FormatMRequest(OpType op, int keyCount, Random rng, int dbSize)
        {
            var sb = new StringBuilder(256);
            AppendCommand(sb, op, keyCount, rng, dbSize);
            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Append a RESP command to the string builder.
        /// Writes the header first, then appends key(/value) pairs iteratively.
        /// For single-key ops (GET, SET, INCR, DEL): keyCount=1, generates one key.
        /// For multi-key ops (MGET, MSET): keyCount=N, generates N keys inline.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AppendCommand(StringBuilder sb, OpType op, int keyCount, Random rng, int dbSize)
        {
            // Write RESP array header
            switch (op)
            {
                case OpType.GET:
                    sb.Append($"*2\r\n$3\r\nGET\r\n");
                    break;
                case OpType.SET:
                    sb.Append($"*3\r\n$3\r\nSET\r\n");
                    break;
                case OpType.INCR:
                    sb.Append($"*2\r\n$4\r\nINCR\r\n");
                    break;
                case OpType.DEL:
                    sb.Append($"*2\r\n$3\r\nDEL\r\n");
                    break;
                case OpType.MGET:
                    sb.Append($"*{keyCount + 1}\r\n$4\r\nMGET\r\n");
                    break;
                case OpType.MSET:
                    sb.Append($"*{(keyCount * 2) + 1}\r\n$4\r\nMSET\r\n");
                    break;
                default:
                    sb.Append($"*2\r\n$3\r\nGET\r\n");
                    break;
            }

            // Append key(/value) pairs
            var hasValue = op is OpType.SET or OpType.MSET;
            for (var i = 0; i < keyCount; i++)
            {
                var key = keyGen.GenerateKey(rng, rng.Next(dbSize));
                sb.Append($"${key.Length}\r\n{key}\r\n");
                if (hasValue)
                {
                    var value = GenerateValue();
                    sb.Append($"${value.Length}\r\n{value}\r\n");
                }
            }
        }

        private string GenerateValue()
        {
            return new string('v', opts.ValueLength == 0 ? 8 : opts.ValueLength);
        }

        /// <summary>
        /// Calculate approximate RESP protocol bytes for a command (sent bytes).
        /// For MGET: *N+1\r\n$4\r\nMGET\r\n$KLen\r\nK\r\n... (N keys)
        /// For MSET: *N*2+1\r\n$4\r\nMSET\r\n$KLen\r\nK\r\n$VLen\r\nV\r\n... (N pairs)
        /// For GET: *2\r\n$3\r\nGET\r\n$KLen\r\nK\r\n
        /// For SET: *3\r\n$3\r\nSET\r\n$KLen\r\nK\r\n$VLen\r\nV\r\n
        /// </summary>
        private int CalculateRespSentBytes(OpType op, int keyCount)
        {
            var keyLen = Math.Max(opts.KeyLength, 8);  // SlotKeyGenerator minimum
            var valLen = Math.Max(opts.ValueLength, 8);  // GenerateValue minimum

            return op switch
            {
                OpType.GET => 3 + 2 + 1 + 2 + 3 + 2 + keyLen.ToString().Length + 2 + keyLen + 2,  // *2\r\n$3\r\nGET\r\n$KLen\r\nK\r\n
                OpType.SET => 3 + 2 + 1 + 2 + 3 + 2 + keyLen.ToString().Length + 2 + keyLen + 2 + valLen.ToString().Length + 2 + valLen + 2,  // *3\r\n$3\r\nSET\r\n$KLen\r\nK\r\n$VLen\r\nV\r\n
                OpType.MGET => (keyCount + 1).ToString().Length + 2 + 2 + 4 + 2 + 4 + 2 + keyCount * (keyLen.ToString().Length + 2 + keyLen + 2),  // *N+1\r\n$4\r\nMGET\r\n + N × ($KLen\r\nK\r\n)
                OpType.MSET => (keyCount * 2 + 1).ToString().Length + 2 + 2 + 4 + 2 + 4 + 2 + keyCount * (keyLen.ToString().Length + 2 + keyLen + 2 + valLen.ToString().Length + 2 + valLen + 2),  // *N*2+1\r\n$4\r\nMSET\r\n + N × ($KLen\r\nK\r\n$VLen\r\nV\r\n)
                OpType.INCR => 3 + 2 + 1 + 2 + 4 + 2 + keyLen.ToString().Length + 2 + keyLen + 2,  // *2\r\n$4\r\nINCR\r\n$KLen\r\nK\r\n
                OpType.DEL => 3 + 2 + 1 + 2 + 3 + 2 + keyLen.ToString().Length + 2 + keyLen + 2,  // *2\r\n$3\r\nDEL\r\n$KLen\r\nK\r\n
                _ => 3 + 2 + 1 + 2 + 3 + 2 + keyLen.ToString().Length + 2 + keyLen + 2,  // Default to GET
            };
        }

        /// <summary>
        /// Calculate approximate RESP protocol bytes for a response (received bytes).
        /// For MGET: *N\r\n$VLen\r\nV\r\n... (N values) - approximate as N × ($VLen\r\nV\r\n) + array header
        /// For MSET: +OK\r\n
        /// For GET: $VLen\r\nV\r\n
        /// For SET: +OK\r\n
        /// </summary>
        private int CalculateRespReceivedBytes(OpType op, int keyCount)
        {
            var valLen = Math.Max(opts.ValueLength, 8);

            return op switch
            {
                OpType.GET => valLen.ToString().Length + 2 + 1 + valLen + 2,  // $VLen\r\nV\r\n
                OpType.SET => 5,  // +OK\r\n
                OpType.MGET => keyCount.ToString().Length + 2 + 1 + keyCount * (valLen.ToString().Length + 2 + 1 + valLen + 2),  // *N\r\n + N × ($VLen\r\nV\r\n)
                OpType.MSET => 5,  // +OK\r\n
                OpType.INCR => 1 + 2 + 2,  // :1\r\n (approximate)
                OpType.DEL => 1 + 2 + 2,  // :1\r\n (approximate)
                _ => valLen.ToString().Length + 2 + 1 + valLen + 2,  // Default to GET
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (int, int) OnResponse(byte* buf, int bytesRead, int opType)
        {
            var count = 0;
            switch ((OpType)opType)
            {
                case OpType.GET:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == '$') count++;
                    break;
                case OpType.SET:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == '+') count++;
                    break;
                case OpType.MGET:
                    // MGET returns array: *N\r\n$Len\r\nVal\r\n...\r\n
                    // Count '*' to count responses (each MGET command returns one array)
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == '*') count++;
                    break;
                case OpType.MSET:
                    // MSET returns simple string: +OK\r\n
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == '+') count++;
                    break;
                case OpType.DEL:
                case OpType.INCR:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                default:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] is (byte)'+' or (byte)'$' or (byte)':' or (byte)'*') count++;
                    break;
            }
            return (bytesRead, count);
        }
    }
}