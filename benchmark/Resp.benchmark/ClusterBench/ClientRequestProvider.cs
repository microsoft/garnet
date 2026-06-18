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
        long keysLoaded;
        long primaryOps;  // Operations executed on primary
        long replicaOps;  // Operations executed on replica

        volatile bool done;

        public long OpsCompleted => Interlocked.Read(ref opsCompleted);
        public long BytesSent => Interlocked.Read(ref bytesSent);
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
            if (opts.ReplicaReadPercent == 0)
                return false;

            // Write operations always go to primary (replicas are read-only)
            if (OperationClassifier.IsWriteOperation(op))
                return false;

            // Read operations: probabilistic routing based on percentage
            if (OperationClassifier.IsReadOperation(op))
            {
                // If percentage is 100, always use replica
                if (opts.ReplicaReadPercent >= 100)
                    return true;

                // Probabilistic: generate random number 0-99, compare with percentage
                // This ensures approximately X% of reads go to replicas across all clients
                var randomValue = rng.Next(100);
                return randomValue < opts.ReplicaReadPercent;
            }

            // Unknown operation type: default to primary (safe default)
            return false;
        }

        /// <summary>
        /// Signal this provider to stop.
        /// </summary>
        public void Stop() => done = true;

        public void Dispose()
        {
            requestBuffers = null;
            requestLengths = null;
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
                _ => Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n"),
            };
        }

        private void AppendCommand(StringBuilder sb, OpType op, string key)
        {
            switch (op)
            {
                case OpType.GET:
                    sb.Append($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n");
                    break;
                case OpType.SET:
                    var value = GenerateValue();
                    sb.Append($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n");
                    break;
                case OpType.INCR:
                    sb.Append($"*2\r\n$4\r\nINCR\r\n${key.Length}\r\n{key}\r\n");
                    break;
                case OpType.DEL:
                    sb.Append($"*2\r\n$3\r\nDEL\r\n${key.Length}\r\n{key}\r\n");
                    break;
                default:
                    sb.Append($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n");
                    break;
            }
        }

        private string GenerateValue()
        {
            return new string('v', opts.ValueLength == 0 ? 8 : opts.ValueLength);
        }

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
                case OpType.DEL:
                case OpType.INCR:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                default:
                    for (var i = 0; i < bytesRead; i++)
                        if (buf[i] is (byte)'+' or (byte)'$' or (byte)':') count++;
                    break;
            }
            return (bytesRead, count);
        }
    }
}