// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using HdrHistogram;

namespace Resp.benchmark
{
    /// <summary>
    /// A single-threaded worker that owns a connection to one shard,
    /// generates keys restricted to that shard's assigned slots,
    /// and measures its own throughput and latency.
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

        // Metrics
        readonly LongHistogram histogram;
        long opsCompleted;
        long bytesSent;
        long keysLoaded;

        volatile bool done;

        public long OpsCompleted => Interlocked.Read(ref opsCompleted);
        public long BytesSent => Interlocked.Read(ref bytesSent);
        public long KeysLoaded => Interlocked.Read(ref keysLoaded);
        public LongHistogram Histogram => histogram;
        public PrimaryInfo Shard => shard;
        public string KeyPrefix => keyGen.KeyPrefix;

        /// <summary>
        /// Index of this thread within its shard (0-based). Used to partition key space during loading.
        /// </summary>
        private readonly int shardThreadIndex;

        public ClientRequestProvider(PrimaryInfo shard, Options opts, int threadIndex, int shardThreadIndex)
        {
            this.shard = shard;
            this.opts = opts;
            this.threadIndex = threadIndex;
            this.shardThreadIndex = shardThreadIndex;
            this.rng = new Random(31337 + (threadIndex * 1000) + shard.Port);
            this.keyGen = new SlotKeyGenerator(shard, opts.KeyLength);
            this.histogram = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);
        }

        /// <summary>
        /// Signal this provider to stop.
        /// </summary>
        public void Stop() => done = true;

        public void Dispose()
        {
            requestBuffers = null;
            requestLengths = null;
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
            int count = 0;
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
