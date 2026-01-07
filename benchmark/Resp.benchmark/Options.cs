// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    public partial class Options
    {
        [Option('p', "port", Required = false, Default = 6379, HelpText = "Port to connect to")]
        public int Port { get; set; }

        [Option('h', "host", Required = false, Default = "127.0.0.1", HelpText = "IP address to connect to")]
        public string Address { get; set; }

        [Option("clientaddr", Required = false, HelpText = "IP address of client")]
        public string ClientAddress { get; set; }

        [Option('s', "skipload", Required = false, Default = false, HelpText = "Skip loading phase")]
        public bool SkipLoad { get; set; }

        [Option("dbsize", Required = false, Default = 1 << 10, HelpText = "DB size")]
        public int DbSize { get; set; }

        [Option("totalops", Required = false, Default = 1 << 25, HelpText = "Total ops")]
        public int TotalOps { get; set; }

        [Option("op", Required = false, Default = OpType.GET, HelpText = "Operation type (GET, MGET, INCR, PING, ZADDREM, PFADD, ZADDCARD)")]
        public OpType Op { get; set; }

        [Option("keylength", Required = false, Default = 1, HelpText = "Key length (bytes) - padded, 0 indicates pad to max DB size")]
        public int KeyLength { get; set; }

        [Option("valuelength", Required = false, Default = 8, HelpText = "Value length (bytes) - 0 indicates use key as value")]
        public int ValueLength { get; set; }

        [Option('b', "batchsize", Separator = ',', Required = false, Default = new[] { 4096 }, HelpText = "Batch size, number of requests (comma separated)")]
        public IEnumerable<int> BatchSize { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time (seconds)")]
        public int RunTime { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }

        [Option('a', "auth", Required = false, Default = null, HelpText = "Authentication password")]
        public string Auth { get; set; }

        [Option("burst", Required = false, Default = false, HelpText = "Wait for response or burst the system (GarnetClientSession)")]
        public bool Burst { get; set; }

        [Option("lset", Required = false, Default = false, HelpText = "Use set instead of mset to load data for benchmarking.")]
        public bool LSet { get; set; }

        [Option("zipf", Required = false, Default = false, HelpText = "Zipf data distribution (0.99)")]
        public bool Zipf { get; set; }

        [Option("client", Required = false, Default = ClientType.LightClient, HelpText = "Choose ClientType to run benchmark (LightClient, SERedis, GarnetClientSession)")]
        public ClientType Client { get; set; }

        [Option("pool", Required = false, Default = false, HelpText = "Pool client instances. Supports SERedis, GarnetClient and GarnetClientSession (online bench only).")]
        public bool Pool { get; set; }

        [Option("tls", Required = false, Default = false, HelpText = "Enable TLS.")]
        public bool EnableTLS { get; set; }

        [Option("tlshost", Required = false, Default = "GarnetTest", HelpText = "TLS remote host name.")]
        public string TlsHost { get; set; }

        [Option("cert-file-name", Required = false, HelpText = "TLS certificate file name (example: testcert.pfx).")]
        public string CertFileName { get; set; }

        [Option("cert-password", Required = false, HelpText = "TLS certificate password (example: placeholder).")]
        public string CertPassword { get; set; }

        [Option('o', "online", Required = false, Default = false, HelpText = "Online get/set mix based on --readpercent.")]
        public bool Online { get; set; }

        [Option('x', "txn", Required = false, Default = false, HelpText = "Transaction micro benchmark")]
        public bool Txn { get; set; }

        [Option("itp", Required = false, Default = 1, HelpText = "Intra-thread parallelism (online bench only).")]
        public int IntraThreadParallelism { get; set; }

        [Option("sync", Required = false, Default = false, HelpText = "Sync mode (online bench GarnetClient only).")]
        public bool SyncMode { get; set; }

        [Option("ttl", Required = false, Default = 0, HelpText = "Ttl for keys, required if --op is SETEX, or can be used to generate keys with expiration (both string and zset) in online benchmarks. ")]
        public int Ttl { get; set; }

        [Option("sscardinality", Required = false, Default = 0, HelpText = "Number of unique sorted sets. Same key will always go to the same sorted set.")]
        public int SortedSetCardinality { get; set; }

        [Option("client-hist", Required = false, Default = false, HelpText = "Enable client side latency tracking through internal client histogram.")]
        public bool ClientHistogram { get; set; }

        [Option("op-percent", Separator = ',', Default = new[] { 60, 30, 10 }, HelpText = "Percent of commands executed from workload")]
        public IEnumerable<int> OpPercent { get; set; }

        [Option("op-workload", Separator = ',', Default = new[] { OpType.GET, OpType.SET, OpType.DEL }, HelpText = "Workload of commands for online bench.")]
        public IEnumerable<OpType> OpWorkload { get; set; }

        [Option("save-freq", Required = false, Default = 0, HelpText = "Save (checkpoint) frequency in seconds")]
        public int SaveFreqSecs { get; set; }

        [Option("logger-level", Required = false, Default = LogLevel.Information, HelpText = "Logging level")]
        public LogLevel LogLevel { get; set; }

        [Option("disable-console-logger", Required = false, Default = false, HelpText = "Disable console logger.")]
        public bool DisableConsoleLogger { get; set; }

        [Option("file-logger", Required = false, Default = null, HelpText = "Enable file logger and write to the specified path.")]
        public string FileLogger { get; set; }

        [Option("aof-bench", Required = false, Default = false, HelpText = "Run AOF bench at replica.")]
        public bool AofBench { get; set; }

        [Option("aof-bench-type", Required = false, Default = AofBenchType.Replay, HelpText = "Run AOF bench at replica.")]
        public AofBenchType AofBenchType { get; set; }

        /*
         * InProc/AofBench server options
         */
        [Option("aof", Required = false, Default = false, HelpText = "Enable AOF")]
        public bool EnableAOF { get; set; }

        [Option("cluster", Required = false, Default = false, HelpText = "Enable Cluster")]
        public bool EnableCluster { get; set; }

        [Option('i', "index", Required = false, Default = "1g", HelpText = "Start size of hash index in bytes (rounds down to power of 2)")]
        public string IndexSize { get; set; }

        [Option("aof-null-device", Required = false, HelpText = "With main-memory replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication.")]
        public bool UseAofNullDevice { get; set; }

        [Option("aof-commit-freq", Required = false, Default = 0, HelpText = "Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command")]
        public int CommitFrequencyMs { get; set; }

        [Option("aof-physical-sublog-count", Required = false, Default = 1, HelpText = "Number of sublogs used for AOF.")]
        public int AofPhysicalSublogCount { get; set; }

        [Option("aof-memory-size", Required = false, Default = "64m", HelpText = "Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit.")]
        public string AofMemorySize { get; set; }

        [Option("aof-page-size", Required = false, Default = "4m", HelpText = "Size of each AOF page in bytes(rounds down to power of 2)")]
        public string AofPageSize { get; set; }

        /// <summary>
        /// Parse size from string specification
        /// </summary>
        /// <param name="value"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        public static long ParseSize(string value, out int bytesRead)
        {
            ReadOnlySpan<char> suffix = ['k', 'm', 'g', 't', 'p'];
            long result = 0;
            bytesRead = 0;
            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (char.IsDigit(c))
                {
                    result = (result * 10) + (byte)c - '0';
                    bytesRead++;
                }
                else
                {
                    for (var s = 0; s < suffix.Length; s++)
                    {
                        if (char.ToLower(c) == suffix[s])
                        {
                            result *= (long)Math.Pow(1024, s + 1);
                            bytesRead++;

                            if (i + 1 < value.Length && char.ToLower(value[i + 1]) == 'b')
                                bytesRead++;

                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get AOF Page size in bits
        /// </summary>
        /// <returns></returns>
        public int AofPageSizeBits()
        {
            var size = ParseSize(AofPageSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Previous power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }
    }
}