// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
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
    }
}