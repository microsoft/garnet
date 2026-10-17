// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Which load-generation model to run.
    /// </summary>
    public enum RunMode
    {
        /// <summary>
        /// Reproduces the two-VM experiment: N mock service instances, each with its own
        /// connection, each running three periodic background jobs (probe 2 s, endpoint sync 250 ms,
        /// tenant sync 5 s). Offered load is fixed by the periods, not by the server's capacity.
        /// </summary>
        Periodic,

        /// <summary>
        /// Open-loop load at a fixed target rate, which is how the production fleet is actually loaded
        /// (~9 K ops/sec per instance on average, ~22 K ops/sec on the hottest instance).
        /// </summary>
        Rate,

        /// <summary>
        /// Closed-loop saturation: every connection keeps a fixed number of operations in flight.
        /// Use this to find the capacity knee and the latency at saturation.
        /// </summary>
        Saturate,
    }

    /// <summary>
    /// Client library used to drive the workload.
    /// </summary>
    public enum ClientKind
    {
        /// <summary>StackExchange.Redis, the library a .NET service would normally use.</summary>
        SERedis,

        /// <summary>Garnet's own async client.</summary>
        GarnetClient,
    }

    /// <summary>
    /// Command line options for the rate-limiter cache workload simulator.
    /// </summary>
    public sealed class Options
    {
        /// <summary>Server host name or address.</summary>
        [Option("host", Default = "127.0.0.1", HelpText = "Server host.")]
        public string Host { get; set; }

        /// <summary>Server port.</summary>
        [Option("port", Default = 6379, HelpText = "Server port.")]
        public int Port { get; set; }

        /// <summary>Whether to connect over TLS.</summary>
        [Option("tls", Default = false, HelpText = "Connect over TLS. Managed caches generally require it, so a no-TLS run models neither server as deployed.")]
        public bool Tls { get; set; }

        /// <summary>TLS target host name presented during the handshake.</summary>
        [Option("tls-host", Default = "localhost", HelpText = "TLS target host name used for SNI and certificate validation.")]
        public string TlsHost { get; set; }

        /// <summary>Load generation model.</summary>
        [Option("mode", Default = RunMode.Periodic, HelpText = "Periodic (reproduces the two-VM experiment), Rate (open loop at --ops), or Saturate (closed loop).")]
        public RunMode Mode { get; set; }

        /// <summary>Client library.</summary>
        [Option("client", Default = ClientKind.SERedis, HelpText = "SERedis or GarnetClient.")]
        public ClientKind Client { get; set; }

        /// <summary>Number of mock service instances / connections.</summary>
        [Option("instances", Default = 50, HelpText = "Mock service instances; each gets its own connection.")]
        public int Instances { get; set; }

        /// <summary>Run duration in seconds.</summary>
        [Option("runtime", Default = 60, HelpText = "Measurement duration in seconds.")]
        public int Runtime { get; set; }

        /// <summary>Warmup duration in seconds, excluded from the reported histograms.</summary>
        [Option("warmup", Default = 5, HelpText = "Warmup seconds, excluded from the reported histograms.")]
        public int Warmup { get; set; }

        /// <summary>
        /// Issue every operation with a blocking call rather than awaiting it. This is what naive
        /// synchronous service code does, and it is the single biggest source of client-side
        /// measurement error in a periodic harness.
        /// </summary>
        [Option("sync", Default = false, HelpText = "Use blocking calls instead of async awaits.")]
        public bool Sync { get; set; }

        /// <summary>
        /// Give each periodic job its own dedicated OS thread instead of running it on the thread pool.
        /// </summary>
        [Option("dedicated-threads", Default = false, HelpText = "Periodic mode: one dedicated OS thread per job instead of the thread pool.")]
        public bool DedicatedThreads { get; set; }

        /// <summary>Thread pool minimum worker thread count; 0 leaves the runtime default.</summary>
        [Option("min-threads", Default = 0, HelpText = "ThreadPool.SetMinThreads worker count; 0 leaves the runtime default.")]
        public int MinThreads { get; set; }

        /// <summary>Target aggregate operations per second for <see cref="RunMode.Rate"/>.</summary>
        [Option("ops", Default = 20000, HelpText = "Rate mode: target aggregate operations per second.")]
        public int Ops { get; set; }

        /// <summary>Operations kept in flight per connection for <see cref="RunMode.Saturate"/>.</summary>
        [Option("itp", Default = 8, HelpText = "Saturate mode: operations in flight per connection.")]
        public int InFlightPerConnection { get; set; }

        /// <summary>Probability that a scripted operation is the HGET + HSET + EXPIRE variant.</summary>
        [Option("rw-ratio", Default = 0.2, HelpText = "Fraction of scripted ops that are HGET+HSET+EXPIRE; the rest are HGET only.")]
        public double ReadWriteRatio { get; set; }

        /// <summary>Probability that an operation is scripted at all.</summary>
        [Option("eval-ratio", Default = 0.97, HelpText = "Fraction of ops issued as EVALSHA; the rest are plain GET.")]
        public double EvalRatio { get; set; }

        /// <summary>Send scripts with EVAL instead of EVALSHA.</summary>
        [Option("eval-full", Default = false, HelpText = "Send the script body with EVAL on every call instead of EVALSHA.")]
        public bool EvalFull { get; set; }

        /// <summary>Distinct endpoint bucket hashes.</summary>
        [Option("endpoints", Default = 10000, HelpText = "Distinct endpoint bucket keys.")]
        public int Endpoints { get; set; }

        /// <summary>Distinct tenant bucket hashes.</summary>
        [Option("tenants", Default = 1000, HelpText = "Distinct tenant bucket keys.")]
        public int Tenants { get; set; }

        /// <summary>Fields per bucket hash.</summary>
        [Option("fields", Default = 8, HelpText = "Fields per bucket hash.")]
        public int Fields { get; set; }

        /// <summary>Bucket value length in bytes.</summary>
        [Option("valuelength", Default = 32, HelpText = "Bucket value length in bytes.")]
        public int ValueLength { get; set; }

        /// <summary>TTL applied by the read-write script, in seconds.</summary>
        [Option("ttl", Default = 300, HelpText = "TTL in seconds applied by the read-write script.")]
        public int Ttl { get; set; }

        /// <summary>Probe job period in milliseconds.</summary>
        [Option("probe-period", Default = 2000, HelpText = "Periodic mode: probe job period in ms.")]
        public int ProbePeriodMs { get; set; }

        /// <summary>Endpoint bucket sync job period in milliseconds.</summary>
        [Option("endpoint-period", Default = 250, HelpText = "Periodic mode: endpoint bucket sync period in ms.")]
        public int EndpointPeriodMs { get; set; }

        /// <summary>Tenant bucket sync job period in milliseconds.</summary>
        [Option("tenant-period", Default = 5000, HelpText = "Periodic mode: tenant bucket sync period in ms.")]
        public int TenantPeriodMs { get; set; }

        /// <summary>Skip populating the key space.</summary>
        [Option("skip-load", Default = false, HelpText = "Skip populating the bucket key space.")]
        public bool SkipLoad { get; set; }

        /// <summary>Label printed with the results.</summary>
        [Option("label", Default = "", HelpText = "Label printed with the result rows.")]
        public string Label { get; set; }

        /// <summary>Emit one machine-readable summary line per run.</summary>
        [Option("csv", Default = false, HelpText = "Emit a machine-readable CSV summary line.")]
        public bool Csv { get; set; }
    }
}