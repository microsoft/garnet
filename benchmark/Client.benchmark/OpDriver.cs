// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Text;
using Garnet.client;
using LightEpoch = Garnet.client.LightEpoch;

namespace Client.benchmark
{
    /// <summary>
    /// A benchmark arm connection: wraps a single connected client instance and exposes a uniform,
    /// allocation-light way to seed a key and issue the measured operation. Implemented as
    /// a <see langword="readonly struct"/> so the workload runner can be generic over the
    /// concrete driver (<c>where TDriver : struct, IOpDriver</c>) and dispatch the per-op
    /// call through a constrained (devirtualized) invocation rather than an interface call.
    /// Each worker thread owns its own driver (its own client connection); the clients are not
    /// safe for concurrent use from multiple threads, so a per-thread connection is the realistic
    /// multi-threaded topology. All connections of a run share one <see cref="LightEpoch"/>.
    /// </summary>
    internal interface IOpDriver : IDisposable
    {
        /// <summary>Establishes the client connection to the server.</summary>
        void Connect();

        /// <summary>Seeds the key read by <see cref="IssueAsync"/> (not measured).</summary>
        Task SeedAsync();

        /// <summary>Issues a single measured operation and completes when its reply is parsed.</summary>
        Task IssueAsync();
    }

    /// <summary>
    /// Creates per-thread <typeparamref name="TDriver"/> instances that share one epoch. The factory
    /// is a <see langword="struct"/> so the runner stays generic over the concrete driver type and keeps
    /// the per-op call devirtualized; <see cref="Create"/> runs once per worker (off the hot path).
    /// </summary>
    internal interface IDriverFactory<out TDriver>
        where TDriver : struct, IOpDriver
    {
        /// <summary>Creates one unconnected driver whose client shares <paramref name="epoch"/>.</summary>
        TDriver Create(LightEpoch epoch);
    }

    /// <summary>
    /// Driver for the original <see cref="GarnetClient"/> (inline send path). The measured
    /// operation is a GET of the seeded key.
    /// </summary>
    internal readonly struct GarnetClientDriver : IOpDriver
    {
        readonly GarnetClient client;
        readonly string key;
        readonly string value;

        public GarnetClientDriver(EndPoint endpoint, string key, string value, BenchmarkOptions opts, LightEpoch epoch)
        {
            client = new GarnetClient(
                endpoint,
                bufferSize: opts.NetworkBufferBytes,
                networkSendThrottleMax: opts.MaxConcurrentSends,
                epoch: epoch);
            this.key = key;
            this.value = value;
        }

        public void Connect() => client.Connect();

        public Task SeedAsync() => client.StringSetAsync(key, value);

        public Task IssueAsync() => client.StringGetAsync(key);

        public void Dispose() => client.Dispose();
    }

    /// <summary>
    /// Factory producing per-thread <see cref="GarnetClientDriver"/> instances sharing one epoch.
    /// </summary>
    internal readonly struct GarnetClientDriverFactory : IDriverFactory<GarnetClientDriver>
    {
        readonly EndPoint endpoint;
        readonly string key;
        readonly string value;
        readonly BenchmarkOptions opts;

        public GarnetClientDriverFactory(EndPoint endpoint, string key, string value, BenchmarkOptions opts)
        {
            this.endpoint = endpoint;
            this.key = key;
            this.value = value;
            this.opts = opts;
        }

        public GarnetClientDriver Create(LightEpoch epoch) => new(endpoint, key, value, opts, epoch);
    }

    /// <summary>
    /// Driver for the <see cref="GarnetLightClient"/> (duplex-ring send path). The measured
    /// operation is a GET of the seeded key, issued through the raw RESP execute API so the
    /// wire format is identical to <see cref="GarnetClientDriver"/>.
    /// </summary>
    internal readonly struct GarnetLightClientDriver : IOpDriver
    {
        static readonly Memory<byte> GET = "$3\r\nGET\r\n"u8.ToArray();
        static readonly Memory<byte> SET = "$3\r\nSET\r\n"u8.ToArray();

        readonly GarnetLightClient client;
        readonly Memory<byte>[] getArgs;
        readonly Memory<byte>[] setArgs;

        public GarnetLightClientDriver(EndPoint endpoint, string key, string value, BenchmarkOptions opts, LightEpoch epoch)
        {
            // Both duplex rings must hold the offered in-flight load (threads*batch) or producers stall.
            // The benchmark sizes them from offered load by default so GarnetLightClient matches GarnetClient
            // out of the box; LightNetworkWriterOptions keeps its own small library defaults untouched.
            //
            // The inline request buffer is sized in BYTES to hold the offered load with headroom (mirroring
            // GarnetClient's byte-addressed send buffer). Request and completion capacities count requests,
            // one per in-flight operation regardless of size; --max-requests and --max-completions override them.
            const int requestPageCount = 4;
            const int bytesPerInflightRecord = 64;      // small inline GET/SET payload, with headroom
            const int maxRequestIds = 1 << 15;          // LightPageAndOffset request-id space
            var offered = Math.Max(1, opts.Threads * opts.Batch);

            var targetRingBytes = Math.Max((long)opts.NetworkBufferBytes, (long)offered * bytesPerInflightRecord);
            var requestPageSizeBytes = 1 << 13;
            while ((long)requestPageSizeBytes * requestPageCount < targetRingBytes)
                requestPageSizeBytes <<= 1;

            var maxOutstandingRequests = opts.MaxOutstandingRequests > 0
                ? opts.MaxOutstandingRequests
                : Math.Min(maxRequestIds, Math.Max(512, offered * 2));

            // One completion slot per in-flight reply; hold the offered load with headroom by default so
            // replies are never completion-throttled.
            var maxOutstandingCompletions = opts.MaxOutstandingCompletions > 0
                ? opts.MaxOutstandingCompletions
                : Math.Max(512, offered * 2);

            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: opts.NetworkBufferBytes,
                requestPageSizeBytes: requestPageSizeBytes,
                requestPageCount: requestPageCount,
                maxOutstandingRequests: maxOutstandingRequests,
                maxOutstandingCompletions: maxOutstandingCompletions,
                maxConcurrentNetworkSends: opts.MaxConcurrentSends,
                maxOutOfLineRentedBytes: 64L << 20,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            client = new GarnetLightClient(endpoint, networkWriterOptions: options, epoch: epoch);
            var keyBytes = Encoding.UTF8.GetBytes(key);
            var valueBytes = Encoding.UTF8.GetBytes(value);
            getArgs = [keyBytes];
            setArgs = [keyBytes, valueBytes];
        }

        public void Connect() => client.Connect();

        public Task SeedAsync() => client.ExecuteForStringResultAsync(SET, setArgs);

        public Task IssueAsync() => client.ExecuteForStringResultAsync(GET, getArgs);

        public void Dispose() => client.Dispose();
    }

    /// <summary>
    /// Factory producing per-thread <see cref="GarnetLightClientDriver"/> instances sharing one epoch.
    /// </summary>
    internal readonly struct GarnetLightClientDriverFactory : IDriverFactory<GarnetLightClientDriver>
    {
        readonly EndPoint endpoint;
        readonly string key;
        readonly string value;
        readonly BenchmarkOptions opts;

        public GarnetLightClientDriverFactory(EndPoint endpoint, string key, string value, BenchmarkOptions opts)
        {
            this.endpoint = endpoint;
            this.key = key;
            this.value = value;
            this.opts = opts;
        }

        public GarnetLightClientDriver Create(LightEpoch epoch) => new(endpoint, key, value, opts, epoch);
    }
}