// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Garnet.client;
using Garnet.common;
using HdrHistogram;

namespace Resp.benchmark
{
    /// <summary>
    /// A single-threaded worker that owns a connection to one shard,
    /// generates keys restricted to that shard's assigned slots,
    /// and measures its own throughput and latency.
    /// </summary>
    public unsafe class ClientRequestProvider : IDisposable
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        readonly ShardInfo shard;
        readonly Options opts;
        readonly int threadIndex;
        readonly SlotKeyGenerator keyGen;
        readonly Random rng;

        // Metrics
        readonly LongHistogram histogram;
        long opsCompleted;

        // Pre-generated buffers for offline mode
        byte[][] requestBuffers;
        int[] requestLengths;
        int batchCount;

        volatile bool done;

        public long OpsCompleted => Interlocked.Read(ref opsCompleted);
        public LongHistogram Histogram => histogram;
        public ShardInfo Shard => shard;

        public ClientRequestProvider(ShardInfo shard, Options opts, int threadIndex)
        {
            this.shard = shard;
            this.opts = opts;
            this.threadIndex = threadIndex;
            this.rng = new Random(31337 + threadIndex * 1000 + shard.Port);
            this.keyGen = new SlotKeyGenerator(shard, opts.KeyLength);
            this.histogram = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);
        }

        /// <summary>
        /// Pre-generate request buffers for offline mode.
        /// Each buffer contains a batch of commands with keys restricted to this shard's slots.
        /// </summary>
        public void PrepareOfflineBuffers()
        {
            int dbSizePerThread = opts.DbSize / opts.NumThreads.First();
            int batchSize = opts.BatchSize.First();
            batchCount = Math.Max(1, dbSizePerThread / batchSize);

            requestBuffers = new byte[batchCount][];
            requestLengths = new int[batchCount];

            for (int b = 0; b < batchCount; b++)
            {
                var buffer = GenerateBatch(batchSize, b * batchSize);
                requestBuffers[b] = buffer;
                requestLengths[b] = buffer.Length;
            }
        }

        /// <summary>
        /// Load data into the shard for this provider's key space.
        /// </summary>
        public void LoadData()
        {
            int dbSizePerThread = opts.DbSize / opts.NumThreads.First();
            int batchSize = Math.Min(256, dbSizePerThread);

            var endpoint = new IPEndPoint(IPAddress.Parse(shard.Address), shard.Port);
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)OpType.SET,
                onResponse,
                batchSize,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            int loaded = 0;
            while (loaded < dbSizePerThread)
            {
                int thisBatch = Math.Min(batchSize, dbSizePerThread - loaded);
                var buffer = GenerateSetBatch(thisBatch, loaded);

                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, buffer.Length, thisBatch);
                    client.CompletePendingRequests();
                }
                loaded += thisBatch;
            }

            Console.WriteLine($"  Shard {shard.Address}:{shard.Port} thread[{threadIndex}]: loaded {loaded} keys");
        }

        /// <summary>
        /// Run offline benchmark: send pre-generated batches and measure throughput/latency.
        /// </summary>
        public void RunOffline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            if (requestBuffers == null)
                throw new InvalidOperationException("Must call PrepareOfflineBuffers() before RunOffline()");

            var endpoint = new IPEndPoint(IPAddress.Parse(shard.Address), shard.Port);
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)opts.Op,
                onResponse,
                opts.BatchSize.First(),
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            // Wait for start signal
            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            int batchIdx = 0;
            int batchSize = opts.BatchSize.First();

            while (!done && sw.Elapsed < runTime)
            {
                var buffer = requestBuffers[batchIdx % batchCount];
                var len = requestLengths[batchIdx % batchCount];

                var opStart = Stopwatch.GetTimestamp();

                fixed (byte* bufPtr = buffer)
                {
                    client.Send(bufPtr, len, batchSize);
                    client.CompletePendingRequests();
                }

                var elapsed = Stopwatch.GetTimestamp() - opStart;

                if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                    histogram.RecordValue(elapsed);

                Interlocked.Add(ref opsCompleted, batchSize);
                batchIdx++;
            }
        }

        /// <summary>
        /// Run online benchmark: generate and send requests on-the-fly.
        /// </summary>
        public void RunOnline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var endpoint = new IPEndPoint(IPAddress.Parse(shard.Address), shard.Port);

            switch (opts.Client)
            {
                case ClientType.LightClient:
                    RunOnlineLightClient(endpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClientSession:
                    RunOnlineGarnetClientSession(endpoint, startSignal, runTime);
                    break;
                case ClientType.GarnetClient:
                    RunOnlineGarnetClient(endpoint, startSignal, runTime);
                    break;
                default:
                    throw new NotSupportedException($"Client type {opts.Client} not supported in cluster bench mode.");
            }
        }

        private void RunOnlineLightClient(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnResponse);

            using var client = new LightClient(
                endpoint,
                (int)opts.Op,
                onResponse,
                1,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            int keyIdx = threadIndex * 100_000;

            while (!done && sw.Elapsed < runTime)
            {
                var key = keyGen.GenerateKey(rng, keyIdx++);
                var request = FormatRequest(opts.Op, key);

                var opStart = Stopwatch.GetTimestamp();

                fixed (byte* bufPtr = request)
                {
                    client.Send(bufPtr, request.Length, 1);
                    client.CompletePendingRequests();
                }

                var elapsed = Stopwatch.GetTimestamp() - opStart;

                if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                    histogram.RecordValue(elapsed);

                Interlocked.Increment(ref opsCompleted);
            }
        }

        private void RunOnlineGarnetClientSession(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            using var client = new GarnetClientSession(
                endpoint,
                new(),
                tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();

            if (opts.Auth != null)
            {
                client.Execute("AUTH", opts.Auth);
                client.CompletePending();
            }

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            int keyIdx = threadIndex * 100_000;
            int itp = opts.IntraThreadParallelism;

            while (!done && sw.Elapsed < runTime)
            {
                var opStart = Stopwatch.GetTimestamp();

                for (int p = 0; p < itp; p++)
                {
                    var key = keyGen.GenerateKey(rng, keyIdx++);

                    if (opts.Op == OpType.GET)
                        client.Execute("GET", key);
                    else if (opts.Op == OpType.SET)
                        client.Execute("SET", key, GenerateValue());
                    else
                        client.Execute("GET", key);
                }

                client.CompletePending();

                var elapsed = Stopwatch.GetTimestamp() - opStart;

                if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                    histogram.RecordValue(elapsed);

                Interlocked.Add(ref opsCompleted, itp);
            }
        }

        private void RunOnlineGarnetClient(IPEndPoint endpoint, ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            var client = new GarnetClient(
                endpoint,
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                recordLatency: opts.ClientHistogram);

            client.Connect();

            if (opts.Auth != null)
                client.ExecuteForStringResultAsync("AUTH", [opts.Auth]).GetAwaiter().GetResult();

            startSignal.Wait();

            var sw = Stopwatch.StartNew();
            int keyIdx = threadIndex * 100_000;
            int itp = opts.IntraThreadParallelism;

            try
            {
                while (!done && sw.Elapsed < runTime)
                {
                    var opStart = Stopwatch.GetTimestamp();
                    var tasks = new Task[itp];

                    for (int p = 0; p < itp; p++)
                    {
                        var key = keyGen.GenerateKey(rng, keyIdx++);

                        if (opts.Op == OpType.GET)
                            tasks[p] = client.StringGetAsMemoryAsync(key);
                        else if (opts.Op == OpType.SET)
                            tasks[p] = client.StringSetAsync(key, GenerateValue());
                        else
                            tasks[p] = client.StringGetAsMemoryAsync(key);
                    }

                    Task.WaitAll(tasks);

                    var elapsed = Stopwatch.GetTimestamp() - opStart;

                    if (elapsed > HISTOGRAM_LOWER_BOUND && elapsed < HISTOGRAM_UPPER_BOUND)
                        histogram.RecordValue(elapsed);

                    Interlocked.Add(ref opsCompleted, itp);
                }
            }
            finally
            {
                client.Dispose();
            }
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

        private byte[] GenerateBatch(int batchSize, int startKeyIndex)
        {
            var sb = new StringBuilder();

            for (int i = 0; i < batchSize; i++)
            {
                var key = keyGen.GenerateKey(rng, startKeyIndex + i);
                AppendCommand(sb, opts.Op, key);
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        private byte[] GenerateSetBatch(int batchSize, int startKeyIndex)
        {
            var sb = new StringBuilder();

            for (int i = 0; i < batchSize; i++)
            {
                var key = keyGen.GenerateKey(rng, startKeyIndex + i);
                var value = GenerateValue();
                sb.Append($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n");
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
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
            // Count responses based on operation type
            int count = 0;
            switch ((OpType)opType)
            {
                case OpType.GET:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '$') count++;
                    break;
                case OpType.SET:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '+') count++;
                    break;
                default:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '+' || buf[i] == '$' || buf[i] == ':') count++;
                    break;
            }
            return (count, 0);
        }
    }
}
