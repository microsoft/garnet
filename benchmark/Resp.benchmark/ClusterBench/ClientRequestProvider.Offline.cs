// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Garnet.common;

namespace Resp.benchmark
{
    public unsafe partial class ClientRequestProvider
    {
        // Pre-generated buffers for offline mode
        byte[][] requestBuffers;
        int[] requestLengths;
        int batchCount;

        /// <summary>
        /// Pre-generate request buffers for offline mode.
        /// Each buffer contains a batch of commands with keys restricted to this shard's slots.
        /// </summary>
        public void PrepareOfflineBuffers()
        {
            var dbSizePerThread = opts.DbSize / opts.NumThreads.First();
            var batchSize = opts.BatchSize.First();
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
            var dbSizePerThread = opts.DbSize / opts.NumThreads.First();
            var batchSize = Math.Min(256, dbSizePerThread);

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

            var loaded = 0;
            while (loaded < dbSizePerThread)
            {
                var thisBatch = Math.Min(batchSize, dbSizePerThread - loaded);
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
    }
}
