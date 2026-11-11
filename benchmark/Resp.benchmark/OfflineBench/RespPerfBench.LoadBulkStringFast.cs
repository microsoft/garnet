// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Numerics;
using System.Text;
using Garnet.client;
using Garnet.common;

namespace Resp.benchmark
{
    public partial class RespPerfBench
    {
        const int FastLoadStampLength = 8;

        /// <summary>
        /// Validate the options supported by the streaming RESP loader.
        /// </summary>
        /// <param name="options">Benchmark options.</param>
        internal static void ValidateFastLoad(Options options)
        {
            if (options.ClusterBench || options.Online || options.Txn || options.AofBench ||
                options.Client == ClientType.InProc ||
                options.Op is not (OpType.GET or OpType.SET or OpType.MGET or OpType.MSET))
                throw new ArgumentException("--fast-load supports offline network RESP string benchmarks only.");
            if (options.LoadThreads < 1 || options.DbSize < 1 || options.KeyLength < 0 ||
                (options.ValueLength != 0 && options.ValueLength < FastLoadStampLength))
                throw new ArgumentException("--fast-load requires positive --load-threads and --dbsize, nonnegative --keylength, and --valuelength of at least 8 (or 0 for the default).");
        }

        void LoadDataFast(int loadThreads, int batchSize, int keyLength, int valueLength, bool numericValue)
        {
            ValidateFastLoad(opts);
            ArgumentOutOfRangeException.ThrowIfLessThan(loadThreads, 1);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            ArgumentOutOfRangeException.ThrowIfNegative(Start);
            ArgumentOutOfRangeException.ThrowIfNegative(keyLength);
            if (numericValue)
                throw new ArgumentException("Fast loading does not support numeric values.", nameof(numericValue));
            valueLength = valueLength == 0 ? 8 : valueLength;
            ArgumentOutOfRangeException.ThrowIfLessThan(valueLength, FastLoadStampLength);
            keyLength = keyLength == 0 ? NumUtils.NumDigits(opts.DbSize) : keyLength;
            var maxKeyLength = Math.Max(keyLength, NumUtils.NumDigits(checked((int)((long)Start + opts.DbSize - 1))));
            var workers = Math.Min(loadThreads, opts.DbSize);
            var maxKeysPerWorker = (int)(((long)opts.DbSize + workers - 1) / workers);
            batchSize = Math.Min(batchSize, maxKeysPerWorker);
            // Each argument must fit in the session's fixed send buffer; commands can span buffers.
            var bufferSize = checked(Math.Max(Math.Max(maxKeyLength, valueLength), opts.Auth?.Length ?? 0) + 32);
            var clientBufferSize = Math.Max(1 << 17, checked((int)BitOperations.RoundUpToPowerOf2((uint)bufferSize)));
            var seed = opts.WorkloadSeed == -1 ? 789110123 : opts.WorkloadSeed;
            var operation = opts.LSet ? OpType.SET : OpType.MSET;
            Console.WriteLine($"Fast load: {operation}, GarnetClientSession, {workers}/{loadThreads} workers, seed {seed}, 8-byte ASCII hexadecimal worker ID, key length >= {keyLength}, value length {valueLength}");
            var stopwatch = Stopwatch.StartNew();
            _ = RunWorkers(workers, worker =>
            {
                var first = Start + (long)opts.DbSize * worker / workers;
                var limit = Start + (long)opts.DbSize * (worker + 1) / workers;
                var payloadBytes = new byte[valueLength];
                // ASCII values preserve the existing LightClient delimiter-counting response handlers.
                new Random(unchecked(seed * -1521134295 + worker)).GetItems(
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"u8, payloadBytes);
                _ = Encoding.ASCII.GetBytes(worker.ToString("X8"), payloadBytes);
                var payload = Encoding.ASCII.GetString(payloadBytes);
                var command = new string[opts.LSet ? 3 : checked(2 * batchSize + 1)];
                command[0] = operation.ToString();
                for (var index = 2; index < command.Length; index += 2)
                    command[index] = payload;
                var responses = new Task<string>[opts.LSet ? batchSize : 1];
                using var client = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port),
                    new NetworkBufferSettings(sendBufferSize: clientBufferSize),
                    tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                    authPassword: opts.Auth);
                client.ConnectAsync(timeoutMs: 30_000).WaitAsync(TimeSpan.FromSeconds(30)).GetAwaiter().GetResult();
                waiter.Wait();
                while (first < limit)
                {
                    var count = (int)Math.Min(batchSize, limit - first);
                    if (opts.LSet && responses.Length != count)
                        Array.Resize(ref responses, count);
                    if (!opts.LSet && command.Length != 2 * count + 1)
                        Array.Resize(ref command, 2 * count + 1);
                    for (var index = 0; index < count; index++)
                    {
                        command[opts.LSet ? 1 : 2 * index + 1] =
                            (first + index).ToString(CultureInfo.InvariantCulture).PadLeft(keyLength, 'X');
                        if (opts.LSet)
                            responses[index] = client.ExecuteAsyncBatch(command);
                    }
                    if (!opts.LSet)
                        responses[0] = client.ExecuteAsyncBatch(command);
                    client.CompletePending(wait: false);
                    foreach (var response in Task.WhenAll(responses).WaitAsync(TimeSpan.FromSeconds(30)).GetAwaiter().GetResult())
                    {
                        if (response != "OK")
                            throw new InvalidOperationException($"Expected OK from fast-load write, received: {response}");
                    }
                    first += count;
                }
            }, fixedOperationCount: true);
            stopwatch.Stop();
            Console.WriteLine($"Operation type: {operation}");
            Console.WriteLine($"[Total time]: {stopwatch.Elapsed.TotalMilliseconds:N2}ms for {opts.DbSize:N2} ops");
            Console.WriteLine($"[Throughput]: {opts.DbSize / stopwatch.Elapsed.TotalSeconds:N2} ops/sec");
        }

    }
}