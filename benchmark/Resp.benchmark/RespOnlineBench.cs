// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;


namespace Resp.benchmark
{
    /// <summary>
    /// Online (continuous) load generating benchmark
    /// </summary>
    /// 
// Remove this when support for .net 6 is not required
#pragma warning disable IDE0300
    internal class RespOnlineBench
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);
        const int bufferSizeValue = 1 << 17;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool IsValidRange(long value)
            => value < HISTOGRAM_UPPER_BOUND && value > HISTOGRAM_LOWER_BOUND;

        readonly EndPoint endpoint;
        readonly int NumThreads;
        readonly OpType op;
        readonly Options opts;
        readonly string auth;
        readonly int BatchSize;

        readonly ManualResetEventSlim waiter = new();

        readonly LongHistogram[] thread_histograms;
        readonly ManualResetEventSlim WaitForEpochBump = new();
        readonly int runDuration;
        readonly int resetInterval;
        ulong iteration = 0;

        readonly int[] opPercent;
        readonly OpType[] opWorkload;

        AsyncPool<GarnetClientSession> gcsPool;
        AsyncPool<GarnetClient> gdbPool;
        AsyncPool<IConnectionMultiplexer> redisPool;
        IConnectionMultiplexer redis;
        GarnetClient garnetClient;

        readonly LightEpoch epoch = new();
        readonly Stopwatch epochWatch = new();

        readonly ILogger logger;

        readonly CancellationTokenSource cts = new();
        volatile int workerCount = 0;


        public RespOnlineBench(Options opts, int resetInterval = 30, int runDuration = int.MaxValue, ILoggerFactory loggerFactory = null)
        {
            this.runDuration = runDuration;
            this.resetInterval = resetInterval;
            this.endpoint = new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port);
            this.op = opts.Op;
            this.opts = opts;
            this.auth = opts.Auth;
            NumThreads = opts.NumThreads.ToArray()[0];
            BatchSize = opts.BatchSize.ToArray()[0];
            thread_histograms = new LongHistogram[NumThreads * 2];
            for (int i = 0; i < thread_histograms.Length; i++)
                thread_histograms[i] = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);

            opPercent = opts.OpPercent?.ToArray();
            opWorkload = opts.OpWorkload?.ToArray();

            this.logger = loggerFactory?.CreateLogger("online");

            if (opPercent != null && opWorkload != null)
            {
                if (opPercent.Length != opWorkload.Length)
                    throw new Exception($"opPercent {opWorkload.Length} and opWorkload {opWorkload.Length} mismatch!");
                for (int i = 1; i < opPercent.Length; i++)
                {
                    opPercent[i] += opPercent[i - 1];
                }
                if (opPercent[^1] != 100)
                    throw new Exception($"opPercent must sum to 100, distribution: {String.Join(',', opPercent)}");
            }

        }

        OpType SelectOpType(int percent)
        {
            for (int i = 0; i < opPercent.Length; i++)
                if (percent <= opPercent[i])
                    return opWorkload[i];
            throw new Exception($"Invalid percent range {percent}");
        }

        private double WaitForMonitorIterationSwitch(out int prev_iter)
        {
            prev_iter = (int)((this.iteration++));
            epochWatch.Start();
            try
            {
                epoch.Resume();
                epoch.BumpCurrentEpoch(WaitForEpochBump.Set);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                WaitForEpochBump.Reset();
                return 0;
            }
            finally
            {
                epoch.Suspend();
            }
            WaitForEpochBump.Wait();
            WaitForEpochBump.Reset();
            epochWatch.Stop();
            return epochWatch.ElapsedMilliseconds;
        }

        private void RecordValue(int thread_id, long elapsed)
        {
            try
            {
                epoch.Resume();
                var _offset = (int)(this.iteration & 0x1);
                if (IsValidRange(elapsed))
                    thread_histograms[_offset * NumThreads + thread_id].RecordValue(elapsed);
                else
                    thread_histograms[_offset * NumThreads + thread_id].RecordValue(HISTOGRAM_UPPER_BOUND);
            }
            finally
            {
                epoch.Suspend();
            }
        }

        private void InitializeClients()
        {
            if (opts.Client == ClientType.GarnetClientSession && opts.Pool)
            {
                gcsPool = new AsyncPool<GarnetClientSession>(opts.NumThreads.First(), () =>
                {
                    var c = new GarnetClientSession(endpoint, new(), tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    c.Connect();
                    if (auth != null)
                    {
                        c.Execute("AUTH", auth);
                        c.CompletePending();
                    }
                    return c;
                });
            }
            if (opts.Client == ClientType.GarnetClient)
            {
                if (opts.Pool)
                {
                    gdbPool = new AsyncPool<GarnetClient>(opts.NumThreads.First(), () =>
                    {
                        var gdb = new GarnetClient(endpoint, opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null, recordLatency: opts.ClientHistogram);
                        gdb.Connect();
                        if (auth != null)
                        {
                            gdb.ExecuteForStringResultAsync("AUTH", new string[] { auth }).GetAwaiter().GetResult();
                        }
                        return gdb;
                    });
                }
                else
                {
                    garnetClient = new GarnetClient(endpoint, opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null, recordLatency: opts.ClientHistogram);
                    garnetClient.Connect();
                    if (auth != null)
                    {
                        garnetClient.ExecuteForStringResultAsync("AUTH", new string[] { auth }).GetAwaiter().GetResult();
                    }
                }
            }
            if (opts.Client == ClientType.SERedis)
            {
                if (opts.Pool)
                {
                    redisPool = new AsyncPool<IConnectionMultiplexer>(opts.NumThreads.First(), () =>
                    {
                        return ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost));
                    });
                }
                else
                {
                    redis = Program.redis;
                }
            }
        }

        private Thread[] InitializeThreadWorkers()
        {
            Thread[] workers = new Thread[NumThreads];
            for (int idx = 0; idx < NumThreads; idx++)
            {
                int x = idx;

                switch (opts.Client)
                {
                    case ClientType.LightClient:
                        if (idx == 0) Console.WriteLine("Using OpRunnerLightClient...");
                        workers[idx] = new Thread(() => OpRunnerLightClient(x));
                        break;
                    case ClientType.GarnetClientSession:
                        if (opts.IntraThreadParallelism > 1)
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerGarnetClientSessionParallel...");
                            workers[idx] = new Thread(() => OpRunnerGarnetClientSessionParallel(x, opts.IntraThreadParallelism));
                        }
                        else
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerGarnetClientSession...");
                            workers[idx] = new Thread(() => OpRunnerGarnetClientSession(x));
                        }
                        break;
                    case ClientType.GarnetClient:
                        if (opts.SyncMode)
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerGarnetClientSync...");
                            workers[idx] = new Thread(() => OpRunnerGarnetClientSync(x));
                        }
                        else if (opts.IntraThreadParallelism > 1)
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerGarnetClientParallel...");
                            workers[idx] = new Thread(() => OpRunnerGarnetClientParallel(x, opts.IntraThreadParallelism));
                        }
                        else
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerGarnetClient...");
                            workers[idx] = new Thread(() => OpRunnerGarnetClient(x));
                        }
                        break;
                    case ClientType.SERedis:
                        if (opts.IntraThreadParallelism > 1)
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerSERedisParallel...");
                            workers[idx] = new Thread(() => OpRunnerSERedisParallel(x, opts.IntraThreadParallelism));
                        }
                        else
                        {
                            if (idx == 0) Console.WriteLine("Using OpRunnerSERedis...");
                            workers[idx] = new Thread(() => OpRunnerSERedis(x));
                        }
                        break;
                    default:
                        throw new Exception($"ClientType {opts.Client} not supported");
                }
            }
            return workers;
        }

        public void Run()
        {
            Console.WriteLine($"Running benchmark using {opts.Client} client type");
            InitializeClients();
            var workers = InitializeThreadWorkers();

            // Start threads.
            foreach (Thread worker in workers)
                worker.Start();

            var summary = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);
            bool printHeader = true;
            long last_iter_ops = 0;
            Stopwatch swatch = new();
            swatch.Start();
            waiter.Set();
            const int pad = -15;
            ulong resetInterval = (ulong)this.resetInterval;

            while (true)
            {
                Thread.Sleep(2000);

                var epochElapsedMs = WaitForMonitorIterationSwitch(out var prev_iter);

                if (printHeader)
                {
                    printHeader = false;
                    if (opts.DisableConsoleLogger && opts.FileLogger == null)
                    {
                        Console.WriteLine(
                            $"{"min (us);",pad}" +
                            $"{"5th (us);",pad}" +
                            $"{"median (us);",pad}" +
                            $"{"avg (us);",pad}" +
                            $"{"95th (us);",pad}" +
                            $"{"99th (us);",pad}" +
                            $"{"99.9th (us);",pad}" +
                            $"{"total_ops;",pad}" +
                            $"{"iter_tops;",pad}" +
                            $"{"tpt (Kops/sec)",pad}");
                    }
                    else
                    {
                        var msg = $"{"min (us);",pad}" +
                            $"{"5th (us);",pad}" +
                            $"{"median (us);",pad}" +
                            $"{"avg (us);",pad}" +
                            $"{"95th (us);",pad}" +
                            $"{"99th (us);",pad}" +
                            $"{"99.9th (us);",pad}" +
                            $"{"total_ops;",pad}" +
                            $"{"iter_tops;",pad}" +
                            $"{"tpt (Kops/sec)",pad}";
                        logger?.LogInformation("{msg}", msg);
                    }
                }

                var offset = prev_iter & 0x1;
                for (int i = offset * NumThreads; i < offset * NumThreads + NumThreads; i++)
                {
                    summary.Add(thread_histograms[i]);
                    thread_histograms[i].Reset();
                }

                //find operation perform during polling period
                long curr_iter_ops = summary.TotalCount - last_iter_ops;
                //if more than one operation per latency recording
                if (opts.IntraThreadParallelism > 1 && !opts.SyncMode)
                    curr_iter_ops *= opts.IntraThreadParallelism;

                swatch.Stop();

                double elapsedSecs = (double)swatch.ElapsedMilliseconds - epochElapsedMs;

                if (opts.DisableConsoleLogger && opts.FileLogger == null)
                {
                    if (summary.TotalCount > 0)
                    {
                        Console.WriteLine(
                            $"{Math.Round(summary.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetMean() / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{Math.Round(summary.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                            $"{summary.TotalCount,pad}" +
                            $"{curr_iter_ops,pad}" +
                            $"{Math.Round(BatchSize * curr_iter_ops / elapsedSecs, 2),pad}");
                    }
                    else
                    {
                        Console.WriteLine($"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}");
                    }
                }
                else
                {
                    if (summary.TotalCount > 0)
                    {
                        var histogramOutput = $"{Math.Round(summary.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetMean() / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{Math.Round(summary.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds, 2),pad}" +
                        $"{summary.TotalCount,pad}" +
                        $"{curr_iter_ops,pad}" +
                        $"{Math.Round(BatchSize * curr_iter_ops / elapsedSecs, 2),pad}";
                        logger.Log(LogLevel.Information, "{msg}", histogramOutput);
                    }
                    else
                    {
                        var histogramOutput = $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}" + $"{0,pad}";
                        logger.Log(LogLevel.Information, "{msg}", histogramOutput);
                    }
                }
                last_iter_ops = summary.TotalCount;
                swatch.Reset();
                swatch.Start();

                if (iteration % resetInterval == 0)
                {
                    summary.Reset();
                    last_iter_ops = 0;
                }

                if ((ulong)runDuration == iteration)
                    break;
            }

            while (workerCount > 0)
            {
                cts.Cancel();
                Thread.Yield();
            }
        }

        public unsafe void OpRunnerLightClient(int thread_id)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);

            var keyLen = req.keyLen;
            var valueLen = req.valueLen;

            var sskey = "sskey";
            var sskeyLen = sskey.Length;

            var exactSize = 13 + RespWriteUtils.GetBulkStringLength(keyLen) + RespWriteUtils.GetBulkStringLength(valueLen);
            var size = (int)Utility.PreviousPowerOf2(exactSize);
            if (exactSize > size) size *= 2;

            var onResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);

            var client = new LightClient(endpoint, (int)op, onResponseDelegate, size, opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
            client.Connect();
            client.Authenticate(auth);

            var pingBufferAllocation = GC.AllocateArray<byte>(14, true);
            var pingBuffer = (byte*)Unsafe.AsPointer(ref pingBufferAllocation[0]);
            "*1\r\n$4\r\nPING\r\n"u8.CopyTo(new Span<byte>(pingBuffer, 14));

            var getBufferA = GC.AllocateArray<byte>(13 + RespWriteUtils.GetBulkStringLength(keyLen), true);
            var getBuffer = (byte*)Unsafe.AsPointer(ref getBufferA[0]);
            "*2\r\n$3\r\nGET\r\n"u8.CopyTo(new Span<byte>(getBuffer, 13));

            var setBufferA = GC.AllocateArray<byte>(130 + RespWriteUtils.GetBulkStringLength(keyLen) + RespWriteUtils.GetBulkStringLength(valueLen), true);
            var setBuffer = (byte*)Unsafe.AsPointer(ref setBufferA[0]);
            "*3\r\n$3\r\nSET\r\n"u8.CopyTo(new Span<byte>(setBuffer, 13));

            var setexBufferA = GC.AllocateArray<byte>(15 + RespWriteUtils.GetBulkStringLength(keyLen) + RespWriteUtils.GetIntegerAsBulkStringLength(opts.Ttl) + RespWriteUtils.GetBulkStringLength(valueLen), true);
            var setexBuffer = (byte*)Unsafe.AsPointer(ref setexBufferA[0]);
            "*4\r\n$5\r\nSETEX\r\n"u8.CopyTo(new Span<byte>(setexBuffer, 15));

            var delBufferA = GC.AllocateArray<byte>(13 + RespWriteUtils.GetBulkStringLength(keyLen), true);
            var delBuffer = (byte*)Unsafe.AsPointer(ref delBufferA[0]);
            "*2\r\n$3\r\nDEL\r\n"u8.CopyTo(new Span<byte>(delBuffer, 13));

            var zaddBufferA = GC.AllocateArray<byte>(14 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetIntegerAsBulkStringLength(1) + RespWriteUtils.GetBulkStringLength(keyLen), true);
            var zaddBuffer = (byte*)Unsafe.AsPointer(ref zaddBufferA[0]);
            "*4\r\n$4\r\nZADD\r\n"u8.CopyTo(new Span<byte>(zaddBuffer, 14));

            var zremBufferA = GC.AllocateArray<byte>(14 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetBulkStringLength(keyLen), true);
            var zremBuffer = (byte*)Unsafe.AsPointer(ref zremBufferA[0]);
            "*3\r\n$4\r\nZREM\r\n"u8.CopyTo(new Span<byte>(zremBuffer, 14));

            var zcardBufferAllocation = GC.AllocateArray<byte>(15 + RespWriteUtils.GetBulkStringLength(sskeyLen), true);
            var zcardBuffer = (byte*)Unsafe.AsPointer(ref zcardBufferAllocation[0]);
            "*2\r\n$4\r\nZCARD\r\n"u8.CopyTo(new Span<byte>(zcardBuffer, 15));

            var expireBufferA = GC.AllocateArray<byte>(16 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetIntegerAsBulkStringLength(opts.Ttl), true);
            var expireBuffer = (byte*)Unsafe.AsPointer(ref expireBufferA[0]);
            "*3\r\n$6\r\nEXPIRE\r\n"u8.CopyTo(new Span<byte>(expireBuffer, 16));

            var getEnd = getBuffer + 13 + RespWriteUtils.GetBulkStringLength(keyLen);
            var setEnd = setBuffer + 130 + RespWriteUtils.GetBulkStringLength(keyLen) + RespWriteUtils.GetBulkStringLength(valueLen);
            var setexEnd = setexBuffer + 130 + RespWriteUtils.GetBulkStringLength(keyLen) + RespWriteUtils.GetIntegerAsBulkStringLength(opts.Ttl) + RespWriteUtils.GetBulkStringLength(valueLen);
            var delEnd = delBuffer + 13 + RespWriteUtils.GetBulkStringLength(keyLen);
            var zaddEnd = zaddBuffer + 14 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetBulkStringLength(keyLen);
            var zremEnd = zremBuffer + 14 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetBulkStringLength(keyLen);
            var expireEnd = expireBuffer + 16 + RespWriteUtils.GetBulkStringLength(sskeyLen) + RespWriteUtils.GetIntegerAsBulkStringLength(opts.Ttl);
            var zcardEnd = zcardBuffer + 15 + RespWriteUtils.GetBulkStringLength(keyLen);

            Random r = new(thread_id + 100);
            waiter.Wait();
            while (true)
            {
                var rand = r.Next(100);
                var op = SelectOpType(rand);
                client.opType = (int)op;
                var startTimestamp = Stopwatch.GetTimestamp();

                if (cts.IsCancellationRequested) break;

                switch (op)
                {
                    case OpType.PING:
                        client.Send(pingBuffer, 14, 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.GET:
                        var getCurr = getBuffer + 13;
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref getCurr, getEnd);
                        client.Send(getBuffer, (int)(getCurr - getBuffer), 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.SET:
                        var setCurr = setBuffer + 13;
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref setCurr, setEnd);
                        RespWriteUtils.TryWriteBulkString(req.GenerateValueBytes().Span, ref setCurr, setEnd);
                        client.Send(setBuffer, (int)(setCurr - setBuffer), 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.SETEX:
                        var setexCurr = setexBuffer + 15;
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref setexCurr, setexEnd);
                        RespWriteUtils.TryWriteInt32AsBulkString(opts.Ttl, ref setexCurr, setexEnd);
                        RespWriteUtils.TryWriteBulkString(req.GenerateValueBytes().Span, ref setexCurr, setexEnd);
                        client.Send(setexBuffer, (int)(setexCurr - setexBuffer), 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.DEL:
                        var delCurr = delBuffer + 13;
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref delCurr, delEnd);
                        client.Send(delBuffer, (int)(delCurr - delBuffer), 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.ZADD:
                        var zaddCurr = zaddBuffer + 14;
                        RespWriteUtils.TryWriteAsciiBulkString(sskey, ref zaddCurr, zaddEnd);
                        RespWriteUtils.TryWriteInt32AsBulkString(1, ref zaddCurr, zaddEnd);
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref zaddCurr, zaddEnd);
                        client.Send(zaddBuffer, (int)(zaddCurr - zaddBuffer), 1);
                        if (opts.Ttl > 0)
                        {
                            // NOTE: Here we are not resetting opType. This only works for online bench
                            var expireCurr = expireBuffer + 16;
                            RespWriteUtils.TryWriteAsciiBulkString(sskey, ref expireCurr, expireEnd);
                            RespWriteUtils.TryWriteInt32AsBulkString(opts.Ttl, ref expireCurr, expireEnd);
                            client.Send(expireBuffer, (int)(expireCurr - expireBuffer), 1);
                        }
                        client.CompletePendingRequests();
                        break;
                    case OpType.ZREM:
                        var zremCurr = zremBuffer + 14;
                        RespWriteUtils.TryWriteAsciiBulkString(sskey, ref zremCurr, zremEnd);
                        RespWriteUtils.TryWriteInt32AsBulkString(1, ref zremCurr, zremEnd);
                        RespWriteUtils.TryWriteAsciiBulkString(req.GenerateKey(), ref zremCurr, zremEnd);
                        client.Send(zremBuffer, (int)(zremCurr - zremBuffer), 1);
                        client.CompletePendingRequests();
                        break;
                    case OpType.ZCARD:
                        var zcardCurr = zcardBuffer + 15;
                        RespWriteUtils.TryWriteAsciiBulkString(sskey, ref zcardCurr, zcardEnd);
                        client.Send(zcardBuffer, (int)(zcardCurr - zcardEnd), 1);
                        client.CompletePendingRequests();
                        break;
                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }

                var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                RecordValue(thread_id, elapsed);
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClientSession(int thread_id)
        {
            _ = Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);

            GarnetClientSession client = null;
            if (!opts.Pool)
            {
                client = new GarnetClientSession(
                    endpoint,
                    new(Math.Max(bufferSizeValue, opts.ValueLength * opts.IntraThreadParallelism)),
                    tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                client.Connect();
                if (auth != null)
                {
                    client.Execute("AUTH", auth);
                    client.CompletePending();
                }
            }

            var r = new Random(thread_id + 100);
            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested) break;
                    var rand = r.Next(100);
                    var op = SelectOpType(rand);
                    var startTimestamp = Stopwatch.GetTimestamp();
                    var c = opts.Pool ? await gcsPool.GetAsync() : client;
                    _ = op switch
                    {
                        OpType.PING => await c.ExecuteAsync(["PING"]),
                        OpType.GET => await c.ExecuteAsync(["GET", req.GenerateKey()]),
                        OpType.SET => await c.ExecuteAsync(["SET", req.GenerateKey(), req.GenerateValue()]),
                        OpType.SETEX => await c.ExecuteAsync(["SETEX", req.GenerateKey(), opts.Ttl.ToString(), req.GenerateValue()]),
                        OpType.DEL => await c.ExecuteAsync(["DEL", req.GenerateKey()]),
                        OpType.SETBIT => await c.ExecuteAsync(["SETBIT", req.GenerateKey(), req.GenerateBitOffset()]),
                        OpType.GETBIT => await c.ExecuteAsync(["GETBIT", req.GenerateKey(), req.GenerateBitOffset()]),
                        OpType.PUBLISH => await c.ExecuteAsync(["PUBLISH", req.GenerateKey(), req.GenerateValue()]),
                        OpType.SPUBLISH => await c.ExecuteAsync(["SPUBLISH", req.GenerateKey(), req.GenerateValue()]),
                        OpType.ZADD => await ZADD(),
                        OpType.ZREM => await ZREM(),
                        OpType.ZCARD => await ZCARD(),
                        OpType.READWRITETX => await c.ExecuteAsync("READWRITETX", req.GenerateKey(), req.GenerateKey(), req.GenerateKey(), "1000"),
                        OpType.SAMPLEUPDATETX => await c.ExecuteAsync("SAMPLEUPDATETX", req.GenerateKeyRandom(), req.GenerateValue(),   // stringKey
                                            req.GenerateObjectKeyRandom(), req.GenerateObjectEntry(), req.GenerateObjectEntryScore(),   // sortedSetKey1
                                            req.GenerateObjectKeyRandom(), req.GenerateObjectEntry(), req.GenerateObjectEntryScore()),  // sortedSetKey2
                        OpType.SAMPLEDELETETX => await c.ExecuteAsync("SAMPLEDELETETX", req.GenerateKeyRandom(),    // stringKey
                                            req.GenerateObjectKeyRandom(), req.GenerateObjectEntry(),               // sortedSetKey1                    
                                            req.GenerateObjectKeyRandom(), req.GenerateObjectEntry()),              // sortedSetKey2
                        _ => throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!")
                    };

                    if (opts.Pool)
                        gcsPool.Return(c);

                    var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    RecordValue(thread_id, elapsed);

                    async Task<string> ZADD()
                    {
                        var key = req.GenerateKey();
                        var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                        var res = await c.ExecuteAsync(new string[] { "ZADD", sskey, "1.0", key });
                        if (opts.Ttl > 0)
                            return await c.ExecuteAsync(new string[] { "EXPIRE", sskey, opts.Ttl.ToString() });
                        return res;
                    }

                    async Task<string> ZREM()
                    {
                        var key = req.GenerateKey();
                        var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                        var resp = await c.ExecuteAsync(new string[] { "ZREM", sskey, key });
                        if (opts.Ttl > 0)
                            return await c.ExecuteAsync(new string[] { "EXPIRE", sskey, opts.Ttl.ToString() });
                        return resp;
                    }

                    async Task<string> ZCARD()
                    {
                        var key = req.GenerateKey();
                        var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                        var resp = await c.ExecuteAsync(new string[] { "ZCARD", sskey });
                        if (opts.Ttl > 0)
                            return await c.ExecuteAsync(new string[] { "EXPIRE", sskey, opts.Ttl.ToString() });
                        return resp;
                    }
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerGarnetClientSession)}");
                }
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClientSessionParallel(int thread_id, int parallel)
        {
            _ = Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);

            GarnetClientSession client = null;
            if (!opts.Pool)
            {
                client = new GarnetClientSession(
                    endpoint,
                    new NetworkBufferSettings(Math.Max(131072, opts.IntraThreadParallelism * opts.ValueLength)),
                    tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                client.Connect();
                if (auth != null)
                {
                    client.Execute("AUTH", auth);
                    client.CompletePending();
                }
            }
            var r = new Random(thread_id + 100);
            var offset = 0;

            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                var op = SelectOpType(rand);
                var c = opts.Pool ? await gcsPool.GetAsync() : client;
                var startTimestamp = Stopwatch.GetTimestamp();

                switch (op)
                {
                    case OpType.PING:
                        c.ExecuteBatch(["PING"]);
                        break;
                    case OpType.GET:
                        c.ExecuteBatch(["GET", req.GenerateKey()]);
                        break;
                    case OpType.SET:
                        c.ExecuteBatch(["SET", req.GenerateKey(), req.GenerateValue()]);
                        break;
                    case OpType.SETEX:
                        c.ExecuteBatch(["SETEX", req.GenerateKey(), opts.Ttl.ToString(), req.GenerateValue()]);
                        break;
                    case OpType.DEL:
                        c.ExecuteBatch(["DEL", req.GenerateKey()]);
                        break;
                    case OpType.SETBIT:
                        c.ExecuteBatch(["SETBIT", req.GenerateKey(), req.GenerateBitOffset()]);
                        break;
                    case OpType.GETBIT:
                        c.ExecuteBatch(["GETBIT", req.GenerateKey(), req.GenerateBitOffset()]);
                        break;
                    case OpType.PUBLISH:
                        c.ExecuteBatch(["PUBLISH", req.GenerateKey(), req.GenerateValue()]);
                        break;
                    case OpType.SPUBLISH:
                        c.ExecuteBatch(["SPUBLISH", req.GenerateKey(), req.GenerateValue()]);
                        break;

                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }
                offset++;

                try
                {
                    if (offset == parallel)
                    {
                        c.CompletePending(!opts.Burst);
                        if (opts.Pool) gcsPool.Return(c);
                        offset = 0;
                        var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                        RecordValue(thread_id, elapsed);
                    }
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerGarnetClientSessionParallel)}");
                }
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public void OpRunnerGarnetClientSync(int thread_id)
        {
            _ = Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var local_ops_started = 0;
            var local_ops_completed = 0;
            var waitCallback = new ManualResetEventSlim();
            var r = new Random(thread_id + 100);

            void callbackMem(long start, MemoryResult<byte> mem)
            {
                var elapsed = Stopwatch.GetTimestamp() - start;
                RecordValue(thread_id, elapsed);
                if (opts.IntraThreadParallelism > 1)
                    _ = Interlocked.Increment(ref local_ops_completed);
                else
                    waitCallback.Set();
            }

            void callbackStr(long start, string str)
            {
                var elapsed = Stopwatch.GetTimestamp() - start;
                RecordValue(thread_id, elapsed);
                if (opts.IntraThreadParallelism > 1)
                    _ = Interlocked.Increment(ref local_ops_completed);
                else
                    waitCallback.Set();
            }

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                var op = SelectOpType(rand);
                var startTimestamp = Stopwatch.GetTimestamp();

                var gdb = opts.Pool ? gdbPool.Get() : garnetClient;
                switch (op)
                {
                    case OpType.PING:
                        gdb.Ping(callbackStr);
                        break;
                    case OpType.GET:
                        gdb.StringGetAsMemory(req.GenerateKeyBytes(), callbackMem, startTimestamp);
                        break;
                    case OpType.SET:
                        gdb.StringSet(req.GenerateKeyBytes(), req.GenerateValueBytes(), callbackStr, startTimestamp);
                        break;
                    case OpType.SETEX:
                        gdb.ExecuteForStringResult(callbackStr, startTimestamp, "SETEX", [req.GenerateKey(), opts.Ttl.ToString(), req.GenerateValue()]);
                        break;
                    case OpType.DEL:
                        gdb.KeyDelete(req.GenerateKeyBytes(), callbackStr, startTimestamp);
                        break;
                    case OpType.ZADD:
                        {
                            var key = req.GenerateKey();
                            var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                            gdb.ExecuteForStringResult(callbackStr, startTimestamp, "ZADD", [sskey, "1.0", key]);
                            if (opts.Ttl > 0)
                                gdb.ExecuteForStringResult(callbackStr, startTimestamp, "EXPIRE", [sskey, opts.Ttl.ToString()]);
                        }
                        break;
                    case OpType.ZREM:
                        {
                            var key = req.GenerateKey();
                            var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                            gdb.ExecuteForStringResult(callbackStr, startTimestamp, "ZREM", [sskey, key]);
                            if (opts.Ttl > 0)
                                gdb.ExecuteForStringResult(callbackStr, startTimestamp, "EXPIRE", [sskey, opts.Ttl.ToString()]);
                        }
                        break;
                    case OpType.ZCARD:
                        {
                            var key = req.GenerateKey();
                            var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                            gdb.ExecuteForStringResult(callbackStr, startTimestamp, "ZCARD", [sskey]);
                            if (opts.Ttl > 0)
                                gdb.ExecuteForStringResult(callbackStr, startTimestamp, "EXPIRE", [sskey, opts.Ttl.ToString()]);
                        }
                        break;
                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }

                local_ops_started++;
                if (opts.IntraThreadParallelism > 1)
                {
                    if (local_ops_started == opts.IntraThreadParallelism)
                    {
                        while (local_ops_completed < local_ops_started) Thread.Yield();
                        local_ops_started = local_ops_completed = 0;
                    }
                }
                else
                {
                    waitCallback.Wait();
                    waitCallback.Reset();
                }

                if (opts.Pool)
                    gdbPool.Return(gdb);
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClient(int thread_id)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var r = new Random(thread_id + 100);

            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested) break;
                    var rand = r.Next(100);
                    var op = SelectOpType(rand);
                    var startTimestamp = Stopwatch.GetTimestamp();

                    var gdb = opts.Pool ? await gdbPool.GetAsync() : garnetClient;
                    switch (op)
                    {
                        case OpType.PING:
                            await gdb.PingAsync();
                            break;
                        case OpType.GET:
                            await gdb.StringGetAsMemoryAsync(req.GenerateKeyBytes());
                            break;
                        case OpType.SET:
                            await gdb.StringSetAsync(req.GenerateKeyBytes(), req.GenerateValueBytes());
                            break;
                        case OpType.SETEX:
                            await gdb.ExecuteForStringResultAsync("SETEX", new string[] { req.GenerateKey(), opts.Ttl.ToString(), req.GenerateValue() });
                            break;
                        case OpType.DEL:
                            await gdb.KeyDeleteAsync(req.GenerateKeyBytes());
                            break;
                        case OpType.ZADD:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                await gdb.ExecuteForStringResultAsync("ZADD", new string[] { sskey, "1.0", key });
                                if (opts.Ttl > 0)
                                {
                                    await gdb.ExecuteForStringResultAsync("EXPIRE", new string[] { sskey, opts.Ttl.ToString() });
                                }
                            }
                            break;
                        case OpType.ZREM:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                await gdb.ExecuteForStringResultAsync("ZREM", new string[] { sskey, key });
                                if (opts.Ttl > 0)
                                {
                                    await gdb.ExecuteForStringResultAsync("EXPIRE", new string[] { sskey, opts.Ttl.ToString() });
                                }
                            }
                            break;
                        default:
                            throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                    }
                    if (opts.Pool)
                    {
                        gdbPool.Return(gdb);
                    }

                    var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    RecordValue(thread_id, elapsed);
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerGarnetClient)}");
                }
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClientParallel(int thread_id, int parallel)
        {
            _ = Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var r = new Random(thread_id + 100);
            var tasks = new Task[parallel];
            var offset = 0;

            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested) break;
                    var rand = r.Next(100);
                    var op = SelectOpType(rand);
                    var startTimestamp = Stopwatch.GetTimestamp();

                    var gdb = opts.Pool ? await gdbPool.GetAsync() : garnetClient;

                    async Task<string> ZADD()
                    {
                        var key = req.GenerateKey();
                        var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                        var resp = await gdb.ExecuteForStringResultAsync("ZADD", new string[] { sskey, "1.0", key });
                        if (opts.Ttl > 0)
                            return await gdb.ExecuteForStringResultAsync("EXPIRE", new string[] { sskey, opts.Ttl.ToString() });
                        return resp;
                    }

                    async Task<string> ZREM()
                    {
                        var key = req.GenerateKey();
                        var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                        var resp = await gdb.ExecuteForStringResultAsync("ZREM", new string[] { sskey, key });
                        if (opts.Ttl > 0)
                            return await gdb.ExecuteForStringResultAsync("EXPIRE", new string[] { sskey, opts.Ttl.ToString() });
                        return resp;
                    }

                    tasks[offset++] = op switch
                    {
                        OpType.PING => gdb.PingAsync(),
                        OpType.GET => gdb.StringGetAsync(req.GenerateKey()),
                        OpType.SET => gdb.StringSetAsync(req.GenerateKey(), req.GenerateValue()),
                        OpType.SETEX => gdb.ExecuteForStringResultAsync("SETEX", [req.GenerateKey(), opts.Ttl.ToString(), req.GenerateValue()]),
                        OpType.DEL => gdb.KeyDeleteAsync(req.GenerateKey()),
                        OpType.ZADD => ZADD(),
                        OpType.ZREM => ZREM(),
                        _ => throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!")
                    };

                    if (opts.Pool)
                        gdbPool.Return(gdb);

                    if (offset == parallel)
                    {
                        await Task.WhenAll(tasks);
                        offset = 0;
                        var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                        RecordValue(thread_id, elapsed);
                    }
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerGarnetClientParallel)}");
                }

            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerSERedis(int thread_id)
        {
            _ = Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var r = new Random(thread_id + 100);

            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested) break;
                    var rand = r.Next(100);
                    var op = SelectOpType(rand);
                    var startTimestamp = Stopwatch.GetTimestamp();

                    var rd = opts.Pool ? redisPool.Get() : redis;
                    var db = rd.GetDatabase(0);

                    switch (op)
                    {
                        case OpType.PING:
                            await db.PingAsync();
                            break;
                        case OpType.GET:
                            await db.StringGetAsync(req.GenerateKey());
                            break;
                        case OpType.SET:
                            await db.StringSetAsync(req.GenerateKey(), req.GenerateValue());
                            break;
                        case OpType.SETEX:
                            await db.StringSetAsync(req.GenerateKey(), req.GenerateValue(), TimeSpan.FromSeconds(opts.Ttl));
                            break;
                        case OpType.DEL:
                            await db.KeyDeleteAsync(req.GenerateKey());
                            break;
                        case OpType.PUBLISH:
                            await db.PublishAsync(RedisChannel.Literal(req.GenerateKey()), req.GenerateValue());
                            break;
                        case OpType.SPUBLISH:
                            await db.ExecuteAsync("SPUBLISH", req.GenerateKey(), req.GenerateValue());
                            break;
                        case OpType.ZADD:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                await db.SortedSetAddAsync(sskey, key, 1.0);
                                if (opts.Ttl > 0)
                                {
                                    await db.KeyExpireAsync(sskey, TimeSpan.FromSeconds(opts.Ttl));
                                }
                            }
                            break;
                        case OpType.ZREM:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                await db.SortedSetRemoveAsync(sskey, key);
                                if (opts.Ttl > 0)
                                {
                                    await db.KeyExpireAsync(sskey, TimeSpan.FromSeconds(opts.Ttl));
                                }
                            }
                            break;
                        default:
                            throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                    }

                    if (opts.Pool)
                        redisPool.Return(rd);

                    var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    RecordValue(thread_id, elapsed);
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerSERedis)}");
                }
            }
            _ = Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerSERedisParallel(int thread_id, int parallel)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            var r = new Random(thread_id + 100);
            var tasks = new Task[parallel];
            var offset = 0;

            waiter.Wait();
            ulong countErrors = 0;
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested) break;
                    var rand = r.Next(100);
                    var op = SelectOpType(rand);
                    var startTimestamp = Stopwatch.GetTimestamp();

                    var rd = opts.Pool ? await redisPool.GetAsync() : redis;
                    var db = rd.GetDatabase(0);
                    switch (op)
                    {
                        case OpType.PING:
                            tasks[offset++] = db.PingAsync();
                            break;
                        case OpType.GET:
                            tasks[offset++] = db.StringGetAsync(req.GenerateKey());
                            break;
                        case OpType.SET:
                            tasks[offset++] = db.StringSetAsync(req.GenerateKey(), req.GenerateValue());
                            break;
                        case OpType.PUBLISH:
                            tasks[offset++] = db.PublishAsync(RedisChannel.Literal(req.GenerateKey()), req.GenerateValue());
                            break;
                        case OpType.SPUBLISH:
                            tasks[offset++] = db.ExecuteAsync("SPUBLISH", req.GenerateKey(), req.GenerateValue());
                            break;
                        case OpType.SETEX:
                            tasks[offset++] = db.StringSetAsync(req.GenerateKey(), req.GenerateValue(), TimeSpan.FromSeconds(opts.Ttl));
                            break;
                        case OpType.DEL:
                            tasks[offset++] = db.KeyDeleteAsync(req.GenerateKey());
                            break;
                        case OpType.ZADD:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                tasks[offset++] = Task.Run(async () =>
                                {
                                    await db.SortedSetAddAsync(sskey, key, 1.0);
                                    if (opts.Ttl > 0)
                                    {
                                        await db.KeyExpireAsync(sskey, TimeSpan.FromSeconds(opts.Ttl));
                                    }
                                });
                            }
                            break;
                        case OpType.ZREM:
                            {
                                var key = req.GenerateKey();
                                var sskey = opts.SortedSetCardinality > 0 ? $"sskey{Math.Abs(HashUtils.StableHash(key)) % opts.SortedSetCardinality}" : "sskey";
                                tasks[offset++] = Task.Run(async () =>
                                {
                                    await db.SortedSetRemoveAsync(sskey, key);
                                    if (opts.Ttl > 0)
                                    {
                                        await db.KeyExpireAsync(sskey, TimeSpan.FromSeconds(opts.Ttl));
                                    }
                                });
                            }
                            break;
                        default:
                            throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                    }

                    if (opts.Pool)
                        redisPool.Return(rd);

                    if (offset == parallel)
                    {
                        await Task.WhenAll(tasks);
                        offset = 0;
                        var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                        RecordValue(thread_id, elapsed);
                    }
                }
                catch (Exception ex)
                {
                    if (countErrors++ % (1 << 15) == 0)
                        logger?.LogError(ex, $"{nameof(OpRunnerSERedisParallel)}");
                }

            }
            _ = Interlocked.Decrement(ref workerCount);
        }
    }
#pragma warning restore IDE0300
}