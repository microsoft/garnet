// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using Garnet.client;
using Garnet.common;
using HdrHistogram;
using StackExchange.Redis;

namespace Resp.benchmark
{
    /// <summary>
    /// Dummy clients issuing commands as fast as possible, with varying number of
    /// threads, to stress server side.
    /// </summary>
    public class TxnPerfBench
    {
        readonly int readPerTxn;
        readonly int writePerTxn;
        readonly string address;
        readonly int port;
        readonly int NumThreads;
        readonly int BatchSize;
        readonly int runDuration;
        readonly int resetInterval;
        readonly int reportInterval;
        readonly OpType op;
        readonly Options opts;
        readonly string auth;

        readonly ManualResetEventSlim waiter = new();
        readonly LongHistogram[] saved;
        volatile int pendingRunners = 0;
        long total_ops_done = 0;

        int[] opPercent;
        OpType[] opWorkload;

        AsyncPool<GarnetClientSession> gcsPool;
        AsyncPool<IConnectionMultiplexer> redisPool;
        IConnectionMultiplexer redis;

        public TxnPerfBench(Options opts, int resetInterval = 30, int runDuration = int.MaxValue, int reportInterval = 2, int readPerTxn = 4, int writePerTxn = 4)
        {
            this.address = opts.Address;
            this.port = opts.Port;
            this.runDuration = runDuration;
            this.resetInterval = resetInterval;
            this.op = opts.Op;
            this.opts = opts;
            this.reportInterval = reportInterval;
            this.auth = opts.Auth;
            this.readPerTxn = readPerTxn;
            this.writePerTxn = writePerTxn;
            NumThreads = opts.NumThreads.ToArray()[0];
            BatchSize = opts.BatchSize.ToArray()[0];
            saved = new LongHistogram[NumThreads];

            opPercent = opts.OpPercent?.ToArray();
            opWorkload = opts.OpWorkload?.ToArray();

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

        private static void DumpHistogram(HistogramBase histogram)
        {
            Console.WriteLine("<<< History >>>");
            Console.WriteLine("min (us); 5th (us); median (us); avg (us); 95th (us); 99th (us); 99.9th (us)");
            Console.WriteLine("{0:0.0}; {1:0.0}; {2:0.0}; {3:0.0}; {4:0.0}; {5:0.0}; {6:0.0}",
                histogram.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetMean() / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds,
                histogram.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds);
        }

        public void Run()
        {
            // Query database
            Thread[] workers = new Thread[NumThreads];
            Console.WriteLine($"Running benchmark using {opts.Client} client type");

            if (opts.Client == ClientType.GarnetClientSession && opts.Pool)
            {
                gcsPool = new AsyncPool<GarnetClientSession>(opts.NumThreads.First(), () =>
                {
                    var c = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(address), port), new(), tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    c.Connect();
                    if (auth != null)
                    {
                        c.Execute("AUTH", auth);
                        c.CompletePending();
                    }
                    return c;
                });
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
            for (int idx = 0; idx < NumThreads; idx++)
            {
                int x = idx;

                switch (opts.Client)
                {
                    case ClientType.SERedis:
                        workers[idx] = new Thread(() => OpRunnerSERedis(x));
                        break;
                    default:
                        throw new Exception($"ClientType {opts.Client} not supported");
                }
            }

            // Start threads.
            foreach (Thread worker in workers)
                worker.Start();
            int iteration = -reportInterval;
            var histogram = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            var summary = (LongHistogram)histogram.Copy();

            Stopwatch swatch = new();
            waiter.Set();

            bool printHeader = true;
            while (true)
            {
                Thread.Sleep(1000);
                iteration++;

                if (iteration % reportInterval == 0)
                {
                    waiter.Reset();
                    Interlocked.Add(ref pendingRunners, NumThreads);
                    while (pendingRunners > 0) Thread.Yield();

                    if (iteration == 0) // ignore first report
                    {
                        total_ops_done = 0;
                        swatch.Restart();
                    }
                    else
                    {
                        swatch.Stop();
                        if (!opts.ClientHistogram)
                        {
                            for (int i = 0; i < NumThreads; i++)
                                histogram.Add(saved[i]);
                        }

                        if (printHeader)
                        {
                            printHeader = false;
                            Console.WriteLine("min (us); 5th (us); median (us); avg (us); 95th (us); 99th (us); 99.9th (us); cnt; tpt (Kops/sec)");
                        }
                        Console.WriteLine("{0:0.0}; {1:0.0}; {2:0.0}; {3:0.0}; {4:0.0}; {5:0.0}; {6:0.0}; {7:0.0}; {8:N2}",
                            histogram.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetMean() / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds,
                            histogram.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds,
                            total_ops_done,
                            BatchSize * total_ops_done / (double)swatch.ElapsedMilliseconds
                            );
                        // histogram.OutputPercentileDistribution(Console.Out, 1, OutputScalingFactor.TimeStampToMicroseconds);

                        summary.Add(histogram);
                        total_ops_done = 0;
                        histogram.Reset();
                        if (iteration % resetInterval == 0)
                            swatch.Restart();
                        else
                            swatch.Start();
                    }
                    waiter.Set();
                }
                if (iteration == runDuration)
                {
                    Interlocked.Add(ref pendingRunners, -NumThreads);
                    while (pendingRunners < 0) Thread.Yield();
                    break;
                }
            }
            swatch.Stop();

            foreach (Thread worker in workers)
                worker.Join();

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            double opsPerSecond = total_ops_done / seconds;

            Console.WriteLine($"Total time: {swatch.ElapsedMilliseconds:N2}ms for {total_ops_done:N2} ops");
            Console.WriteLine($"Throughput: {opsPerSecond:N2} ops/sec");
            DumpHistogram(summary);
            summary.Return();
            histogram.Return();
            total_ops_done = 0;
            waiter.Reset();
        }


        public void OpRunnerSERedis(int thread_id)
        {
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);

            var histogram = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            saved[thread_id] = histogram;

            waiter.Wait();
            int iteration = -reportInterval;
            int local_ops_done = 0;
            Random r = new(thread_id + 100);
            while (true)
            {
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                long startTimestamp = Stopwatch.GetTimestamp();

                var rd = opts.Pool ? redisPool.Get() : redis;
                var db = rd.GetDatabase(0);
                ITransaction txn;
                bool committed;
                switch (op)
                {
                    case OpType.READ_TXN:
                        txn = db.CreateTransaction();
                        for (int i = 0; i < readPerTxn; i++)
                            _ = txn.StringGetAsync(req.GenerateKey());
                        local_ops_done += readPerTxn;
                        committed = txn.Execute();
                        if (!committed)
                            throw new Exception("aborted txn");
                        break;
                    case OpType.WRITE_TXN:
                        txn = db.CreateTransaction();
                        for (int i = 0; i < writePerTxn; i++)
                            _ = txn.StringSetAsync(req.GenerateKey(), req.GenerateValue());
                        local_ops_done += writePerTxn;
                        committed = txn.Execute();
                        if (!committed)
                            throw new Exception("aborted txn");
                        break;
                    case OpType.READWRITETX:
                        txn = db.CreateTransaction();
                        local_ops_done += (readPerTxn + writePerTxn);
                        for (int i = 0; i < readPerTxn; i++)
                            _ = txn.StringGetAsync(req.GenerateKey());
                        for (int i = 0; i < writePerTxn; i++)
                            _ = txn.StringSetAsync(req.GenerateKey(), req.GenerateValue());

                        committed = txn.Execute();
                        if (!committed)
                            throw new Exception("aborted txn");
                        break;
                    case OpType.WATCH_TXN:
                        txn = db.CreateTransaction();
                        local_ops_done += (readPerTxn + writePerTxn);
                        for (int i = 0; i < readPerTxn; i++)
                        {
                            string key = req.GenerateKey();
                            txn.AddCondition(Condition.StringNotEqual(key, "0"));
                        }
                        for (int i = 0; i < writePerTxn; i++)
                            _ = txn.StringSetAsync(req.GenerateKey(), req.GenerateValue());

                        committed = txn.Execute();
                        if (!committed)
                            throw new Exception("aborted txn");
                        break;
                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }
                if (opts.Pool)
                {
                    redisPool.Return(rd);
                }

                long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                histogram.RecordValue(elapsed);
                if (pendingRunners != 0)
                {
                    if (!ResetRunner(ref iteration, ref local_ops_done, ref histogram))
                        break;
                }
            }
        }

        public void LoadData()
        {
            var req = new OnlineReqGen(0, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength);
            GarnetClientSession client = new(new IPEndPoint(IPAddress.Parse(address), port), new(), tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
            client.Connect();
            if (auth != null)
            {
                client.Execute("AUTH", auth);
                client.CompletePending();
            }

            for (int key = 0; key < opts.DbSize; key++)
                client.Execute(["SET", req.GenerateExactKey(key), req.GenerateValue()]);
        }


        private bool ResetRunner(ref int iteration, ref int local_ops_done, ref LongHistogram histogram)
        {
            iteration += reportInterval;
            if (pendingRunners < 0)
            {
                Interlocked.Increment(ref pendingRunners);
                return false;
            }

            Interlocked.Add(ref total_ops_done, local_ops_done);
            Interlocked.Decrement(ref pendingRunners);
            waiter.Wait();
            if (iteration % resetInterval == 0)
            {
                histogram.Reset();
                local_ops_done = 0;
            }
            // if (iteration == 0) local_ops_done = 0;
            return true;
        }
    }
}