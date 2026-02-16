// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
    internal class ShardedRespOnlineBench
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);
        const int bufferSizeValue = 1 << 17;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool IsValidRange(long value)
            => value < HISTOGRAM_UPPER_BOUND && value > HISTOGRAM_LOWER_BOUND;

        readonly int NumThreads;
        readonly int BatchSize;
        readonly Options opts;
        readonly string auth;

        readonly ManualResetEventSlim waiter = new();
        readonly LongHistogram[] thread_histograms;
        readonly ManualResetEventSlim WaitForEpochBump = new();
        readonly int runDuration;
        readonly int resetInterval;
        ulong iteration = 0;

        readonly int[] opPercent;
        readonly OpType[] opWorkload;

        readonly ClusterConfiguration clusterConfig;
        readonly ushort[] slotMap = new ushort[16384];
        ClusterNode[] primaryNodes;
        ClusterNode[] replicaNodes;

        GarnetClient[] gclient = null;
        GarnetClientSession[][] gcs = null;

        readonly LightEpoch epoch = new();
        readonly Stopwatch epochWatch = new();

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        AsyncPool<IConnectionMultiplexer> redisPool;
        IConnectionMultiplexer redis;

        readonly CancellationTokenSource cts = new();
        volatile int workerCount = 0;

        public ShardedRespOnlineBench(Options opts, int resetInterval = 30, int runDuration = int.MaxValue, ILoggerFactory loggerFactory = null)
        {
            this.runDuration = runDuration;
            this.resetInterval = resetInterval;
            this.opts = opts;
            this.auth = opts.Auth;
            NumThreads = opts.NumThreads.ToArray()[0];
            BatchSize = opts.BatchSize.ToArray()[0];
            thread_histograms = new LongHistogram[NumThreads * 2];
            for (int i = 0; i < thread_histograms.Length; i++)
                thread_histograms[i] = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);


            opPercent = opts.OpPercent?.ToArray();
            opWorkload = opts.OpWorkload?.ToArray();

            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("sonline");

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

            clusterConfig = GetClusterConfig();
        }

        OpType SelectOpType(int percent)
        {
            for (int i = 0; i < opPercent.Length; i++)
                if (percent <= opPercent[i])
                    return opWorkload[i];
            throw new Exception($"Invalid percent range {percent}");
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

        private void PrintClusterConfig()
        {
            Console.WriteLine("Cluster Retrieved Configuration...");
            var nodes = clusterConfig.Nodes.ToArray();
            Array.Sort(nodes, (x, y) => ((IPEndPoint)x.EndPoint).Address.ToString().CompareTo(((IPEndPoint)y.EndPoint).Address.ToString()));
            foreach (var node in nodes)
            {
                var endpoint = (IPEndPoint)node.EndPoint;
                Console.Write($"host: {endpoint.Address}:{endpoint.Port}, ");
                Console.Write($"role: {((node.IsReplica || node.Slots.Count == 0) ? "REPLICA" : "PRIMARY")}, ");
                var slotRanges = node.Slots;

                Console.Write("slotRanges: ");
                foreach (var slotRange in slotRanges)
                    Console.Write($"{slotRange.From} - {slotRange.To} ");
                Console.WriteLine("");
            }
            Console.WriteLine("<--------------------------------------------->");
        }

        private ClusterConfiguration GetClusterConfig()
        {
            using var redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));
            var clusterConfig = redis.GetServer(opts.Address + ":" + opts.Port).ClusterNodes();

            UpdateSlotMap(clusterConfig);

            return clusterConfig;
        }

        private void UpdateSlotMap(ClusterConfiguration clusterConfig)
        {
            var nodes = clusterConfig.Nodes.ToArray();
            primaryNodes = [.. nodes.ToList().FindAll(p => !p.IsReplica)];
            replicaNodes = [.. nodes.ToList().FindAll(p => p.IsReplica)];
            ushort j = 0;
            foreach (var node in nodes)
            {
                var slotRanges = node.Slots;
                foreach (var slotRange in slotRanges)
                {
                    for (int i = slotRange.From; i <= slotRange.To; i++)
                    {
                        slotMap[i] = j;
                    }
                }
                j++;
            }
        }

        private void InitClients(ClusterNode[] nodes)
        {
            switch (opts.Client)
            {
                case ClientType.GarnetClient:
                    gclient = new GarnetClient[nodes.Length];
                    for (int i = 0; i < nodes.Length; i++)
                    {
                        var endpoint = (IPEndPoint)nodes[i].EndPoint;
                        gclient[i] = new GarnetClient(endpoint,
                            opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null,
                            recordLatency: opts.ClientHistogram);
                        gclient[i].Connect();
                    }
                    break;
                case ClientType.GarnetClientSession:
                    gcs = new GarnetClientSession[NumThreads][];
                    for (int j = 0; j < NumThreads; j++)
                    {
                        gcs[j] = new GarnetClientSession[nodes.Length];
                        for (int i = 0; i < nodes.Length; i++)
                        {
                            var endpoint = (IPEndPoint)nodes[i].EndPoint;
                            gcs[j][i] = new GarnetClientSession(
                                endpoint,
                                new(Math.Max(bufferSizeValue, opts.IntraThreadParallelism * opts.ValueLength)),
                                tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                            gcs[j][i].Connect();
                            if (auth != null)
                            {
                                gcs[j][i].Execute("AUTH", auth);
                                gcs[j][i].CompletePending();
                            }
                        }
                    }
                    break;
                case ClientType.SERedis:
                    if (opts.Pool)
                    {
                        redisPool = new AsyncPool<IConnectionMultiplexer>(opts.NumThreads.First(), () =>
                        {
                            return ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost));
                        });
                    }
                    else
                    {
                        redis = ClusterStress.Program.redis;
                    }
                    break;
                default:
                    throw new Exception($"ClientType {opts.Client} not supported");
            }
        }

        private Thread[] InitializeThreadWorkers()
        {
            Thread[] workers = new Thread[NumThreads];
            for (int idx = 0; idx < NumThreads; ++idx)
            {
                int x = idx;
                switch (opts.Client)
                {
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
                        if (opts.IntraThreadParallelism > 1)
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

        private async void PeriodicConfigUpdate()
        {
            using var redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                if (cts.IsCancellationRequested) return;
                var clusterConfig = redis.GetServer(opts.Address + ":" + opts.Port).ClusterNodes();
                UpdateSlotMap(clusterConfig);
            }
        }

        private async void MigrationBgTask()
        {
            using var redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));
            Random r = new(7638);
            try
            {
                while (true)
                {
                    //Check if cancellation is requested
                    await Task.Delay(TimeSpan.FromSeconds(opts.MigrateSlotsFreq));
                    if (cts.IsCancellationRequested) return;

                    //Retrieve latest cluster config and update client slotMap
                    var clusterConfig = redis.GetServer(opts.Address + ":" + opts.Port).ClusterNodes();
                    UpdateSlotMap(clusterConfig);

                    //Initiate a migration operation between nodes
                    InitiateMigration(redis, r);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Migration bg task failed");
            }
        }

        private void InitiateMigration(ConnectionMultiplexer redis, Random r)
        {
            List<int> migratingSlots = new();
            //Initiate a migration operation between nodes
            int source = r.Next(0, primaryNodes.Length);
            int target = r.Next(0, primaryNodes.Length);

            //Choose target different from source
            while (target == source)
            {
                target = r.Next(0, primaryNodes.Length);
                if (cts.IsCancellationRequested) return;
            }

            //Retrieve source and target info
            var sourceNode = primaryNodes[source];
            var targetNode = primaryNodes[target];
            var sourceNodeEndpoint = (IPEndPoint)sourceNode.EndPoint;
            var targetNodeEndpoint = (IPEndPoint)targetNode.EndPoint;

            //Pick randomly a few number of slots to migrate
            foreach (var slotRange in sourceNode.Slots)
            {
                for (int i = slotRange.From; i < slotRange.To; i++)
                    if (r.Next(0, 2) > 0 && migratingSlots.Count < opts.MigrateBatch)
                        migratingSlots.Add(i);
            }

            Console.WriteLine($"{sourceNodeEndpoint.Address}:{sourceNodeEndpoint.Port} > {targetNodeEndpoint.Address}:{targetNodeEndpoint.Port} slots:{migratingSlots.Count}");
            //Initiate migration
            if (migratingSlots.Count > 0)
                MigrateSlots(redis, sourceNodeEndpoint, targetNodeEndpoint, migratingSlots);

            //Clear migration list
            migratingSlots.Clear();
        }

        public static void MigrateSlots(ConnectionMultiplexer redis, IPEndPoint source, IPEndPoint target, List<int> slots, bool range = false, ILogger logger = null)
        {
            //MIGRATE host port <key | ""> destination-db timeout [COPY] [REPLACE] [[AUTH password] | [AUTH2 username password]] [KEYS key [key...]]
            var server = redis.GetServer(source);
            List<object> args = new()
            {
                target.Address.ToString(),
                target.Port,
                "",
                0,
                -1,
                range ? "SLOTSRANGE": "SLOTS"
            };
            foreach (var slot in slots)
                args.Add(slot);

            try
            {
                var resp = server.Execute("migrate", args);
                if (!resp.Equals("OK"))
                    logger?.LogError("{errorMessage}", resp.ToString());
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
        }

        public void Run()
        {
            PrintClusterConfig();
            Console.WriteLine($"Running benchmark using {opts.Client} client type");

            // Initialize clients to nodes using the retrieved configuration
            InitClients([.. clusterConfig.Nodes]);
            Thread[] workers = InitializeThreadWorkers();

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

            //Pull config to update slotmap periodically
            Task.Run(PeriodicConfigUpdate);

            if (opts.MigrateSlotsFreq > 0)
                Task.Run(MigrationBgTask);

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
                        var histogramHeader = $"{"min (us);",pad}" +
                            $"{"5th (us);",pad}" +
                            $"{"median (us);",pad}" +
                            $"{"avg (us);",pad}" +
                            $"{"95th (us);",pad}" +
                            $"{"99th (us);",pad}" +
                            $"{"99.9th (us);",pad}" +
                            $"{"total_ops;",pad}" +
                            $"{"iter_tops;",pad}" +
                            $"{"tpt (Kops/sec)",pad}";
                        logger.Log(LogLevel.Information, "{msg}", histogramHeader);
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
            epoch.Dispose();
        }

        public async void OpRunnerGarnetClient(int thread_id)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                byte[] reqKey = req.GenerateKeyBytes(out int slot);
                Memory<byte> valueData = req.GenerateValueBytes();
                int clientIdx = slotMap[slot];

                long startTimestamp = Stopwatch.GetTimestamp();
                switch (op)
                {
                    case OpType.GET:
                        await gclient[clientIdx].StringGetAsMemoryAsync(reqKey);
                        break;
                    case OpType.SET:
                        await gclient[clientIdx].StringSetAsync(reqKey, valueData);
                        break;
                    case OpType.DEL:
                        await gclient[clientIdx].KeyDeleteAsync(reqKey);
                        break;
                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }

                long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                RecordValue(thread_id, elapsed);
            }
            Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClientParallel(int thread_id, int parallel)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);
            Task[] tasks = new Task[parallel];
            int offset = 0;

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                byte[] reqKey = req.GenerateKeyBytes(out int slot);
                Memory<byte> valueData = req.GenerateValueBytes();
                int clientIdx = slotMap[slot];

                long startTimestamp = Stopwatch.GetTimestamp();
                tasks[offset++] = op switch
                {
                    OpType.GET => gclient[clientIdx].StringGetAsMemoryAsync(reqKey),
                    OpType.SET => gclient[clientIdx].StringSetAsync(reqKey, valueData),
                    OpType.DEL => gclient[clientIdx].KeyDeleteAsync(reqKey),
                    _ => throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!"),
                };

                if (offset == parallel)
                {
                    await Task.WhenAll(tasks);
                    offset = 0;
                    long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    RecordValue(thread_id, elapsed);
                }
            }
            Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerGarnetClientSession(int thread_id)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);
            var _gcs = gcs[thread_id];

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                string reqKey = req.GenerateKeyInSlot(out int slot);
                string valueData = req.GenerateValue();
                int clientIdx = slotMap[slot];

                long startTimestamp = Stopwatch.GetTimestamp();
                try
                {
                    switch (op)
                    {
                        case OpType.GET:
                            await _gcs[clientIdx].ExecuteAsync(["GET", reqKey]);
                            break;
                        case OpType.SET:
                            await _gcs[clientIdx].ExecuteAsync(["SET", reqKey, valueData]);
                            break;
                        case OpType.DEL:
                            await _gcs[clientIdx].ExecuteAsync(["DEL", reqKey]);
                            break;
                        default:
                            throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                    }
                }
                catch (Exception e)
                {
                    //if(e.ToString().Contains())
                    if (e.ToString().Contains("CLUSTERDOWN"))
                        logger?.LogError(e, "An error has occurred");
                }

                long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                RecordValue(thread_id, elapsed);
            }
            Interlocked.Decrement(ref workerCount);
        }

        public void OpRunnerGarnetClientSessionParallel(int thread_id, int parallel)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);
            int offset = 0;
            var _gcs = gcs[thread_id];

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                string reqKey = req.GenerateKey(out int slot);
                string valueData = req.GenerateValue();
                int clientIdx = slotMap[slot];

                long startTimestamp = Stopwatch.GetTimestamp();
                switch (op)
                {
                    case OpType.GET:
                        _gcs[clientIdx].ExecuteBatch(["GET", reqKey]);
                        break;
                    case OpType.SET:
                        _gcs[clientIdx].ExecuteBatch(["SET", reqKey, valueData]);
                        break;
                    case OpType.DEL:
                        _gcs[clientIdx].ExecuteBatch(["DEL", reqKey]);
                        break;
                    default:
                        throw new Exception($"opType: {op} benchmark not supported with {opts.Client} ClientType!");
                }

                offset++;
                if (offset == parallel)
                {
                    for (int i = 0; i < _gcs.Length; i++) _gcs[i].CompletePending(false);
                    if (!opts.Burst)
                        for (int i = 0; i < _gcs.Length; i++) _gcs[i].Wait();
                    long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    offset = 0;
                    RecordValue(thread_id, elapsed);
                }
            }
            Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerSERedis(int thread_id)
        {
            Interlocked.Increment(ref workerCount);
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);

            waiter.Wait();
            while (true)
            {
                if (cts.IsCancellationRequested) break;
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                long startTimestamp = Stopwatch.GetTimestamp();

                var rd = opts.Pool ? redisPool.Get() : redis;
                var db = rd.GetDatabase(0);
                switch (op)
                {
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
                {
                    redisPool.Return(rd);
                }

                long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                RecordValue(thread_id, elapsed);
            }
            Interlocked.Decrement(ref workerCount);
        }

        public async void OpRunnerSERedisParallel(int thread_id, int parallel)
        {
            if (opts.BatchSize.First() != 1)
                throw new Exception("Only batch size 1 supported for online bench");
            var req = new OnlineReqGen(thread_id, opts.DbSize, true, opts.Zipf, opts.KeyLength, opts.ValueLength, cluster: true);
            Random r = new(thread_id + 100);
            Task[] tasks = new Task[parallel];
            int offset = 0;

            waiter.Wait();
            while (true)
            {
                var rand = r.Next(100);
                OpType op = SelectOpType(rand);
                long startTimestamp = Stopwatch.GetTimestamp();

                var rd = opts.Pool ? await redisPool.GetAsync() : redis;
                var db = rd.GetDatabase(0);
                switch (op)
                {
                    case OpType.GET:
                        tasks[offset++] = db.StringGetAsync(req.GenerateKey());
                        break;
                    case OpType.SET:
                        tasks[offset++] = db.StringSetAsync(req.GenerateKey(), req.GenerateValue());
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
                {
                    redisPool.Return(rd);
                }

                if (offset == parallel)
                {
                    await Task.WhenAll(tasks);
                    offset = 0;
                    long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                    RecordValue(thread_id, elapsed);
                }
            }
        }
    }
}