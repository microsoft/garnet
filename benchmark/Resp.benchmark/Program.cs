// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using CommandLine;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Resp.benchmark
{
    /// <summary>
    /// Benchmarking client. Example command-line args:
    ///   "--op GET -t 1,32"
    ///   "--op GET -t 1,2,4,8,16,32,64"
    ///   "--op INCR -t 1,2,4,8,16,32 --valuelength 0"
    ///   "--op ZADDREM --dbsize 2048 -b 1024 -t 1,2"
    ///   "--op PFADD -t 32 --batchsize 1024 --dbsize 64"
    ///   "--op BITCOUNT -t 1 --dbsize 256 --valuelength 1048576"
    ///   "--op BITOP_AND -t 1 --dbsize 256 --valuelength 1048576"
    ///   "--op SETBIT -t 1 --dbsize 256 --valuelength 1048576"
    ///   "--op BITFIELD_SET -t 1 --dbsize 256 --valuelength 1048576"
    ///   "--op GEOADDREM --dbsize 2048 -b 256 -t 1,2"
    /// </summary>

    class Program
    {
        public static IConnectionMultiplexer redis;
        public static ILoggerFactory loggerFactory;
        public static PeriodicCheckpointer pc = null;

        static ILoggerFactory CreateLoggerFactory(Options opts)
        {
            return LoggerFactory.Create(builder =>
            {
                if (!opts.DisableConsoleLogger)
                {
                    builder.AddProvider(new BenchmarkLoggerProvider(Console.Out));
                }

                // Optional: Flush log output to file.
                if (opts.FileLogger != null)
                    builder.AddFile(opts.FileLogger);
                builder.SetMinimumLevel(opts.LogLevel);
            });
        }

        static void PrintBenchMarkSummary(Options opts)
        {
            Console.WriteLine("<<<<<<< Benchmark Configuration >>>>>>>>");
            Console.WriteLine($"benchmarkType: {(opts.Online ? "Online" : "Throughput")}");
            Console.WriteLine($"skipLoad: {(opts.SkipLoad ? "Enabled" : "Disabled")}");
            Console.WriteLine($"DBsize: {opts.DbSize}");
            Console.WriteLine($"TotalOps: {opts.TotalOps}");
            Console.WriteLine($"Op Benchmark: {opts.Op}");
            Console.WriteLine($"KeyLength: {opts.KeyLength}");
            Console.WriteLine($"ValueLength: {opts.ValueLength}");
            Console.WriteLine($"BatchSize: {string.Join(",", opts.BatchSize.ToList())}");
            Console.WriteLine($"RunTime: {opts.RunTime}");
            Console.WriteLine($"NumThreads: {string.Join(",", opts.NumThreads.ToList())}");
            Console.WriteLine($"Auth: {opts.Auth}");
            Console.WriteLine($"Load Using SET: {(opts.LSet ? "Enabled" : "Disabled")}");
            Console.WriteLine($"ClientType used for benchmarking: {opts.Client}");
            if (opts.Online)
            {
                Console.WriteLine($"Pool: {opts.Pool}");
                Console.WriteLine($"OpWorkload {String.Join(',', opts.OpWorkload)}");
                Console.WriteLine($"OpPercent {String.Join(',', opts.OpPercent)}");
                if (opts.Ttl > 0)
                {
                    Console.WriteLine($"Ttl: {opts.Ttl}");
                }

                if (opts.SortedSetCardinality > 0)
                {
                    Console.WriteLine($"Sorted set cardinality: {opts.SortedSetCardinality}");
                }
                Console.WriteLine($"Intra thread parallelism: {opts.IntraThreadParallelism}");
                Console.WriteLine($"SyncMode: {opts.SyncMode}");
            }
            if (opts.Op == OpType.SETEX && opts.Ttl > 0)
            {
                Console.WriteLine($"Ttl: {opts.Ttl}");
            }
            Console.WriteLine($"TLS: {(opts.EnableTLS ? "Enabled" : "Disabled")}");
            Console.WriteLine($"ClientHistogram: {opts.ClientHistogram}");
            ThreadPool.SetMinThreads(workerThreads: 1000, completionPortThreads: 1000);
            ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);
            Console.WriteLine($"minWorkerThreads: {minWorkerThreads}");
            Console.WriteLine($"minCompletionPortThreads: {minCompletionPortThreads}");
            Console.WriteLine("----------------------------------");

            if (opts.Client == ClientType.InProc)
            {
                Console.WriteLine("------EMBEDDED-SERVER-CONFIG------");
                Console.WriteLine($"aof:{opts.EnableAOF || opts.AofBench}");
                Console.WriteLine($"aof-null-device:{opts.UseAofNullDevice}");
                Console.WriteLine($"aof-commit-freq:{opts.CommitFrequencyMs}");
                Console.WriteLine($"aof-memory-size:{opts.AofMemorySize}");
                Console.WriteLine($"aof-page-size:{opts.AofPageSize}");
                Console.WriteLine($"cluster:{opts.EnableCluster}");
                Console.WriteLine($"index:{opts.IndexSize}");
                Console.WriteLine($"aof-sublog-count:{opts.AofPhysicalSublogCount}");
                Console.WriteLine("----------------------------------");
            }
        }

        static bool DisabledFeatures(Options opts)
        {
            HashSet<OpType> seRedisSupportOpSet = new();
            seRedisSupportOpSet.Add(OpType.GET);
            seRedisSupportOpSet.Add(OpType.SET);
            seRedisSupportOpSet.Add(OpType.SETEX);
            seRedisSupportOpSet.Add(OpType.PING);

            if (opts.Online)
            {
                if (opts.Op != OpType.GET)
                {
                    Console.WriteLine($"OpType not supported for online benchmark (defaults to get/set)");
                    return true;
                }
            }

            if (opts.Online)
            {
                var batches = opts.BatchSize.ToList();
                if (batches.Count != 1 || batches[0] != 1)
                {
                    Console.WriteLine("Batch size parameter should be one entry of size 1, for the online benchmark, setting to [ 1 ]. Use --itp to control batch size instead.");
                    opts.BatchSize = [1];
                }
                if (opts.DbSize < opts.SortedSetCardinality)
                {
                    Console.WriteLine("DB size cannot be smaller than SortedSet cardinality");
                    return true;
                }

                if (opts.Client == ClientType.LightClient && opts.Pool)
                {
                    Console.WriteLine("Pooling of LightClient is not supported");
                    return true;
                }

                if (opts.ClientHistogram && opts.Pool)
                {
                    Console.WriteLine("Client hisogram and pool are not supported at the same time.");
                    return true;
                }
            }
            else
            {
                if (opts.Op == OpType.SETEX && opts.Ttl <= 0)
                {
                    Console.WriteLine("TTL must be positive for SETEX operations.");
                    return true;
                }
            }

            if (opts.EnableTLS && opts.CertFileName == null)
            {
                Console.WriteLine("Certificate file name is required for TLS");
                return true;
            }

            if (!opts.EnableTLS && opts.CertFileName != null)
            {
                Console.WriteLine("Certificate file name is not required for non-TLS");
                return true;
            }
            return false;
        }

        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            if (DisabledFeatures(opts))
                return;

            loggerFactory = CreateLoggerFactory(opts);

            if (opts.Client != ClientType.InProc)
                WaitForServer(opts);

            if (opts.SaveFreqSecs > 0)
            {
                pc = new PeriodicCheckpointer(opts.SaveFreqSecs * 1000);
                new Thread(() => pc.Start(opts.Address, opts.Port)).Start();
            }

            if (opts.Client == ClientType.SERedis)
                redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost));

            PrintBenchMarkSummary(opts);

            if (opts.Op == OpType.PFADD || opts.Op == OpType.PFCOUNT || opts.Op == OpType.PFMERGE)
                RunHLLBenchmark(opts);
            else
                RunBasicCommandsBenchmark(opts);

            pc?.Stop();
        }

        static void WaitForServer(Options opts)
        {
            using var client = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port), new(), tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
            while (true)
            {
                try
                {
                    client.Connect();
                    client.Execute("QUIT");
                }
                catch
                {
                    Console.WriteLine($"Waiting for server at {opts.Address}:{opts.Port}");
                    Thread.Sleep(1000);
                    continue;
                }
                break;
            }
        }

        static void RunBasicCommandsBenchmark(Options opts)
        {
            int[] threadBench = [.. opts.NumThreads];
            int keyLen = opts.KeyLength;
            int valueLen = opts.ValueLength;

            if (opts.Op == OpType.PUBLISH || opts.Op == OpType.SPUBLISH || opts.Op == OpType.ZADD || opts.Op == OpType.ZREM || opts.Op == OpType.ZADDREM || opts.Op == OpType.PING || opts.Op == OpType.GEOADD || opts.Op == OpType.GEOADDREM || opts.Op == OpType.SETEX || opts.Op == OpType.ZCARD || opts.Op == OpType.ZADDCARD)
                opts.SkipLoad = true;

            //if we have scripts ops we need to load them in memory
            if (opts.Op == OpType.SCRIPTGET || opts.Op == OpType.SCRIPTSET || opts.Op == OpType.SCRIPTRETKEY)
            {
                unsafe
                {
                    var onResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);
                    using var client = new LightClient(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port), (int)OpType.GET, onResponseDelegate, opts.DbSize, opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                    client.Connect();
                    client.Authenticate(opts.Auth);
                    BenchUtils.LoadSetGetScripts(client, out BenchUtils.sha1SetScript, out BenchUtils.sha1GetScript);
                }
            }

            if (opts.Online)
            {
                if (opts.SkipLoad)
                    throw new Exception("Skipload not supported with --online");
                var bench = new RespOnlineBench(opts, runDuration: opts.RunTime == -1 ? int.MaxValue : opts.RunTime, loggerFactory: loggerFactory);
                bench.Run();
                return;
            }
            else if (opts.Txn)
            {
                if (opts.SkipLoad)
                    throw new Exception("Skipload not supported with --txn");

                var bench = new TxnPerfBench(opts, runDuration: opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
                bench.LoadData();
                bench.Run();
                return;
            }
            else if (opts.AofBench)
            {
                if (opts.AofBenchType == AofBenchType.Replay)
                {
                    var bench = new AofBench(opts);
                    bench.GenerateData();
                    bench.Run(opts.AofPhysicalSublogCount);
                }
                else
                {
                    var bench = new AofBench(opts);
                    bench.GenerateData();

                    foreach (var threadCount in opts.NumThreads)
                        bench.Run(threadCount);
                }
            }
            else
            {
                var bench = new RespPerfBench(opts, 0, redis);

                if (!opts.SkipLoad)
                    bench.LoadData(keyLen: keyLen, valueLen: valueLen, numericValue: opts.Op == OpType.INCR);

                foreach (var BatchSize in opts.BatchSize)
                    bench.Run(
                        opts.Op,
                        opts.TotalOps,
                        threadBench, runTime: TimeSpan.FromSeconds(opts.RunTime),
                        keyLen: keyLen,
                        valueLen: valueLen,
                        BatchSize: BatchSize,
                        ttl: opts.Ttl);
            }
        }

        static void RunHLLBenchmark(Options opts)
        {
            var bench = new RespPerfBench(opts, 0, redis);
            int[] threadBench = [.. opts.NumThreads];

            int loadThreads = 8;
            int loadBatchSize = opts.DbSize / loadThreads;
            loadBatchSize = opts.DbSize < 4096 ? loadBatchSize : 4096;

            if (opts.SkipLoad && opts.Op != OpType.PFADD)
                throw new Exception(opts.Op + " cannot be benchmarked using the skipload option");

            if (!opts.SkipLoad)
                bench.LoadHLLData(loadDbThreads: loadThreads, BatchSize: loadBatchSize);

            //PFCOUNT, PFMERGE
            foreach (int BatchSize in opts.BatchSize)
                bench.Run(opts.Op, opts.TotalOps, threadBench, keyLen: opts.KeyLength, valueLen: opts.ValueLength, BatchSize: BatchSize);
        }
    }
}