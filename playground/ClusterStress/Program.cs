// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using CommandLine;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Resp.benchmark;
using StackExchange.Redis;

namespace ClusterStress
{
    class Program
    {
        public static IConnectionMultiplexer redis;
        public static ILoggerFactory loggerFactory;

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

        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            loggerFactory = CreateLoggerFactory(opts);

            if (opts.Client == Resp.benchmark.ClientType.SERedis)
                redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost));

            if (opts.Cluster)
                RunShardedBasicCommandsBenchmark(opts);
        }

        static void RunShardedBasicCommandsBenchmark(Options opts)
        {
            if (opts.Online)
            {
                if (opts.SkipLoad)
                    throw new Exception("Skipload not supported with --online");
                var bench = new ShardedRespOnlineBench(opts, runDuration: opts.RunTime == -1 ? int.MaxValue : opts.RunTime, loggerFactory: loggerFactory);
                bench.Run();
            }
            else
            {
                var bench = new ShardedRespPerfBench(opts, 0);
                if (!opts.SkipLoad)
                    bench.LoadData(keyLen: opts.KeyLength, valueLen: opts.ValueLength, BatchSize: opts.BatchSize.First());

                int[] threadBench = opts.NumThreads.ToArray();
                int keyLen = opts.KeyLength;
                int valueLen = opts.ValueLength;
                foreach (int BatchSize in opts.BatchSize)
                    bench.Run(
                        opts.Op,
                        opts.DbSize,
                        threadBench,
                        runTime: TimeSpan.FromSeconds(opts.RunTime),
                        keyLen: keyLen,
                        valueLen: valueLen,
                        BatchSize: BatchSize,
                        ttl: opts.Ttl);
            }
        }
    }
}
