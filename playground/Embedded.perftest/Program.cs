// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using CommandLine;
using Garnet.common.Logging;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.perftest
{
    /// <summary>
    /// Performance test for the core processing loop of embedded (in-process) Garnet instances.
    /// 
    /// NOTE: This performance test is designed to specifically stress-test the core RESP processing loop
    ///       of Garnet and, as such, tries to keep actual data manipulation minimal.
    ///       In its current form this is not a replacement for a full benchmark that can execute
    ///       on a larger data set.
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
        {
            // Parse and initialize test parameters
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);

            if (result.Tag == ParserResultType.NotParsed)
            {
                return;
            }

            var opts = result.MapResult(o => o, xs => new Options());

            var loggerFactory = CreateLoggerFactory(opts);

            // Create embedded Garnet server in-process
            GarnetServerOptions opt = new GarnetServerOptions
            {
                QuietMode = true
            };
            using var server = new EmbeddedRespServer(opt, loggerFactory);

            PrintBenchmarkConfig(opts);

            // Run performance test
            var perfTest = new EmbeddedPerformanceTest(server, opts, loggerFactory);
            perfTest.Run();

            return;
        }

        /// <summary>
        /// Create logger factory with the given test options
        /// </summary>
        static ILoggerFactory CreateLoggerFactory(Options opts)
        {
            return LoggerFactory.Create(builder =>
            {
                // Unless disabled, add console logger provider
                if (!opts.DisableConsoleLogger)
                {
                    builder.AddProvider(new PerformanceTestLoggerProvider(Console.Out));
                }

                // Optional: Flush log output to file.
                if (opts.FileLogger != null)
                {
                    builder.AddFile(opts.FileLogger);
                }

                // Set logging level
                builder.SetMinimumLevel(opts.LogLevel);
            });
        }

        /// <summary>
        /// Print a human-readable representation of the given test options
        /// </summary>
        /// <param name="opts">Options to print</param>
        static void PrintBenchmarkConfig(Options opts)
        {
            Console.WriteLine("============== Configuration ==============");
            Console.WriteLine($"Run time     : {opts.RunTime} seconds");
            Console.WriteLine($"# Threads    : {String.Join(", ", opts.NumThreads)}");
            Console.WriteLine($"Batch Size   : {opts.BatchSize} operations");
            Console.WriteLine($"OpWorkload   : {String.Join(", ", opts.OpWorkload)}");
            Console.WriteLine($"OpPercent    : {String.Join(", ", opts.OpPercent)}");
            Console.WriteLine($"RNG Seed     : {opts.Seed}");
            Console.WriteLine("===========================================");
        }
    }
}