// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    class Program
    {
        static ILoggerFactory CreateLoggerFactory(Options opts)
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new BenchmarkLoggerProvider(Console.Out));
                builder.SetMinimumLevel(LogLevel.Trace);
            });
        }

        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            var loggerFactory = CreateLoggerFactory(opts);

            if (opts.Endpoints.Count() == 0)
            {
                MigrateRequest mreq = new(opts, logger: loggerFactory.CreateLogger("MigrateBench"));
                mreq.Run();
            }
            else
            {
                MigrateSlotWalk mwalk = new(opts, logger: loggerFactory.CreateLogger("MigrateWalk"));
                mwalk.Run();
            }
        }
    }
}