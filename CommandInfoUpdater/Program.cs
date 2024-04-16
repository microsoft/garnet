// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using CommandInfoUpdater;
using CommandLine;
using Microsoft.Extensions.Logging;

class Program
{
    static void Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "hh::mm::ss ";
        }));
        ILogger logger = loggerFactory.CreateLogger<Program>();

        Options? config = default;
        Parser.Default.ParseArguments<Options>(args).WithParsed(op => config = op).WithNotParsed(errs =>
        {
            logger.LogError($"Encountered one or more errors while parsing arguments:");
            foreach (var err in errs)
            {
                logger.LogError(err.ToString());
            }
        });

        if (config == null) return;

        if (!IPAddress.TryParse(config.LocalRedisHost, out var localRedisHost))
        {
            logger.LogError("Unable to parse local Redis host from arguments");
            return;
        }

        CommandInfoUpdater.CommandInfoUpdater.TryUpdateCommandInfo(config.OutputPath, config.LocalRedisPort, localRedisHost, config.IgnoreCommands, logger);
    }
}