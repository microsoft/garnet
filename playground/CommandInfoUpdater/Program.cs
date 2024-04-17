// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using CommandInfoUpdater;
using CommandLine;
using CommandLine.Text;
using Microsoft.Extensions.Logging;

/// <summary>
/// This tool helps generate an updated JSON file containing Garnet's supported commands info.
/// For this tool to run successfully, it needs to be able to query a running Redis server in order to parse its RESP command info
/// (unless you are only removing commands and/or you are adding commands that are not supported by Redis)
/// 
/// To run this tool:
/// a) Make the desired changes to AllSupportedCommands in SupportedCommand.cs (i.e. add / remove supported commands / sub-commands)
/// b) If you're adding commands / sub-commands that are not supported by Redis, manually insert their command info into GarnetCommandsInfo.json.
/// c) Build and run the tool. You'll need to specify an output path and optionally the Redis server host & port (if different than default).
///    Run the tool with -h or --help for more information.
/// d) Replace Garnet's RespCommandsInfo.json file contents with the contents of the updated file.
/// e) Rebuild Garnet to include the latest changes.
/// </summary>
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

        var parser = new Parser(settings =>
        {
            settings.AutoHelp = false;
        });

        var parserResult = parser.ParseArguments<Options>(args);

        Options? config = default;

        parserResult.WithParsed(op => config = op)
            .WithNotParsed(errs => DisplayHelp(parserResult, errs));

        if (config == null) return;

        if (!IPAddress.TryParse(config.RedisServerHost, out var localRedisHost))
        {
            logger.LogError("Unable to parse local Redis host from arguments");
            return;
        }

        CommandInfoUpdater.CommandInfoUpdater.TryUpdateCommandInfo(config.OutputPath, config.RedisServerPort, localRedisHost, config.IgnoreCommands, logger);
    }

    static void DisplayHelp<T>(ParserResult<T> result, IEnumerable<Error> errs)
    {
        var helpText = HelpText.AutoBuild(result, h =>
        {
            h.Heading = "CommandInfoUpdater - A tool for updating Garnet's supported commands info JSON";
            h.Copyright = "Copyright (c) Microsoft Corporation";
            return HelpText.DefaultParsingErrorsHandler(result, h);
        }, e => e);
        Console.WriteLine(helpText);
    }
}