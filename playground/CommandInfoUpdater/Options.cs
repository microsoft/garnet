// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace CommandInfoUpdater
{
    public class Options
    {
        [Option('p', "port", Default = 6379, HelpText = "Redis server port to query")]
        public ushort RedisServerPort { get; set; }

        [Option('h', "host", Default = "127.0.0.1", HelpText = "Redis server host to query")]
        public string RedisServerHost { get; set; }

        [Option('o', "output", Required = true, HelpText = "Output path for updated JSON file")]
        public string OutputPath { get; set; }

        [Option('i', "ignore", Separator = ',', HelpText = "Command names to ignore (comma separated)")]
        public IEnumerable<string> IgnoreCommands { get; set; }
    }
}