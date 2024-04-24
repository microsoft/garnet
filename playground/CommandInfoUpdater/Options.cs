// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace CommandInfoUpdater
{
    public class Options
    {
        [Option('p', "port", Required = false, Default = 6379, HelpText = "RESP server port to query")]
        public int RespServerPort { get; set; }

        [Option('h', "host", Required = false, Default = "127.0.0.1", HelpText = "RESP server host to query")]
        public string RespServerHost { get; set; }

        [Option('o', "output", Required = true, HelpText = "Output path for updated JSON file")]
        public string OutputPath { get; set; }

        [Option('f', "force", Required = false, Default = false, HelpText = "Force overwrite existing commands info")]
        public bool Force { get; set; }

        [Option('i', "ignore", Required = false, Separator = ',', HelpText = "Command names to ignore (comma separated)")]
        public IEnumerable<string> IgnoreCommands { get; set; }
    }
}