// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace CommandInfoUpdater
{
    public class Options
    {
        [Option('p', "port", Default = 6379)]
        public ushort LocalRedisPort { get; set; }

        [Option('h', "host", Default = "127.0.0.1")]
        public string LocalRedisHost { get; set; }

        [Option('o', "output", Required = true)]
        public string OutputPath { get; set; }

        [Option('i', "ignore", Default = new[]{ "SETEXXX", "SETEXNX" })]
        public IEnumerable<string> IgnoreCommands { get; set; }
    }
}
