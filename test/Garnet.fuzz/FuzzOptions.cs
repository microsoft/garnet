// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using CommandLine;

namespace Garnet.fuzz
{
    internal sealed class FuzzOptions
    {
        [Option('t', "fuzz-target",  HelpText = "Class name to run fuzz input against", Required = true)]
        public required FuzzTargets FuzzTarget { get; set; }

        [Option('f', "input-file", HelpText = "Read fuzz input from given file", Group = "input")]
        public FileInfo? InputFile { get; set; }

        [Option('i', "stdin", HelpText = "Read fuzz input from standard input", Group = "input")]
        public bool UseStandardIn { get; set; }

        [Option('q', "quiet", HelpText = "Suppress output")]
        public bool Quiet { get; set; }
    }
}
