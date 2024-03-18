// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace MetricsMonitor
{
    public class Program
    {
        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            ClientMonitor cm = new(opts);
            cm.StartMonitor();
        }
    }
}