// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Threading;
using CommandLine;

namespace GarnetClientStress
{
    public class Program
    {
        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            ConfigureGlobalRuntimeSettings();

            StressTestUtils stressTestUtils = new(opts);
            stressTestUtils.RunTest();
        }

        private static void ConfigureGlobalRuntimeSettings()
        {
            ThreadPool.SetMinThreads(workerThreads: 1000, completionPortThreads: 1000);
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.CheckCertificateRevocationList = false;
            ServicePointManager.DefaultConnectionLimit = 1024;
            ServicePointManager.ReusePort = true;
        }
    }
}