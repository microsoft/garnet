// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.test;
using Garnet.test.cluster;

namespace TstRunner
{
    public class Program
    {
        static async Task Main()
        {
            //Trace.Listeners.Add(new ConsoleTraceListener());

            var test = new RespTlsTests();

            int i = 0;
            Stopwatch swatch = new();
            while (true)
            {
                var clusterMigrateTests = new ClusterMigrateTests(false);
                var clusterReplicationTests = new ClusterReplicationTests(false);
                Console.WriteLine($">>>>>>>>>> run: {i} StartedOn: {DateTime.Now}");
                swatch.Start();

                //RunTest(test, test.TlsSingleSetGet);
                //RunTest(test, test.TlsCanSelectCommand);
                //RunTest(test, test.TlsLargeSetGet);
                //RunTest(test, test.TlsLockTakeRelease);
                //RunTest(test, test.TlsMultiSetGet);
                //RunTest(test, test.TlsSetExpiry);
                //RunTest(test, test.TlsSetExpiryIncr);
                //RunTest(test, test.TlsSetExpiryNx);

                //clusterMigrateTests.logTextWriter = Console.Out;
                //var clusterMigrateTestItems = clusterMigrateTests.GetUnitTests();
                //foreach (var item in clusterMigrateTestItems)
                //    RunTest(clusterMigrateTests, item.Item1, item.Item2);

                clusterReplicationTests.SetLogTextWriter(Console.Out);
                var clusterReplicationTestItems = clusterReplicationTests.GetUnitTests();
                foreach (var item in clusterReplicationTestItems)
                    await RunTest(clusterReplicationTests, item.Item1, item.Item2);

                swatch.Stop();
                Console.WriteLine($">>>>>>>>>> run: {i++} duration (sec):{swatch.ElapsedMilliseconds / 1000}");
                swatch.Restart();
            }
        }

        private static async Task RunTest<T>(T test, Task testCase, string name = "")
        {
            dynamic ctest = test;
            try
            {
                Console.WriteLine($"\tStarted {name} on {DateTime.Now}");
                ctest.Setup();
                await testCase;
            }
            finally
            {
                ctest.TearDown();
            }
        }
    }
}