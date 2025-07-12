// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.test;
using Garnet.test.cluster;

namespace TstRunner
{
    public class Program
    {
        static void Main()
        {
            //Trace.Listeners.Add(new ConsoleTraceListener());

            var test = new RespTlsTests();

            int i = 0;
            Stopwatch swatch = new();
            while (true)
            {
                var clusterMigrateTests = new ClusterMigrateTests(false);
                var clusterReplicationTests = new ClusterReplicationBaseTests();
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

                var clusterReplicationTestItems = clusterReplicationTests.GetUnitTests().AsSpan().Slice(39);
                foreach (var item in clusterReplicationTestItems)
                    RunTest(clusterReplicationTests, item.Item1, item.Item2);

                swatch.Stop();
                Console.WriteLine($">>>>>>>>>> run: {i++} duration (sec):{swatch.ElapsedMilliseconds / 1000}");
                swatch.Restart();
            }
        }

        private static void RunTest<T>(T testGroup, Action testCase, string name = "")
        {
            dynamic cTestGroup = testGroup;
            try
            {
                Console.WriteLine($"\tStarted {name} on {DateTime.Now}");
                cTestGroup.LogTextWriter = Console.Out;
                cTestGroup.Setup();
                testCase();
            }
            finally
            {
                cTestGroup.TearDown();
            }
        }
    }
}