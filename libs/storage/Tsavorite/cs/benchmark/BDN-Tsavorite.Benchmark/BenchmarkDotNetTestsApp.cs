// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Running;

namespace BenchmarkDotNetTests
{
    public class BenchmarkDotNetTestsApp
    {
        public static string TestDirectory => Path.Combine(Path.GetDirectoryName(typeof(BenchmarkDotNetTestsApp).Assembly.Location), "Tests");

        public static void Main(string[] args)
        {
            // Check for debugging a test
            if (args[0].ToLower() == "cursor")
            {
                var test = new IterationTests
                {
                    FlushAndEvict = true
                };
                test.SetupPopulatedStore();
                test.Cursor();
                test.TearDown();
                return;
            }
            if (args[0].ToLower() == "inlining")
            {
                var test = new InliningTests();
                test.NumRecords = 1_000_000;
                test.SetupPopulatedStore();
                if (args.Length > 1)
                {
                    var testName = args[1].ToLower();
                    if (testName == "read")
                        test.Read();
                    else if (testName == "upsert")
                        test.Upsert();
                    else if (testName == "rmw")
                        test.RMW();
                    else
                        throw new ArgumentException($"Unknown inlining test: {args[1]}");
                }
                else
                {
                    test.Read();
                    test.Upsert();
                    test.RMW();
                }
                test.TearDown();
                return;
            }

            // Do regular invocation.
            BenchmarkSwitcher.FromAssembly(typeof(BenchmarkDotNetTestsApp).Assembly).Run(args);
        }
    }
}