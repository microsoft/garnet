// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#if DEBUG
using BenchmarkDotNet.Configs;
#endif
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

            BenchmarkSwitcher.FromAssembly(typeof(BenchmarkDotNetTestsApp).Assembly)
#if DEBUG
                .Run(args, new DebugInProcessConfig());
#else
                .Run(args);
#endif
        }
    }
}