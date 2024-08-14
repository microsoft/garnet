// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.benchmark
{
    public class Program
    {
        const int kTrimResultCount = 3; // Use some high value like int.MaxValue to disable

        public static void Main(string[] args)
        {
            TestLoader testLoader = new(args);
            if (testLoader.error)
                return;

            TestStats testStats = new(testLoader.Options);
            testLoader.LoadData();
            var options = testLoader.Options;   // shortcut

            for (var iter = 0; iter < options.IterationCount; ++iter)
            {
                Console.WriteLine();
                if (options.IterationCount > 1)
                    Console.WriteLine($"Iteration {iter + 1} of {options.IterationCount}");

                switch (testLoader.BenchmarkType)
                {
                    case BenchmarkType.Ycsb:
                        {
                            var tester = new Tsavorite_YcsbBenchmark(testLoader.init_keys, testLoader.txn_keys, testLoader);
                            testStats.AddResult(tester.Run(testLoader));
                            tester.Dispose();
                        }
                        break;
                    case BenchmarkType.SpanByte:
                        {
                            var tester = new SpanByteYcsbBenchmark(testLoader.init_span_keys, testLoader.txn_span_keys, testLoader);
                            testStats.AddResult(tester.Run(testLoader));
                            tester.Dispose();
                        }
                        break;
                    case BenchmarkType.ConcurrentDictionaryYcsb:
                        {
                            var tester = new ConcurrentDictionary_YcsbBenchmark(testLoader.init_keys, testLoader.txn_keys, testLoader);
                            testStats.AddResult(tester.Run(testLoader));
                            tester.Dispose();
                        }
                        break;
                    default:
                        throw new ApplicationException("Unknown benchmark type");
                }

                if (options.IterationCount > 1)
                {
                    testStats.ShowAllStats(AggregateType.Running);
                    if (iter < options.IterationCount - 1)
                    {
                        GC.Collect();
                        GC.WaitForFullGCComplete();
                        Thread.Sleep(1000);
                    }
                }
            }

            Console.WriteLine();
            testStats.ShowAllStats(AggregateType.FinalFull);
            if (options.IterationCount >= kTrimResultCount)
                testStats.ShowTrimmedStats();
        }
    }
}