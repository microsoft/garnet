// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using static Tsavorite.benchmark.YcsbConstants;

namespace Tsavorite.benchmark
{
    class TestStats
    {
        private readonly List<double> initsPerRun = new();
        private readonly List<double> opsPerRun = new();
        private readonly List<long> tailAddresses = new();

        private Options options;
        internal static string OptionsString;

        internal TestStats(Options options)
        {
            this.options = options;
            OptionsString = options.GetOptionsString();
        }

        internal void AddResult((double ips, double ops, long tailAddress) result)
        {
            initsPerRun.Add(result.ips);
            opsPerRun.Add(result.ops);
            tailAddresses.Add(result.tailAddress);
        }

        internal void ShowAllStats(AggregateType aggregateType, string discardMessage = "")
        {
            var aggTypeString = aggregateType == AggregateType.Running ? "Running" : "Final";
            Console.WriteLine($"{aggTypeString} averages per second over {initsPerRun.Count} iteration(s){discardMessage}:");
            var statsLineNum = aggregateType switch
            {
                AggregateType.Running => StatsLineNum.RunningIns,
                AggregateType.FinalFull => StatsLineNum.FinalFullIns,
                AggregateType.FinalTrimmed => StatsLineNum.FinalTrimmedIns,
                _ => throw new InvalidOperationException("Unknown AggregateType")
            };
            ShowStats(statsLineNum, "ins/sec", initsPerRun);
            statsLineNum = aggregateType switch
            {
                AggregateType.Running => StatsLineNum.RunningOps,
                AggregateType.FinalFull => StatsLineNum.FinalFullOps,
                AggregateType.FinalTrimmed => StatsLineNum.FinalTrimmedOps,
                _ => throw new InvalidOperationException("Unknown AggregateType")
            };
            ShowStats(statsLineNum, "ops/sec", opsPerRun);
            statsLineNum = aggregateType switch
            {
                AggregateType.Running => StatsLineNum.RunningTail,
                AggregateType.FinalFull => StatsLineNum.FinalFullTail,
                AggregateType.FinalTrimmed => StatsLineNum.FinalTrimmedTail,
                _ => throw new InvalidOperationException("Unknown AggregateType")
            };
            ShowStats(statsLineNum, "TailAddress", tailAddresses);
        }

        private void ShowStats(StatsLineNum lineNum, string tag, List<double> vec)
        {
            var mean = vec.Sum() / vec.Count;
            var stddev = Math.Sqrt(vec.Sum(n => Math.Pow(n - mean, 2)) / vec.Count);
            var stddevpct = mean == 0 ? 0 : (stddev / mean) * 100;
            Console.WriteLine(GetStatsLine(lineNum, tag, mean, stddev, stddevpct, includeOptions: true));
        }

        private void ShowStats(StatsLineNum lineNum, string tag, List<long> vec)
        {
            var mean = vec.Sum() / vec.Count;
            var stddev = Math.Sqrt(vec.Sum(n => Math.Pow(n - mean, 2)) / vec.Count);
            var stddevpct = mean == 0 ? 0 : (stddev / mean) * 100;
            Console.WriteLine(GetStatsLine(lineNum, tag, mean, stddev, stddevpct, includeOptions: false));
        }

        internal void ShowTrimmedStats()
        {
            static void discardHiLo(List<double> vec)
            {
                vec.Sort();
#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)
                vec[0] = vec[vec.Count - 2];        // overwrite lowest with second-highest
#pragma warning restore IDE0056 // Use index operator
                vec.RemoveRange(vec.Count - 2, 2);  // remove highest and (now-duplicated) second-highest
            }
            discardHiLo(initsPerRun);
            discardHiLo(opsPerRun);

            tailAddresses.Sort();
#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)
            tailAddresses[0] = tailAddresses[tailAddresses.Count - 2];        // overwrite lowest with second-highest
#pragma warning restore IDE0056 // Use index operator
            tailAddresses.RemoveRange(tailAddresses.Count - 2, 2);  // remove highest and (now-duplicated) second-highest

            Console.WriteLine();
            ShowAllStats(AggregateType.FinalTrimmed, $" ({options.IterationCount} iterations specified, with high and low discarded)");
        }

        internal static string GetTotalOpsString(long totalOps, double seconds) => $"Total {totalOps:N0} ops done in {seconds:N3} seconds";

        internal static string GetLoadingTimeLine(double insertsPerSec, long elapsedMs)
            => $"##00; {InsPerSec}: {insertsPerSec:N2}; sec: {(double)elapsedMs / 1000:N3}";

        internal static string GetAddressesLine(AddressLineNum lineNum, long begin, long head, long rdonly, long tail)
            => $"##{(int)lineNum:00}; begin: {begin}; head: {head}; readonly: {rdonly}; tail: {tail}";

        internal static string GetStatsLine(StatsLineNum lineNum, string opsPerSecTag, double opsPerSec)
            => $"##{(int)lineNum:00}; {opsPerSecTag}: {opsPerSec:N2}; {OptionsString}";

        internal static string GetStatsLine(StatsLineNum lineNum, string meanTag, double mean, double stdev, double stdevpct, bool includeOptions)
            => $"##{(int)lineNum:00}; {meanTag}: {mean:N2}; stdev: {stdev:N1}; stdev%: {stdevpct:N1}{(includeOptions ? $"; {OptionsString}" : "")}";
    }
}