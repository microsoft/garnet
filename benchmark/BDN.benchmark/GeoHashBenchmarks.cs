// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;

using Garnet.server;

namespace BDN.benchmark
{
    [Config(typeof(Config))]
    public class GeoHashBenchmarks
    {
        private class Config : BaseConfig
        {
            public Config()
            {
                AddDiagnoser(MemoryDiagnoser.Default);

                var usePdep = new MsBuildArgument("/p:UsePdepPext=true");

                AddJob(Net6BaseJob
                    .WithArguments([usePdep])
                    .WithId(".NET 6 w/ UsePdepPext"));

                AddJob(Net8BaseJob
                    .WithArguments([usePdep])
                    .WithId(".NET 8 w/ UsePdepPext"));
            }
        }

        private const double Latitude = 47.642219912251285;
        private const double Longitude = -122.14205560231471;

        private const long GeoHashInteger = 1557413161902764;

        [Benchmark]
        public long GeoToLongValue() => GeoHash.GeoToLongValue(Latitude, Longitude);

        [Benchmark]
        public (double, double) GetCoordinatesFromLong() => GeoHash.GetCoordinatesFromLong(GeoHashInteger);
    }
}