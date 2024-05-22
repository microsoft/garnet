// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

using Garnet.server;

namespace BDN.benchmark
{
    [MemoryDiagnoser]
    public class GeoHashBenchmarks
    {
        private const double Latitude = 47.642219912251285;
        private const double Longitude = -122.14205560231471;

        private const long GeoHashInteger = 1557413161902764;

        [Benchmark]
        public long GeoToLongValue() => GeoHash.GeoToLongValue(Latitude, Longitude);

        [Benchmark]
        public (double, double) GetCoordinatesFromLong() => GeoHash.GetCoordinatesFromLong(GeoHashInteger);

        [Benchmark]
        public string GetGeoHashCode() => GeoHash.GetGeoHashCode(GeoHashInteger);
    }
}