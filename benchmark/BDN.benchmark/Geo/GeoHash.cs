// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Geo
{
    /// <summary>
    /// Benchmark for GeoHash
    /// </summary>
    [MemoryDiagnoser]
    public class GeoHash
    {
        private const double Latitude = 47.642219912251285;
        private const double Longitude = -122.14205560231471;

        private const long GeoHashInteger = 1557413161902764;

        [Benchmark]
        public long GeoToLongValue() => Garnet.server.GeoHash.GeoToLongValue(Latitude, Longitude);

        [Benchmark]
        public (double, double) GetCoordinatesFromLong() => Garnet.server.GeoHash.GetCoordinatesFromLong(GeoHashInteger);

        [Benchmark]
        public string GetGeoHashCode() => Garnet.server.GeoHash.GetGeoHashCode(GeoHashInteger);
    }
}