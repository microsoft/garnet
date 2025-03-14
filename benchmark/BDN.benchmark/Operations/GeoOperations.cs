// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for GeoOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class GeoOperations : OperationsBase
    {
        static ReadOnlySpan<byte> GEOADDREM => "*5\r\n$6\r\nGEOADD\r\n$5\r\nmykey\r\n$11\r\n-122.142056\r\n$9\r\n47.642220\r\n$3\r\nMSR\r\n*3\r\n$4\r\nZREM\r\n$5\r\nmykey\r\n$3\r\nMSR\r\n"u8;
        static ReadOnlySpan<byte> GEOHASH => "*3\r\n$7\r\nGEOHASH\r\n$5\r\nmykey\r\n$7\r\nPalermo\r\n"u8;
        static ReadOnlySpan<byte> GEOPOS => "*3\r\n$6\r\nGEOPOS\r\n$5\r\nmykey\r\n$7\r\nPalermo\r\n"u8;
        static ReadOnlySpan<byte> GEODIST => "*4\r\n$7\r\nGEODIST\r\n$5\r\nmykey\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n"u8;
        static ReadOnlySpan<byte> GEOSEARCH => "*9\r\n$9\r\nGEOSEARCH\r\n$5\r\nmykey\r\n$10\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$5\r\nBYBOX\r\n$3\r\n400\r\n$3\r\n400\r\n$2\r\nkm\r\n"u8;
        static ReadOnlySpan<byte> GEOSEARCHSTORE => "*10\r\n$14\r\nGEOSEARCHSTORE\r\n$7\r\ndestkey\r\n$5\r\nmykey\r\n$10\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$5\r\nBYBOX\r\n$3\r\n400\r\n$3\r\n400\r\n$2\r\nkm\r\n"u8;

        Request geoAddRem, geoHash, geoPos, geoDist, geoSearch, geoSearchStore;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref geoAddRem, GEOADDREM);
            SetupOperation(ref geoHash, GEOHASH);
            SetupOperation(ref geoPos, GEOPOS);
            SetupOperation(ref geoDist, GEODIST);
            SetupOperation(ref geoSearch, GEOSEARCH);
            SetupOperation(ref geoSearchStore, GEOSEARCHSTORE);

            // Pre-populate data
            SlowConsumeMessage("*8\r\n$6\r\nGEOADD\r\n$5\r\nmykey\r\n$8\r\n13.361389\r\n$8\r\n38.115556\r\n$7\r\nPalermo\r\n"u8);
            SlowConsumeMessage("*8\r\n$6\r\nGEOADD\r\n$5\r\nmykey\r\n$8\r\n15.087269\r\n$8\r\n37.502669\r\n$7\r\nCatania\r\n"u8);
        }

        [Benchmark]
        public void GeoAddRem() => Send(geoAddRem);

        [Benchmark]
        public void GeoHash() => Send(geoHash);

        [Benchmark]
        public void GeoPos() => Send(geoPos);

        [Benchmark]
        public void GeoDist() => Send(geoDist);

        [Benchmark]
        public void GeoSearch() => Send(geoSearch);

        [Benchmark]
        public void GeoSearchStore() => Send(geoSearchStore);
    }
}