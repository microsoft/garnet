// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    enum BenchmarkType : byte
    {
        FixedLen = 0,
        SpanByte,
        Object,
        ConcurrentDictionary
    };

    enum AddressLineNum : int
    {
        Before = 1,
        After = 2
    }

    enum AggregateType
    {
        Running = 0,
        FinalFull = 1,
        FinalTrimmed = 2
    }

    enum RevivificationLevel
    {
        None = 0,
        Chain = 1,
        Full = 2
    }

    enum StatsLineNum : int
    {
        Iteration = 3,
        RunningIns = 4,
        RunningOps = 5,
        RunningTail = 6,
        FinalFullIns = 10,
        FinalFullOps = 11,
        FinalFullTail = 12,
        FinalTrimmedIns = 20,
        FinalTrimmedOps = 21,
        FinalTrimmedTail = 22
    }

    public static class YcsbConstants
    {
        internal const string UniformDist = "uniform";    // Uniformly random distribution of keys
        internal const string ZipfDist = "zipf";          // Smooth zipf curve (most localized keys)

        internal const string SyntheticData = "synthetic";
        internal const string YcsbData = "ycsb";

        internal const string InsPerSec = "ins/sec";
        internal const string OpsPerSec = "ops/sec";

        internal const CheckpointType kPeriodicCheckpointType = CheckpointType.FoldOver;
        internal const bool kPeriodicCheckpointTryIncremental = false;

        internal const double SyntheticZipfTheta = 0.99;

        internal const int kFileChunkSize = 4096;
        internal const long kChunkSize = 640;
    }
}