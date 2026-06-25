// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark
{
    /// <summary>
    /// Category names applied to benchmarks via <see cref="BenchmarkDotNet.Attributes.BenchmarkCategoryAttribute"/>.
    /// Categories group operations by the storage access pattern they exercise, which the results table is ordered by.
    /// </summary>
    internal static class BenchmarkCategories
    {
        /// <summary>Blind writes that overwrite the value without reading it first (e.g. SET, SETEX).</summary>
        public const string Upsert = "Upsert";

        /// <summary>Read-modify-write operations whose result depends on the existing value (e.g. SET NX/XX, INCR/DECR).</summary>
        public const string RMW = "RMW";

        /// <summary>Pure reads that do not modify the value (e.g. GET).</summary>
        public const string Read = "Read";
    }
}
