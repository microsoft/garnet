// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Immutable;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace BDN.benchmark
{
    /// <summary>
    /// Summary orderer that lays out the results table primarily by namespace, then by type, while preserving
    /// BenchmarkDotNet's normal within-type ordering (category, then parameters, job, and declared method order).
    /// <para>
    /// This matters most for joined summaries (the <c>--join</c> switch, i.e.
    /// <see cref="BenchmarkDotNet.Configs.ConfigOptions.JoinSummary"/>), where the default orderer interleaves
    /// benchmarks from different types: it compares only the declared method index and ignores the declaring type
    /// and namespace, so the n-th method of every type is grouped together. Ordering by namespace first keeps every
    /// benchmark from a given namespace in one unbroken series, with its types — and the categories within each type —
    /// laid out in a stable, predictable order.
    /// </para>
    /// </summary>
    public sealed class NamespaceTypeOrderer : DefaultOrderer
    {
        public override IEnumerable<BenchmarkCase> GetSummaryOrder(ImmutableArray<BenchmarkCase> benchmarksCases, Summary summary)
        {
            // Order the types by namespace, then by type name, emitting each type's cases as a contiguous block.
            var orderedTypeGroups = benchmarksCases
                .GroupBy(benchmarkCase => benchmarkCase.Descriptor.Type)
                .OrderBy(group => group.Key.Namespace ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(group => group.Key.Name, StringComparer.Ordinal);

            // Within each type defer to the base orderer so categories (and parameters/jobs/methods) are handled normally.
            foreach (var typeGroup in orderedTypeGroups)
                foreach (var benchmarkCase in base.GetSummaryOrder(typeGroup.ToImmutableArray(), summary))
                    yield return benchmarkCase;
        }
    }
}
