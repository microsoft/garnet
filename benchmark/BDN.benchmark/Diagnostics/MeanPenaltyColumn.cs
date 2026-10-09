// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace BDN.benchmark.Diagnostics
{
    /// <summary>
    /// A statistics column that reports the same value as BenchmarkDotNet's built-in <c>Mean</c>
    /// (the wall-clock arithmetic mean of the measured operation) under a domain-specific header.
    /// In the epoch-barrier benchmarks the measured operation <em>is</em> the penalty under study — a
    /// bump-and-wait for the waiter, or an acquire/release cycle for the waker — so relabeling Mean to
    /// "Waiter penalty" / "Waker penalty" states directly what the number means. Formatting (unit
    /// scaling, precision) is delegated to the built-in <see cref="StatisticColumn.Mean"/> so the value
    /// renders identically to a standard Mean column.
    /// </summary>
    public sealed class MeanPenaltyColumn : IColumn
    {
        readonly IColumn inner = StatisticColumn.Mean;
        readonly string label;

        /// <summary>
        /// Creates the column with the header to display in place of "Mean".
        /// </summary>
        public MeanPenaltyColumn(string label) => this.label = label;

        /// <inheritdoc/>
        public string Id => nameof(MeanPenaltyColumn) + "." + label;

        /// <inheritdoc/>
        public string ColumnName => label;

        /// <inheritdoc/>
        public bool AlwaysShow => true;

        /// <inheritdoc/>
        public ColumnCategory Category => ColumnCategory.Statistics;

        /// <inheritdoc/>
        public int PriorityInCategory => 0;

        /// <inheritdoc/>
        public bool IsNumeric => true;

        /// <inheritdoc/>
        public UnitType UnitType => UnitType.Time;

        /// <inheritdoc/>
        public string Legend => $"{label}: wall-clock mean of the measured operation (same value as Mean)";

        /// <inheritdoc/>
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase) => inner.GetValue(summary, benchmarkCase);

        /// <inheritdoc/>
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) => inner.GetValue(summary, benchmarkCase, style);

        /// <inheritdoc/>
        public bool IsAvailable(Summary summary) => inner.IsAvailable(summary);

        /// <inheritdoc/>
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
    }

    /// <summary>
    /// Config that renders the Mean column as "Waiter penalty" — the per-bump wall-clock latency the
    /// epoch-bumping (waiter) thread pays while it waits for the barrier to converge.
    /// </summary>
    public sealed class WaiterPenaltyConfig : ManualConfig
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public WaiterPenaltyConfig()
        {
            AddColumn(new MeanPenaltyColumn("Waiter penalty"));
            HideColumns("Mean");
        }
    }
}