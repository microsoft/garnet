// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

using Garnet.server.Vector.Filter;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Represents a query execution plan produced by the <see cref="VectorQueryPlanner"/>.
    /// Contains all information needed to execute a vector search with filtering.
    /// </summary>
    internal sealed class QueryPlan
    {
        /// <summary>
        /// The execution strategy chosen for this query.
        /// </summary>
        public ExecutionStrategy Strategy { get; init; }

        /// <summary>
        /// The parsed filter expression AST (null if no filter).
        /// </summary>
        public Expr Filter { get; init; }

        /// <summary>
        /// The raw filter string from the VSIM command.
        /// </summary>
        public string FilterString { get; init; }

        /// <summary>
        /// Names of attribute indexes to be used during execution.
        /// Empty for PostFilter strategy.
        /// </summary>
        public IReadOnlyList<string> IndexNames { get; init; }

        /// <summary>
        /// Estimated selectivity of the filter (0.0 = matches nothing, 1.0 = matches everything).
        /// Used by the planner to choose strategy.
        /// </summary>
        public double EstimatedSelectivity { get; init; }

        /// <summary>
        /// Estimated number of candidate vectors matching the filter.
        /// </summary>
        public long EstimatedCandidates { get; init; }

        /// <summary>
        /// The estimated cost of executing this plan.
        /// </summary>
        public ExecutionCost EstimatedCost { get; init; }

        public override string ToString()
            => $"QueryPlan[{Strategy}, selectivity={EstimatedSelectivity:P2}, candidates={EstimatedCandidates}, cost={EstimatedCost}]";
    }
}
