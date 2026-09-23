// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Applies the <c>--opparams</c> (<c>--op</c>) command line option by excluding Operations benchmark cases
    /// whose <see cref="OperationParams"/> were not selected.
    /// </summary>
    /// <remarks>
    /// This is a case filter rather than a conditional parameter source because of the order BenchmarkDotNet
    /// works in. It enumerates <see cref="OperationsBase.OperationParamsProvider"/> once in the host and
    /// identifies each value by its position in that sequence, builds a <see cref="BenchmarkCase"/> for every
    /// combination of benchmark method, job and parameter value, and only then applies the configured filters.
    /// Excluding a case leaves those positions untouched, whereas yielding a different sequence changes what
    /// every position means. That matters because the generated per-job process re-invokes the provider and
    /// selects by position, and never runs <c>Program.Main</c>, so it always sees the <c>Params*</c> defaults.
    /// Every configured filter must admit a case, so this composes with the <c>--filter</c> globs.
    /// </remarks>
    internal sealed class OperationParamsFilter : IFilter
    {
        /// <inheritdoc/>
        /// <remarks>
        /// BenchmarkDotNet contributes one item per parameterized member, and <see cref="OperationsBase"/>
        /// declares the only <see cref="OperationParams"/> one, so at most one item matches. Selecting by type
        /// rather than by member name keeps that true if a benchmark adds further parameters or arguments.
        /// </remarks>
        public bool Predicate(BenchmarkCase benchmarkCase)
        {
            foreach (var parameter in benchmarkCase.Parameters.Items)
            {
                if (parameter.Value is OperationParams operationParams)
                    return IsEnabled(operationParams);
            }

            // Not an Operations benchmark (Lua, Network, Cluster, ...), so --opparams does not apply.
            return true;
        }

        /// <summary>
        /// Maps a parameter value onto the <c>Params*</c> flag that selects it.
        /// </summary>
        /// <remarks>
        /// Each flag names a single aspect, and <see cref="OperationParams"/> permits at most one per value, so
        /// no combination reaches the switch. The default arm admits anything else rather than dropping it, so
        /// relaxing that constructor rule leaves new combinations visible in a filtered run instead of silently
        /// absent.
        /// </remarks>
        static bool IsEnabled(OperationParams operationParams)
            => (operationParams.useACLs, operationParams.useAof, operationParams.useAad) switch
            {
                (false, false, false) => OperationsBase.ParamsNone,
                (true, false, false) => OperationsBase.ParamsACL,
                (false, true, false) => OperationsBase.ParamsAOF,
                (false, false, true) => OperationsBase.ParamsAAD,
                _ => true,
            };
    }
}