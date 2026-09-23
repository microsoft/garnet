// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Operation parameters
    /// </summary>
    public struct OperationParams
    {
        /// <summary>
        /// Whether to use ACLs
        /// </summary>
        public bool useACLs;

        /// <summary>
        /// Whether to use AOF
        /// </summary>
        public bool useAof;

        /// <summary>
        /// Whether to use AAD authentication. Mutually exclusive with <see cref="useACLs"/>.
        /// </summary>
        public bool useAad;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>
        /// At most one aspect may be set. <c>OperationParamsFilter</c> maps each value onto the single
        /// <c>--opparams</c> flag that selects it, and every provider yields either one aspect or none.
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown when more than one of <paramref name="useACLs"/>,
        /// <paramref name="useAof"/> and <paramref name="useAad"/> is true.</exception>
        public OperationParams(bool useACLs, bool useAof, bool useAad)
        {
            // Benchmarks run against a Release build, where Debug.Assert is compiled out, so this throws.
            if ((useACLs ? 1 : 0) + (useAof ? 1 : 0) + (useAad ? 1 : 0) > 1)
                throw new ArgumentException($"At most one of {nameof(useACLs)}, {nameof(useAof)} and {nameof(useAad)} may be set.");

            // Subsumed by the check above. Restore it if combinations are ever allowed, because the two auth
            // modes remain mutually exclusive regardless.
            // if (useACLs && useAad)
            //     throw new ArgumentException("useACLs and useAad are mutually exclusive; pick one auth mode.");

            this.useACLs = useACLs;
            this.useAof = useAof;
            this.useAad = useAad;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (!useACLs && !useAof && !useAad)
                return "None";

            var ret = "";
            if (useACLs)
                ret += "ACL";
            if (useAad)
                ret += (ret.Length > 0 ? "+" : "") + "AAD";
            if (useAof)
                ret += (ret.Length > 0 ? "+" : "") + "AOF";
            return ret;
        }
    }
}