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
        public OperationParams(bool useACLs, bool useAof, bool useAad = false)
        {
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
