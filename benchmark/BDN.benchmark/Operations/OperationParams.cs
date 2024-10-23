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
        /// Constructor
        /// </summary>
        public OperationParams(bool useACLs, bool useAof)
        {
            this.useACLs = useACLs;
            this.useAof = useAof;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (!useACLs && !useAof)
                return "None";

            var ret = "";
            if (useACLs)
                ret += "ACL";
            if (useAof)
                ret += (ret.Length > 0 ? "+" : "") + "AOF";
            return ret;
        }
    }
}