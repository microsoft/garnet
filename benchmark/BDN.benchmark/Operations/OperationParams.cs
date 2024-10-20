// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Operation parameters
    /// </summary>
    public struct OperationParams
    {
        public bool useACLs;
        public bool useAof;

        public OperationParams(bool useACLs, bool useAof)
        {
            this.useACLs = useACLs;
            this.useAof = useAof;
        }

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