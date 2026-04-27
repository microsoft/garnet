// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Cluster parameters
    /// </summary>
    public struct ClusterParams
    {
        /// <summary>
        /// Whether to disable slot verification
        /// </summary>
        public bool disableSlotVerification;

        /// <summary>
        /// Whether to enable AOF
        /// </summary>
        public bool enableAof;

        /// <summary>
        /// Constructor
        /// </summary>
        public ClusterParams(bool disableSlotVerification, bool enableAof)
        {
            this.disableSlotVerification = disableSlotVerification;
            this.enableAof = enableAof;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (!disableSlotVerification && !enableAof)
                return "None";

            var ret = "";
            if (disableSlotVerification)
                ret += "DSV";

            if (enableAof)
                ret += ret.Length == 0 ? "AOF" : "+AOF";

            return ret;
        }
    }
}