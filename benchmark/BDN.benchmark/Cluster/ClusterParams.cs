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
        /// Constructor
        /// </summary>
        public ClusterParams(bool disableSlotVerification)
        {
            this.disableSlotVerification = disableSlotVerification;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (!disableSlotVerification)
                return "None";

            var ret = "";
            if (disableSlotVerification)
                ret += "DSV";
            return ret;
        }
    }
}