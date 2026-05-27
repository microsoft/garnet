// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Cluster.ConsistentRead
{
    /// <summary>
    /// Parameters for consistent read benchmarks
    /// </summary>
    public struct ConsistentReadParams
    {
        /// <summary>
        /// Whether multi-log (single physical log + multi-replay) is enabled
        /// </summary>
        public bool multiLogEnabled;

        /// <summary>
        /// Whether the node operates in replica mode (exercises consistent read path)
        /// </summary>
        public bool replicaMode;

        /// <summary>
        /// Constructor
        /// </summary>
        public ConsistentReadParams(bool multiLogEnabled, bool replicaMode)
        {
            this.multiLogEnabled = multiLogEnabled;
            this.replicaMode = replicaMode;
        }

        /// <summary>
        /// String representation for BDN output
        /// </summary>
        public override string ToString()
        {
            if (!multiLogEnabled && !replicaMode)
                return "SingleLog";

            if (multiLogEnabled && !replicaMode)
                return "MultiLog+Primary";

            if (multiLogEnabled && replicaMode)
                return "MultiLog+Replica";

            return "Invalid";
        }
    }
}