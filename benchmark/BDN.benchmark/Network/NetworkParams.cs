// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Network parameters
    /// </summary>
    public struct NetworkParams
    {
        /// <summary>
        /// Whether to use TLS
        /// </summary>
        public bool useTLS;

        /// <summary>
        /// Constructor
        /// </summary>
        public NetworkParams(bool useTLS)
        {
            this.useTLS = useTLS;
        }

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (!useTLS)
                return "None";

            var ret = "";
            if (useTLS)
                ret += "TLS";
            return ret;
        }
    }
}