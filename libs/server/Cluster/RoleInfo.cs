// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public struct RoleInfo
    {
        /// <summary>
        /// Replication offset using string store.
        /// </summary>
        public AofAddress replication_offset;

        /// <summary>
        /// Max send timemstamp
        /// </summary>
        public AofAddress sequenceNumber;

        /// <summary>
        /// Replication offset lag. 
        /// </summary>
        public AofAddress replication_lag;

        /// <summary>
        /// Replication state.
        /// ROLE command uses "connect", "connecting", "sync" and "connected".
        /// Metrics use "online" and "offline". 
        /// </summary>
        public string replication_state;

        /// <summary>
        /// Address of instance. 
        /// </summary>
        public string address;

        /// <summary>
        /// Port of instance.
        /// </summary>
        public int port;

        /// <summary>
        /// Printout for Metrics.
        /// </summary>
        /// <returns>string</returns>
        public override readonly string ToString()
        {
            return $"ip={address},port={port},state={replication_state},offset={replication_offset},lag={replication_lag},sequenceNumber={sequenceNumber}";
        }
    }
}