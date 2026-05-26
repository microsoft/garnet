// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public struct ClusterSlotVerificationInput
    {
        /// <summary>
        /// Whether this is a read only command
        /// </summary>
        public bool readOnly;

        /// <summary>
        /// Whether ASKING is enabled for this command
        /// </summary>
        public byte sessionAsking;

        /// <summary>
        /// Simplified key specifications for extracting key positions from the command's parse state
        /// </summary>
        public SimpleRespKeySpec[] keySpecs;

        /// <summary>
        /// Whether the command is a sub-command (affects key index offset calculation)
        /// </summary>
        public bool isSubCommand;

        /// <summary>
        /// If the command being executed requires a slot be STABLE for executing.
        /// 
        /// This requires special handling during migrations.
        /// 
        /// Currently only true for Vector Set commands that are writes.
        /// </summary>
        public bool waitForStableSlot;

        /// <summary>
        /// If true, return TRYAGAIN instead of spin-waiting when a key is blocked
        /// by active migration (sketch status is TRANSMITTING or DELETING).
        /// 
        /// Used for RangeIndex commands where migration can take a long time and
        /// spin-waiting would stall the entire pipelined connection.
        /// </summary>
        public bool returnTryAgainForMigratingKeys;
    }
}