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
        /// Whether the caller can park until a key being migrated becomes accessible, or until a slot that
        /// <see cref="waitForStableSlot"/> requires to be stable stops importing or migrating, and verify again,
        /// rather than have the verification hold the calling thread for it. A caller that can park can also have a
        /// thread-pool thread verify the request again, rather than have the verification wait on the device on a
        /// calling thread that must not wait, to learn whether a key being migrated is still here.
        /// </summary>
        public bool canWait;

        /// <summary>
        /// Set by a verification that found a key being migrated, or a slot that must be stable importing or migrating,
        /// and was allowed to wait by <see cref="canWait"/>.
        /// The request is neither served nor answered: the caller waits for the migration to progress, then
        /// verifies it again. Cleared by the caller that acts on it.
        /// </summary>
        public bool mustWait;

        /// <summary>
        /// Set by a verification that found a key being migrated, which it could only look up on the device, on a calling
        /// thread that must not wait, and was allowed by <see cref="canWait"/> to leave the lookup to the thread pool. The
        /// request is neither served nor answered: the caller moves it to the thread pool and verifies it again there.
        /// Cleared by the caller that acts on it.
        /// </summary>
        public bool mustOffload;
    }
}