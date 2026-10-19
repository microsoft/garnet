// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flushes the response buffer the cluster session shares with the RESP session that owns it, and leaves
    /// the cursors on a buffer with room to carry on writing.
    /// </summary>
    /// <param name="dcurr">Write cursor, reset to the head of a usable buffer on return.</param>
    /// <param name="dend">End of the usable buffer.</param>
    public unsafe delegate void ResponseFlusher(ref byte* dcurr, ref byte* dend);

    /// <summary>
    /// Cluster RESP session
    /// </summary>
    public interface IClusterSession
    {
        /// <summary>
        /// If the current session is being used by a remote cluster node, the id that was last presented during a GOSSIP message.
        /// </summary>
        string RemoteNodeId { get; }

        /// <summary>
        /// Whether this connection may serve read-only commands from a replica
        /// </summary>
        bool ReadOnlySession { get; }

        /// <summary>
        /// Whether this internal session may apply writes while replaying the AOF
        /// </summary>
        bool IsInternalWriteSession { get; }

        /// <summary>
        /// If the current session is part of an active replication stream (set on first APPENDLOG, including the init handshake).
        /// </summary>
        bool IsReplicating { get; }

        /// <summary>
        /// If set, commands can use this to enumerate details about the server or other sessions.
        ///
        /// It is not guaranteed to be set.
        /// </summary>
        IGarnetServer Server { get; set; }

        /// <summary>
        /// Allow this connection to serve read-only commands from a replica
        /// </summary>
        void SetReadOnlySession();

        /// <summary>
        /// Restore the default behavior of redirecting commands from a replica
        /// </summary>
        void SetReadWriteSession();

        /// <summary>
        /// Allow the internal AOF replay session to apply writes on a replica
        /// </summary>
        void SetInternalWriteSession();

        /// <summary>
        /// Local current epoch
        /// </summary>
        long LocalCurrentEpoch { get; }

        /// <summary>
        /// Acquire epoch
        /// </summary>
        void AcquireCurrentEpoch();

        /// <summary>
        /// Release epoch
        /// </summary>
        void ReleaseCurrentEpoch();

        /// <summary>
        /// Process cluster commands.
        /// </summary>
        /// <remarks>
        /// A command that must wait does not block the network thread. It returns false with the wait in
        /// <paramref name="pending"/> and writes no reply; the caller suspends the session on that wait and
        /// then calls <see cref="CompletePendingCommand"/> to write the reply.
        /// </remarks>
        /// <returns>True if the command is complete; false if <paramref name="pending"/> must be awaited.</returns>
        unsafe bool ProcessClusterCommands(RespCommand command, VectorManager vectorManager, ref SessionParseState parseState, ref byte* dcurr, ref byte* dend, out ValueTask pending);

        /// <summary>
        /// Writes the reply of the command that returned a pending wait, now that the wait has completed.
        /// </summary>
        /// <param name="dcurr">Write cursor into the response buffer.</param>
        /// <param name="dend">End of the response buffer.</param>
        unsafe void CompletePendingCommand(ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Reset cached slot verification result
        /// </summary>
        void ResetCachedSlotVerificationResult();

        /// <summary>
        /// Verification method that works iteratively by caching the verification result between calls.
        /// NOTE: Caller must call ResetCachedSlotVerificationResult appropriately
        /// </summary>
        /// <remarks>
        /// Reads <see cref="ClusterSlotVerificationInput.readOnly"/>, <see cref="ClusterSlotVerificationInput.sessionAsking"/>,
        /// <see cref="ClusterSlotVerificationInput.waitForStableSlot"/>, <see cref="ClusterSlotVerificationInput.canWait"/> and
        /// <see cref="ClusterSlotVerificationInput.canOffload"/> for the key. A key being migrated that the caller may not
        /// wait for here, or look up here, sets <see cref="ClusterSlotVerificationInput.mustWait"/> or
        /// <see cref="ClusterSlotVerificationInput.mustOffload"/> and fails the verification; the caller then runs the
        /// request again from the start, after resetting the cached result.
        /// </remarks>
        /// <param name="keySlice">Key to verify.</param>
        /// <param name="csvi">How to verify the key, and whether its verification has to be run again.</param>
        /// <returns>True if the key, and every key verified since the cached result was reset, can be served here.</returns>
        bool NetworkIterativeSlotVerify(PinnedSpanByte keySlice, ref ClusterSlotVerificationInput csvi);

        /// <summary>
        /// Write cached slot verification message to output
        /// </summary>
        /// <param name="output"></param>
        public void WriteCachedSlotVerificationMessage(ref MemoryResult<byte> output);

        /// <summary>
        /// Array slot verify (write result to network)
        /// </summary>
        /// <remarks>
        /// When <see cref="ClusterSlotVerificationInput.canWait"/> is set and a key of the request is being
        /// migrated, nothing is written: the call returns true with <see cref="ClusterSlotVerificationInput.mustWait"/> set,
        /// and the caller waits with <see cref="TryRegisterMigrationWaiter"/> before verifying again. When
        /// <see cref="ClusterSlotVerificationInput.canOffload"/> is set and a key being migrated may have to be looked up on
        /// the device, nothing is written either: the call returns true with <see cref="ClusterSlotVerificationInput.mustOffload"/>
        /// set, and the caller verifies again on the thread pool.
        /// </remarks>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <param name="isTxn"></param>
        /// <returns></returns>
        unsafe bool NetworkMultiKeySlotVerify(ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend, bool isTxn = false);

        /// <summary>
        /// Array slot verify with no response
        /// </summary>
        /// <remarks>
        /// A request that would have to wait for a migration counts as not servable, so a caller that only
        /// looks ahead leaves it for its own verification to wait on.
        /// </remarks>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <param name="isTxn"></param>
        /// <returns></returns>
        unsafe bool NetworkMultiKeySlotVerifyNoResponse(ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend, bool isTxn = false);

        /// <summary>
        /// Registers a waiter to be notified when the migration a request is waiting on moves on.
        /// </summary>
        /// <remarks>
        /// Valid after a verification that left <see cref="ClusterSlotVerificationInput.mustWait"/> set, and
        /// relative to the migration state that verification observed.
        /// </remarks>
        /// <param name="waiter">Waiter to notify. Notified at most once per registration.</param>
        /// <returns>
        /// False if the migration has already moved on since the verification, in which case the waiter is not
        /// registered and will not be notified: the caller verifies again instead of waiting.
        /// </returns>
        bool TryRegisterMigrationWaiter(IMigrationWaiter waiter);

        /// <summary>
        /// Takes a waiter registered by <see cref="TryRegisterMigrationWaiter"/> off before it is notified. Does nothing
        /// if it is not registered. Safe to call from any thread, including after the session is disposed.
        /// </summary>
        /// <param name="waiter">Waiter to take off.</param>
        void RemoveMigrationWaiter(IMigrationWaiter waiter);

        /// <summary>
        /// Sets the <see cref="UserHandle"/> currently authenticated in this session (used for permission checks)
        /// </summary>
        void SetUserHandle(UserHandle userHandle);

        /// <summary>
        /// NOTE: Unsafe! DO NOT USE, other than benchmarking
        /// </summary>
        /// <param name="replicaOf"></param>
        void UnsafeSetConfig(string replicaOf);

        /// <summary>
        /// Dispose
        /// </summary>
        void Dispose();

        /// <summary>
        /// NOTE: Used for micro-benchmark
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <returns>
        /// A completed task when the records were applied without waiting. Otherwise a task that completes once they are,
        /// during which <paramref name="record"/> must stay valid.
        /// </returns>
        unsafe ValueTask ProcessPrimaryStreamAsync(int physicalSublogIdx, byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress);
    }
}