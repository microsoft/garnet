// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server.ACL;

namespace Garnet.server
{
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
        /// Type of session
        /// </summary>
        bool ReadWriteSession { get; }

        /// <summary>
        /// If the current session has seen an APPENDLOG command.
        /// </summary>
        bool IsReplicating { get; }

        /// <summary>
        /// If set, commands can use this to enumerate details about the server or other sessions.
        ///
        /// It is not guaranteed to be set.
        /// </summary>
        IGarnetServer Server { get; set; }

        /// <summary>
        /// Make this cluster session a read-only session
        /// </summary>
        void SetReadOnlySession();

        /// <summary>
        /// Make this cluster session a read-write session
        /// </summary>
        void SetReadWriteSession();

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
        /// Process cluster commands
        /// </summary>
        unsafe void ProcessClusterCommands(RespCommand command, ref SessionParseState parseState, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Reset cached slot verification result
        /// </summary>
        void ResetCachedSlotVerificationResult();

        /// <summary>
        /// Verification method that works iteratively by caching the verification result between calls.
        /// NOTE: Caller must call ResetCachedSlotVerificationResult appropriately
        /// </summary>
        /// <param name="keySlice"></param>
        /// <param name="readOnly"></param>
        /// <param name="SessionAsking"></param>
        /// <returns></returns>
        bool NetworkIterativeSlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking);

        /// <summary>
        /// Write cached slot verification message to output
        /// </summary>
        /// <param name="output"></param>
        public void WriteCachedSlotVerificationMessage(ref MemoryResult<byte> output);

        /// <summary>
        /// Key array slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkKeyArraySlotVerify(Span<ArgSlice> keys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, int count = -1);

        /// <summary>
        /// Array slot verify (write result to network)
        /// </summary>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <returns></returns>
        unsafe bool NetworkMultiKeySlotVerify(ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Array slot verify with no response
        /// </summary>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <returns></returns>
        unsafe bool NetworkMultiKeySlotVerifyNoResponse(ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Sets the <see cref="UserHandle"/> currently authenticated in this session (used for permission checks)
        /// </summary>
        void SetUserHandle(UserHandle userHandle);
    }
}