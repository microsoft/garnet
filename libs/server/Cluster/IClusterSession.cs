// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server.ACL;

namespace Garnet.server
{
    /// <summary>
    /// Cluster RESP session
    /// </summary>
    public interface IClusterSession
    {
        /// <summary>
        /// Type of session
        /// </summary>
        bool ReadWriteSession { get; }

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
        unsafe bool ProcessClusterCommands(RespCommand command, int count, byte* recvBufferPtr, int bytesRead, ref int readHead, ref byte* dcurr, ref byte* dend, out bool result);

        /// <summary>
        /// Single key slot verify (check only, do not write result to network)
        /// </summary>
        unsafe bool CheckSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(ReadOnlySpan<byte> key, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);

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
        unsafe bool NetworkMultiKeySlotVerify(SessionParseState parseState, ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Sets the user currently authenticated in this session (used for permission checks)
        /// </summary>
        void SetUser(User user);
    }
}