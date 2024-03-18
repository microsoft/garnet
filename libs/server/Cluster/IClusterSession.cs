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
        unsafe bool ProcessClusterCommands(ReadOnlySpan<byte> command, ReadOnlySpan<byte> bufSpan, int count, byte* recvBufferPtr, int bytesRead, ref int readHead, ref byte* dcurr, ref byte* dend, out bool result);

        /// <summary>
        /// Single key slot verify (check only, do not write result to network)
        /// </summary>
        unsafe bool CheckSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking);

        /// <summary>
        /// Array slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkArraySlotVerify(int keyCount, ref byte* ptr, byte* endPtr, bool interleavedKeys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, out bool retVal);

        /// <summary>
        /// Key array slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, int count = -1);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(byte[] key, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Sets the user currently authenticated in this session (used for permission checks)
        /// </summary>
        void SetUser(User user);
    }
}