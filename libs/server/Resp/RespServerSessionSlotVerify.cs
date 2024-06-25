// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// This method is used to verify slot ownership for provided key.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="key">Key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation.</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(ReadOnlySpan<byte> key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(key, readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided array of key argslices.
        /// </summary>
        /// <param name="keys">Array of key ArgSlice</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="count">Key count if different than keys array length</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(Span<ArgSlice> keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(keys, readOnly, SessionAsking, ref dcurr, ref dend, count);

        /// <summary>
        /// Verify if the corresponding command can be served given the status of the slot associated with the parsed keys.
        /// </summary>
        /// <param name="readOnly"></param>
        /// <param name="firstKey"></param>
        /// <param name="lastKey"></param>
        /// <param name="step"></param>
        /// <returns></returns>
        bool NetworkMultiKeySlotVerify(bool readOnly = false, int firstKey = 0, int lastKey = -1, int step = 1)
        {
            if (clusterSession == null)
                return false;
            csvi.readOnly = readOnly;
            csvi.sessionAsking = SessionAsking;
            csvi.firstKey = firstKey;
            csvi.lastKey = lastKey < 0 ? parseState.count + 1 + lastKey : lastKey;
            csvi.step = step;
            return clusterSession.NetworkMultiKeySlotVerify(parseState, csvi, ref dcurr, ref dend);
        }
    }
}