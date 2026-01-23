// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// This method is used to verify slot ownership for provided array of key argslices.
        /// </summary>
        /// <param name="keys">Array of key ArgSlice</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="count">Key count if different than keys array length</param>
        /// <param name="waitForStableSlot">Whether the executing command requires the containing slot be STABLE.</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(Span<ArgSlice> keys, bool readOnly, bool waitForStableSlot, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(keys, readOnly, SessionAsking, waitForStableSlot, ref dcurr, ref dend, count);

        /// <summary>
        /// Validate if this command can be served based on the current slot assignment
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlot(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            // Verify slot for command if it falls into data command category
            if (!cmd.IsDataCommand())
                return true;

            cmd = cmd.NormalizeForACLs();
            if (!RespCommandsInfo.TryFastGetRespCommandInfo(cmd, out var commandInfo))
                // This only happens if we failed to parse the json file
                return false;

            // The provided command is not a data command
            // so we can serve without any slot restrictions
            if (commandInfo == null)
                return true;

            csvi.keyNumOffset = -1;
            storeWrapper.clusterProvider.ExtractKeySpecs(commandInfo, cmd, ref parseState, ref csvi);
            csvi.readOnly = cmd.IsReadOnly();
            csvi.sessionAsking = SessionAsking;
            csvi.waitForStableSlot = cmd is RespCommand.VADD or RespCommand.VREM or RespCommand.VSETATTR;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}