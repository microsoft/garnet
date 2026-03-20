// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core;

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
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(Span<PinnedSpanByte> keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(keys, readOnly, SessionAsking, ref dcurr, ref dend, count);

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
            if (!RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var cmdInfo))
                // This only happens if we failed to parse the json file
                return false;

            // The provided command does not have key specs
            // so we can serve without any slot restrictions
            if (cmdInfo.KeySpecs == null || cmdInfo.KeySpecs.Length == 0)
                return true;

            csvi.keySpecs = cmdInfo.KeySpecs;
            // BITOP's operation argument (AND/OR/XOR/NOT) is consumed by the parser,
            // so key indices need a -2 offset like subcommands
            csvi.isSubCommand = cmdInfo.IsSubCommand || cmd == RespCommand.BITOP;
            csvi.readOnly = cmd.IsReadOnly();
            csvi.sessionAsking = SessionAsking;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}