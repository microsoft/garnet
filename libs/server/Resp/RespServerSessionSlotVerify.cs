// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
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
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}