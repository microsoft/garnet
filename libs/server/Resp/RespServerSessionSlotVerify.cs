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
        // Key spec for CustomRawStringCmd and CustomObjCmd: a single key at arg index 1.
        private static readonly SimpleRespKeySpec[] CustomCommandSingleKeySpec =
        [
            new SimpleRespKeySpec
            {
                BeginSearch = new SimpleRespKeySpecBeginSearch(index: 1),
                FindKeys = new SimpleRespKeySpecFindKeys(keyStep: 1, lastKeyOrLimit: 0, isLimit: false),
            }
        ];

        /// <summary>
        /// Validates whether a command can be served based on the current slot assignment
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlot(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            if (!cmd.IsDataCommand())
            {
                // Custom commands sit outside IsDataCommand but still touch user keys.
                if (cmd is RespCommand.CustomRawStringCmd or RespCommand.CustomObjCmd)
                    return CanServeSlotForCustomCommand(cmd);

                return true;
            }

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
            csvi.waitForStableSlot = cmd is RespCommand.VADD or RespCommand.VREM or RespCommand.VSETATTR;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }

        /// <summary>
        /// Validates whether a custom command can be served based on the current slot assignment
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlotForCustomCommand(RespCommand cmd)
        {
            // cmd.IsReadOnly() can't be used here since both custom enum values sort past LastReadCommand.
            var isReadOnly = cmd == RespCommand.CustomRawStringCmd
                ? currentCustomRawStringCommand.type == CommandType.Read
                : currentCustomObjectCommand.type == CommandType.Read;

            csvi.keySpecs = CustomCommandSingleKeySpec;
            csvi.isSubCommand = false;
            csvi.readOnly = isReadOnly;
            csvi.sessionAsking = SessionAsking;
            csvi.waitForStableSlot = false;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}