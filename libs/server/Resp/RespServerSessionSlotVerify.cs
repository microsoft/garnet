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
        // Single-key spec for custom raw-string/object commands (key at parseState[0])
        private static readonly SimpleRespKeySpec[] CustomCommandSingleKeySpec =
        [
            new SimpleRespKeySpec
            {
                BeginSearch = new SimpleRespKeySpecBeginSearch(index: 1),
                FindKeys = new SimpleRespKeySpecFindKeys(keyStep: 1, lastKeyOrLimit: 0, isLimit: false),
            }
        ];

        /// <summary>
        /// Validate if this command can be served based on the current slot assignment
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlot(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            // CustomRawStringCmd and CustomObjCmd fall outside IsDataCommand()'s range and
            // would otherwise skip slot verification, so route them through a synthetic
            // single-key spec (both dispatch via parseState[0] as the only key)
            if (cmd is RespCommand.CustomRawStringCmd or RespCommand.CustomObjCmd)
            {
                // We can't use cmd.IsReadOnly() since both custom enum values fall above LastReadCommand
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
            csvi.waitForStableSlot = cmd is RespCommand.VADD or RespCommand.VREM or RespCommand.VSETATTR;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}