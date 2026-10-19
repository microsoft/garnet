// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

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
            csvi.waitForStableSlot = cmd is RespCommand.VADD or RespCommand.VREM or RespCommand.VSETATTR or RespCommand.XVCREATE or RespCommand.XVIMPORT;
            csvi.canWait = CanSuspend;
            csvi.canOffload = csvi.canWait && ThreadPoolHop.Required;
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
            csvi.canWait = CanSuspend;
            csvi.canOffload = csvi.canWait && ThreadPoolHop.Required;
            return !clusterSession.NetworkMultiKeySlotVerify(ref parseState, ref csvi, ref dcurr, ref dend);
        }

        /// <summary>
        /// Verifies, without writing a redirect, that the key(s) in the parse state for <paramref name="cmd"/>
        /// are servable by this node. Returns true if servable. Used by the scatter-gather GET loop, where keys
        /// past the first bypass the per-command <see cref="CanServeSlot"/> in ProcessMessages.
        /// </summary>
        /// <remarks>
        /// A key being migrated counts as not servable, without this call waiting for the migration or for the device.
        /// The caller stops its run there and leaves the command to <see cref="CanServeSlot"/>, which parks the session.
        /// </remarks>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlotNoResponse(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            cmd = cmd.NormalizeForACLs();
            if (!RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var cmdInfo))
                return false;

            if (cmdInfo.KeySpecs == null || cmdInfo.KeySpecs.Length == 0)
                return true;

            csvi.keySpecs = cmdInfo.KeySpecs;
            csvi.isSubCommand = cmdInfo.IsSubCommand;
            csvi.readOnly = cmd.IsReadOnly();
            csvi.sessionAsking = SessionAsking;
            csvi.waitForStableSlot = false;
            csvi.canWait = CanSuspend;
            csvi.canOffload = csvi.canWait && ThreadPoolHop.Required;
            return !clusterSession.NetworkMultiKeySlotVerifyNoResponse(ref parseState, ref csvi, ref dcurr, ref dend);
        }

        /// <summary>
        /// Verifies a key that the <c>Prepare</c> of a custom transaction adds to the keys it locks, against the keys it
        /// added before. Whether a key being migrated may defer the transaction is set by
        /// <see cref="TryTransactionProc"/> before it runs the transaction.
        /// </summary>
        /// <param name="key">Key to verify.</param>
        /// <param name="readOnly">Whether the transaction only reads the key.</param>
        /// <returns>True if the transaction can be served here so far.</returns>
        internal bool CanServeTransactionKey(PinnedSpanByte key, bool readOnly)
        {
            csvi.readOnly = readOnly;
            csvi.sessionAsking = SessionAsking;
            csvi.waitForStableSlot = false;
            return clusterSession.NetworkIterativeSlotVerify(key, ref csvi);
        }

        /// <summary>
        /// Whether a slot verification has left the command to be run again from the start, once a key being migrated
        /// is accessible or on the thread pool, rather than answered.
        /// </summary>
        internal bool SlotVerificationDeferred => csvi.mustWait || csvi.mustOffload;
    }
}