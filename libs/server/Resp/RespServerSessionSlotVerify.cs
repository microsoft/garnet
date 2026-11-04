// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

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
        /// Whether the cluster session may hold replies that it writes only once their work completes, in command order:
        /// a disk-based sync chunk's acknowledgement waits for its write. Only a CLUSTER command defers a reply, so this is
        /// refreshed after each one, and any other reply first calls <see cref="DrainDeferredClusterReplies"/>. Caching the
        /// flag keeps the check per command to a field read. It may be stale-true once the replies have been sent, which
        /// costs one empty drain, but never stale-false: replies are deferred only on this thread.
        /// </summary>
        bool clusterRepliesMayBeDeferred;

        /// <summary>
        /// Writes the replies that the cluster session deferred, waiting for their work to complete, so that the next
        /// reply follows them.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void DrainDeferredClusterReplies()
        {
            clusterSession.DrainDeferredReplies(ref dcurr, ref dend);
            clusterRepliesMayBeDeferred = false;
        }

        /// <summary>
        /// Validates whether a command can be served based on the current slot assignment
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        bool CanServeSlot(RespCommand cmd)
        {
            Debug.Assert(clusterSession != null);

            // Replies that earlier CLUSTER SNAPSHOT_DATA chunks deferred until their writes complete precede this
            // command's reply. Further chunks may keep deferring theirs.
            if (clusterRepliesMayBeDeferred && cmd != RespCommand.CLUSTER_SNAPSHOT_DATA)
                DrainDeferredClusterReplies();

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

        /// <summary>
        /// Verifies, without writing a redirect, that the key(s) in the parse state for <paramref name="cmd"/>
        /// are servable by this node. Returns true if servable. Used by the scatter-gather GET loop, where keys
        /// past the first bypass the per-command <see cref="CanServeSlot"/> in ProcessMessages.
        /// </summary>
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
            return !clusterSession.NetworkMultiKeySlotVerifyNoResponse(ref parseState, ref csvi, ref dcurr, ref dend);
        }
    }
}