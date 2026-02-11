// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Replica read context used with sharded log
        /// </summary>
        ReplicaReadSessionContext replicaReadContext = new() { sessionVersion = -1, maximumSessionSequenceNumber = 0, lastVirtualSublogIdx = -1 };

        /// <summary>
        /// A CancellationTokenSource used to signal cancellation for consistent read operations.
        /// </summary>
        CancellationTokenSource consistentReadCts = new();

        /// <summary>
        /// This method is used to verify slot ownership for provided array of key argslices.
        /// </summary>
        /// <param name="keys">Array of key ArgSlice</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="count">Key count if different than keys array length</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(Span<PinnedSpanByte> keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(keys, readOnly, SessionAsking > 0, ref dcurr, ref dend, count);

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

        /// <summary>
        /// Consistent read key prepare callback
        /// </summary>
        /// <param name="key"></param>
        public void ValidateKeySequenceNumber(PinnedSpanByte key)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(consistentReadCts.Token);
            timeoutCts.CancelAfter(storeWrapper.serverOptions.ReplicaSyncTimeout);
            storeWrapper.appendOnlyFile.ConsistentReadKeyPrepare(key, ref replicaReadContext, timeoutCts.Token);
        }

        /// <summary>
        /// Consistent read key update callback
        /// </summary>
        public void UpdateKeySequenceNumber()
            => storeWrapper.appendOnlyFile.ConsistentReadSequenceNumberUpdate(ref replicaReadContext);
    }
}