// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Redirect message for readonly operation COUNTKEYS GETKEYSINSLOT
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="config"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Redirect(ushort slot, ClusterConfig config)
        {
            var (address, port) = config.GetEndpointFromSlot(slot);
            ReadOnlySpan<byte> errorMessage;
            if (port != 0)
                errorMessage = Encoding.ASCII.GetBytes($"MOVED {slot} {address}:{port}");
            else
                errorMessage = CmdStrings.RESP_ERR_CLUSTERDOWN;

            logger?.LogDebug("SEND: {msg}", Encoding.ASCII.GetString(errorMessage));
            while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                SendAndReset();
        }

        private void WriteClusterSlotVerificationMessage(ClusterConfig config, ClusterSlotVerificationResult vres, ref byte* dcurr, ref byte* dend)
        {
            ReadOnlySpan<byte> errorMessage;
            var state = vres.state;
            var slot = vres.slot;
            string address;
            int port;
            switch (state)
            {
                case SlotVerifiedState.MOVED:
                    (address, port) = config.GetEndpointFromSlot(slot);
                    errorMessage = Encoding.ASCII.GetBytes($"MOVED {slot} {address}:{port}");
                    break;
                case SlotVerifiedState.CLUSTERDOWN:
                    errorMessage = CmdStrings.RESP_ERR_CLUSTERDOWN;
                    break;
                case SlotVerifiedState.ASK:
                    (address, port) = config.AskEndpointFromSlot(slot);
                    errorMessage = Encoding.ASCII.GetBytes($"ASK {slot} {address}:{port}");
                    break;
                case SlotVerifiedState.CROSSSLOT:
                    errorMessage = CmdStrings.RESP_ERR_CROSSSLOT;
                    break;
                case SlotVerifiedState.TRYAGAIN:
                    errorMessage = CmdStrings.RESP_ERR_TRYAGAIN;
                    break;
                default:
                    throw new Exception($"Unknown SlotVerifiedState {state}");
            }
            while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                SendAndReset(ref dcurr, ref dend);
        }

        /// <summary>
        /// Check if read or read/write is permitted on a single key and generate the appropriate response
        ///         LOCAL   |   ~LOCAL  | MIGRATING EXISTS  |   MIGRATING ~EXISTS   |   IMPORTING ASKING    |   IMPORTING ~ASKING
        /// R       OK      |   -MOVED  |   OK              |   -ASK                |   OK                  |   -MOVED
        /// R/W     OK      |   -MOVED  |   -MIGRATING      |   -ASK                |   OK                  |   -MOVED
        /// </summary>
        /// <returns>True if redirect, False if can serve</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NetworkSingleKeySlotVerify(ReadOnlySpan<byte> key, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend)
        {
            fixed (byte* keyPtr = key)
                return NetworkSingleKeySlotVerify(new ArgSlice(keyPtr, key.Length), readOnly, SessionAsking, ref dcurr, ref dend);
        }

        /// <summary>
        /// Check if read or read/write is permitted on a single key and generate the appropriate response
        ///         LOCAL   |   ~LOCAL  | MIGRATING EXISTS  |   MIGRATING ~EXISTS   |   IMPORTING ASKING    |   IMPORTING ~ASKING
        /// R       OK      |   -MOVED  |   OK              |   -ASK                |   OK                  |   -MOVED
        /// R/W     OK      |   -MOVED  |   -MIGRATING      |   -ASK                |   OK                  |   -MOVED
        /// </summary>
        /// <returns>True if redirect, False if can serve</returns>
        public bool NetworkSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend)
        {
            // If cluster is not enabled or a transaction is running skip slot check
            if (!clusterProvider.serverOptions.EnableCluster || txnManager.state == TxnState.Running) return false;

            var config = clusterProvider.clusterManager.CurrentConfig;
            var vres = readOnly ? SingleKeyReadSlotVerify(config, keySlice, SessionAsking) : SingleKeyReadWriteSlotVerify(config, keySlice, SessionAsking);

            if (vres.state == SlotVerifiedState.OK)
                return false;
            else
                WriteClusterSlotVerificationMessage(config, vres, ref dcurr, ref dend);
            return true;
        }

        /// <summary>
        /// Check if read/write is permitted on an array of keys and generate appropriate resp response.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="readOnly"></param>
        /// <param name="sessionAsking"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public bool NetworkKeyArraySlotVerify(Span<ArgSlice> keys, bool readOnly, byte sessionAsking, ref byte* dcurr, ref byte* dend, int count = -1)
        {
            // If cluster is not enabled or a transaction is running skip slot check
            if (!clusterProvider.serverOptions.EnableCluster || txnManager.state == TxnState.Running) return false;

            var config = clusterProvider.clusterManager.CurrentConfig;
            var vres = MultiKeySlotVerify(config, ref keys, readOnly, sessionAsking, count);

            if (vres.state == SlotVerifiedState.OK)
                return false;
            else
                WriteClusterSlotVerificationMessage(config, vres, ref dcurr, ref dend);
            return true;
        }

        public unsafe bool NetworkMultiKeySlotVerify(SessionParseState parseState, ClusterSlotVerificationInput csvi, ref byte* dcurr, ref byte* dend)
        {
            // If cluster is not enabled or a transaction is running skip slot check
            if (!clusterProvider.serverOptions.EnableCluster || txnManager.state == TxnState.Running) return false;

            var config = clusterProvider.clusterManager.CurrentConfig;
            var vres = MultiKeySlotVerify(config, parseState, csvi);

            if (vres.state == SlotVerifiedState.OK)
                return false;
            else
                WriteClusterSlotVerificationMessage(config, vres, ref dcurr, ref dend);
            return true;
        }
    }
}