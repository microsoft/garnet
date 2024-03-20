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
        private bool CheckIfKeyExists(byte[] key)
        {
            fixed (byte* keyPtr = key)
                return CheckIfKeyExists(new ArgSlice(keyPtr, key.Length));
        }

        private bool CheckIfKeyExists(ArgSlice keySlice)
            => basicGarnetApi.EXISTS(keySlice, StoreType.All) == GarnetStatus.OK;

        /// <summary>
        /// Redirect message for readonly operation COUNTKEYS GETKEYSINSLOT
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="config"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Redirect(ushort slot, ClusterConfig config)
        {
            var (address, port) = config.GetEndpointFromSlot(slot);
            ReadOnlySpan<byte> resp;
            if (port != 0)
                resp = Encoding.ASCII.GetBytes($"-MOVED {slot} {address}:{port}\r\n");
            else
                resp = "-CLUSTERDOWN Hash slot not served\r\n"u8;

            logger?.LogDebug("SEND: {msg}", Encoding.ASCII.GetString(resp).Replace("\n", "!").Replace("\r", "|"));
            while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                SendAndReset();
        }

        private void WriteClusterSlotVerificationMessage(ClusterConfig config, ClusterSlotVerificationResult vres, ref byte* dcurr, ref byte* dend)
        {
            ReadOnlySpan<byte> resp = default;
            SlotVerifiedState state = vres.state;
            ushort slot = vres.slot;
            string address;
            int port;
            switch (state)
            {
                case SlotVerifiedState.MOVED:
                    (address, port) = config.GetEndpointFromSlot(slot);
                    resp = Encoding.ASCII.GetBytes($"-MOVED {slot} {address}:{port}\r\n");
                    break;
                case SlotVerifiedState.MIGRATING:
                    resp = "-MIGRATING.\r\n"u8;
                    break;
                case SlotVerifiedState.CLUSTERDOWN:
                    resp = "-CLUSTERDOWN Hash slot not served\r\n"u8;
                    break;
                case SlotVerifiedState.ASK:
                    (address, port) = config.AskEndpointFromSlot(slot);
                    resp = Encoding.ASCII.GetBytes($"-ASK {slot} {address}:{port}\r\n");
                    break;
                case SlotVerifiedState.CROSSLOT:
                    resp = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"u8;
                    break;
                default:
                    throw new Exception($"Unknown SlotVerifiedState {state}");
            }
            while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
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
        public bool NetworkSingleKeySlotVerify(byte[] key, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend)
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
            //If cluster is not enabled or a transaction is running skip slot check
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
        /// Check if write is permitted on an array of RESP formatted keys starting at ptr, in sequence/interleaved with values and generate appropriate resp response.
        /// </summary>
        /// <param name="keyCount"></param>
        /// <param name="ptr"></param>
        /// <param name="endPtr"></param>        
        /// <param name="interleavedKeys"></param>
        /// <param name="readOnly"></param>
        /// <param name="SessionAsking"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <param name="retVal"></param>
        /// <returns>True if redirect, False if can serve</returns>
        public bool NetworkArraySlotVerify(int keyCount, ref byte* ptr, byte* endPtr, bool interleavedKeys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, out bool retVal)
        {
            retVal = false;
            //If cluster is not enabled or a transaction is running skip slot check
            if (!clusterProvider.serverOptions.EnableCluster || txnManager.state == TxnState.Running) return false;

            var config = clusterProvider.clusterManager.CurrentConfig;
            var vres = KeyArraySlotVerify(config, keyCount, ref ptr, endPtr, readOnly: readOnly, interleavedKeys: interleavedKeys, SessionAsking, out retVal);

            if (vres.state == SlotVerifiedState.OK)
                return false;
            else
            {
                WriteClusterSlotVerificationMessage(config, vres, ref dcurr, ref dend);
            }
            return true;
        }

        /// <summary>
        /// Check if read/write is permitted on an array of keys and generate appropriate resp response.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="readOnly"></param>
        /// <param name="SessionAsking"></param>
        /// <param name="dcurr"></param>
        /// <param name="dend"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, int count = -1)
        {
            //If cluster is not enabled or a transaction is running skip slot check
            if (!clusterProvider.serverOptions.EnableCluster || txnManager.state == TxnState.Running) return false;

            var config = clusterProvider.clusterManager.CurrentConfig;
            var vres = KeyArraySlotVerify(config, ref keys, readOnly, SessionAsking, count);

            if (vres.state == SlotVerifiedState.OK)
                return false;
            else
                WriteClusterSlotVerificationMessage(config, vres, ref dcurr, ref dend);
            return true;
        }
    }
}