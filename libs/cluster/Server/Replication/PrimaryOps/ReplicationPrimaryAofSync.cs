// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        // Must be the same as the TsavoriteAof start address of allocator
        public static readonly long kFirstValidAofAddress = 64;
        readonly AofSyncDriverStore aofSyncDriverStore;

        public AofSyncDriverStore AofSyncDriverStore => aofSyncDriverStore;

        public int ConnectedReplicasCount => aofSyncDriverStore.CountConnectedReplicas();

        public List<RoleInfo> GetReplicaInfo() => aofSyncDriverStore.GetReplicaInfo(ReplicationOffset);

        /// <summary>
        /// Try to initiate connection from primary to replica in order to stream aof.
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="startAddress"></param>
        /// <param name="aofSyncDriver"></param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns></returns>
        public bool TryConnectToReplica(string nodeid, ref AofAddress startAddress, AofSyncDriver aofSyncDriver, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            if (_disposed)
            {
                aofSyncDriverStore.TryRemove(aofSyncDriver);

                errorMessage = "ERR Replication Manager Disposed"u8;
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            // TODO: why do we need to verify this?
            // No guarantee at call time that provided nodeId is of a trusted node because of gossip propagation delay
            var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(nodeid);
            if (address == null)
            {
                aofSyncDriverStore.TryRemove(aofSyncDriver);
                errorMessage = Encoding.ASCII.GetBytes($"ERR unknown endpoint for {nodeid}");
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            var tailAddress = storeWrapper.appendOnlyFile.Log.TailAddress;
            // Check if requested AOF address goes beyond the maximum available AOF address of this primary
            if (startAddress.AnyGreater(tailAddress))
            {
                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    logger?.LogWarning("MainMemoryReplication: Requested address {startAddress} unavailable. Local primary tail address {tailAddress}. Proceeding as best effort.", startAddress, tailAddress);
                }
                else
                {
                    aofSyncDriverStore.TryRemove(aofSyncDriver);
                    logger?.LogError("AOF sync task failed to start. Requested address {startAddress} unavailable. Local primary tail address {tailAddress}", startAddress, tailAddress);
                    errorMessage = Encoding.ASCII.GetBytes($"ERR requested AOF address: {startAddress} goes beyond, primary tail address: {tailAddress}");
                    return false;
                }
            }

            Task.Run(aofSyncDriver.Run);
            return true;
        }
    }
}