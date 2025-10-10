// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        ReplicaAofSyncReplayTask replicaAofSyncTask = null;

        /// <summary>
        /// Reset background replay iterator
        /// </summary>
        public void ResetReplayIterator()
            => replicaAofSyncTask?.ResetReplayIterator();

        /// <summary>
        /// Apply primary AOF records.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        public unsafe void ProcessPrimaryStream(int sublogIdx, byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress)
        {
            // logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            try
            {
                if (clusterProvider.replicationManager.CannotStreamAOF)
                {
                    logger?.LogError("Replica is recovering cannot sync AOF");
                    throw new GarnetException("Replica is recovering cannot sync AOF", LogLevel.Warning, clientResponse: false);
                }

                if (currentConfig.LocalNodeRole != NodeRole.REPLICA)
                {
                    logger?.LogWarning("This node {nodeId} is not a replica", currentConfig.LocalNodeId);
                    throw new GarnetException($"This node {currentConfig.LocalNodeId} is not a replica", LogLevel.Warning, clientResponse: false);
                }

                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    // If the incoming AOF chunk fits in the space between previousAddress and currentAddress (ReplicationOffset),
                    // an enqueue will result in an offset mismatch. So, we have to first reset the AOF to point to currentAddress.
                    if (currentAddress > previousAddress)
                    {
                        if (
                            (currentAddress % (1 << pageSizeBits) != 0) || // the skip was to a non-page-boundary
                            (currentAddress >= previousAddress + recordLength) // the skip will not be auto-handled by the AOF enqueue
                            )
                        {
                            logger?.LogWarning("MainMemoryReplication: Skipping from {ReplicaReplicationOffset} to {currentAddress}", ReplicationOffset, currentAddress);
                            storeWrapper.appendOnlyFile.SafeInitialize(sublogIdx, currentAddress, currentAddress);
                            replicationOffset[0] = currentAddress;
                        }
                    }
                }

                // Injection for a "something went wrong with THIS Replica's AOF file"
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Divergent_AOF_Stream);

                ref var tail = ref storeWrapper.appendOnlyFile.Log.TailAddress;
                var nextPageBeginAddress = ((tail[sublogIdx] >> pageSizeBits) + 1) << pageSizeBits;
                // Check to ensure:
                // 1. if record fits in current page tailAddress of this local node (replica) should be equal to the incoming currentAddress (address of chunk send from primary node)
                // 2. if record does not fit in current page start address of the next page matches incoming currentAddress (address of chunk send from primary node)
                // otherwise fail and break the connection
                if ((tail[sublogIdx] + recordLength <= nextPageBeginAddress && tail[sublogIdx] != currentAddress) ||
                    (tail[sublogIdx] + recordLength > nextPageBeginAddress && nextPageBeginAddress != currentAddress))
                {
                    logger?.LogError("Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    throw new GarnetException($"Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", LogLevel.Warning, clientResponse: false);
                }

                // Address check only if synchronous replication is enabled
                if (storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0 && ReplicationOffset[sublogIdx] != storeWrapper.appendOnlyFile.Log.TailAddress[sublogIdx])
                {
                    logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.Log.TailAddress);
                    logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", ReplicationOffset, storeWrapper.appendOnlyFile.Log.TailAddress);
                    throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.Log.TailAddress}", LogLevel.Warning, clientResponse: false);
                }

                // Check that sublogIdx received is one expected
                replicaAofSyncTask?.ValidateSublogIndex(sublogIdx);

                // Enqueue to AOF
                _ = clusterProvider.storeWrapper.appendOnlyFile?.UnsafeEnqueueRaw(
                    sublogIdx,
                    new Span<byte>(record, recordLength),
                    noCommit: clusterProvider.serverOptions.EnableFastCommit);

                replicaAofSyncTask ??= new ReplicaAofSyncReplayTask(sublogIdx, clusterProvider, logger);

                if (storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0)
                {
                    // Synchronous replay
                    replicaAofSyncTask.Consume(record, recordLength, currentAddress, nextAddress, isProtected: false);
                }
                else
                {
                    // Initialize iterator and run background task once
                    replicaAofSyncTask.InitializeIterator(previousAddress);

                    // Throttle to give the opportunity to the background replay task to catch up
                    replicaAofSyncTask?.ThrottlePrimary();
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                ResetReplayIterator();
                throw new GarnetException(ex.Message, ex, LogLevel.Warning, clientResponse: false);
            }
        }
    }
}