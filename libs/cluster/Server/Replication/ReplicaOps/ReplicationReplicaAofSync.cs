// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ThrottlePrimary()
        {
            while (storeWrapper.serverOptions.ReplicationOffsetMaxLag != -1 && replayIterator != null && storeWrapper.appendOnlyFile.TailAddress - ReplicationOffset > storeWrapper.serverOptions.ReplicationOffsetMaxLag)
            {
                replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                Thread.Yield();
            }
        }

        /// <summary>
        /// Apply primary AOF records.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        public unsafe void ProcessPrimaryStream(byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress)
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
                            storeWrapper.appendOnlyFile.SafeInitialize(currentAddress, currentAddress);
                            ReplicationOffset = currentAddress;
                        }
                    }
                }

                // Injection for a "something went wrong with THIS Replica's AOF file"
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Divergent_AOF_Stream);

                var tail = storeWrapper.appendOnlyFile.TailAddress;
                var nextPageBeginAddress = ((tail >> pageSizeBits) + 1) << pageSizeBits;
                // Check to ensure:
                // 1. if record fits in current page tailAddress of this local node (replica) should be equal to the incoming currentAddress (address of chunk send from primary node)
                // 2. if record does not fit in current page start address of the next page matches incoming currentAddress (address of chunk send from primary node)
                // otherwise fail and break the connection
                if ((tail + recordLength <= nextPageBeginAddress && tail != currentAddress) ||
                    (tail + recordLength > nextPageBeginAddress && nextPageBeginAddress != currentAddress))
                {
                    logger?.LogError("Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    throw new GarnetException($"Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", LogLevel.Warning, clientResponse: false);
                }

                // Address check only if synchronous replication is enabled
                if (storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0 && ReplicationOffset != storeWrapper.appendOnlyFile.TailAddress)
                {
                    logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
                    logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", ReplicationOffset, storeWrapper.appendOnlyFile.TailAddress);
                    throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.TailAddress}", LogLevel.Warning, clientResponse: false);
                }

                // Enqueue to AOF
                _ = clusterProvider.storeWrapper.appendOnlyFile?.UnsafeEnqueueRaw(new Span<byte>(record, recordLength), noCommit: clusterProvider.serverOptions.EnableFastCommit);

                if (storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0)
                {
                    // Synchronous replay
                    Consume(record, recordLength, currentAddress, nextAddress, isProtected: false);
                }
                else
                {
                    // Throttle to give the opportunity to the background replay task to catch up
                    ThrottlePrimary();

                    // If background task has not been initialized
                    // initialize it here and start background replay task
                    if (replayIterator == null)
                    {
                        replayIterator = clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(
                            previousAddress,
                            long.MaxValue,
                            scanUncommitted: true,
                            recover: false,
                            logger: logger);

                        System.Threading.Tasks.Task.Run(ReplicaReplayTask);
                    }
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