// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ReplicaReplayDriverStore replicaReplayDriverStore = null;
        TsavoriteLog physicalSublog = null;

        /// <summary>
        /// Apply primary AOF records.
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <returns>
        /// A completed task when the record has been applied, or handed to the replay task, without waiting. Otherwise a
        /// task that completes once the session may go on to the next message, until which <paramref name="record"/> must
        /// stay valid.
        /// </returns>
        public ValueTask ProcessPrimaryStreamAsync(int physicalSublogIdx, byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress)
        {
            // logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            var aofReplayMaxLagBytes = clusterProvider.storeWrapper.runtimeConfig.GetInt(ServerConfigType.AOF_REPLAY_MAX_LAG_BYTES);
            var syncReplay = aofReplayMaxLagBytes == 0;

            // Need to ensure that this replay task is allowed to complete before the replicaReplayGroup is disposed
            // NOTE: this should not be expensive because every replay task has its own lock copy
            // Cache invalidation happens only on dispose which is rare operation
            var failReplay = syncReplay && !replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).ResumeReplay();

            // Whether the replay taken above is given up by the task returned instead of here
            var handedOff = false;
            try
            {
                if (failReplay)
                    throw new GarnetException($"[{physicalSublogIdx}] Failed to acquire activeReplay lock!", LogLevel.Warning, clientResponse: false);

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
                    // Dispatch moves a chunk that skips ahead off a thread that must not wait, as the reset waits on the device.
                    if (GarnetAppendOnlyFile.ReplicaStreamSkipsAhead(previousAddress, currentAddress, recordLength, clusterProvider.replicationManager.PageSizeBits))
                    {
                        logger?.LogWarning("MainMemoryReplication: Skipping from {ReplicaReplicationOffset} to {currentAddress}", clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx), currentAddress);
                        clusterProvider.storeWrapper.appendOnlyFile.Log.SafeInitialize(physicalSublogIdx, currentAddress, currentAddress);

                        // Vector set operations in progress finish before a new replication offset is advertised. They
                        // run on the thread pool, so the session parks rather than holds a thread until they do.
                        var vectorsDone = clusterProvider.storeWrapper.DefaultDatabase?.VectorManager?.WaitForVectorOperationsToCompleteAsync() ?? default;
                        if (!vectorsDone.IsCompletedSuccessfully)
                        {
                            handedOff = true;
                            return SkipAheadAsync(vectorsDone, physicalSublogIdx, (nint)record, recordLength, previousAddress, currentAddress, nextAddress, syncReplay);
                        }

                        clusterProvider.replicationManager.SetSublogReplicationOffset(physicalSublogIdx, currentAddress);
                    }
                }

                return AppendPrimaryStreamAsync(physicalSublogIdx, (nint)record, recordLength, previousAddress, currentAddress, nextAddress, syncReplay, out handedOff);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                throw new GarnetException(ex.Message, ex, LogLevel.Warning, clientResponse: false);
            }
            finally
            {
                if (syncReplay && !failReplay && !handedOff)
                    replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).SuspendReplay();
            }
        }

        /// <summary>
        /// Appends primary AOF records that follow on from this replica's tail, and applies them or hands them to the
        /// replay task.
        /// </summary>
        /// <param name="physicalSublogIdx">Sublog the records belong to.</param>
        /// <param name="record">Address of the records, valid until the task returned completes.</param>
        /// <param name="recordLength">Length of the records.</param>
        /// <param name="previousAddress">Primary address the records follow.</param>
        /// <param name="currentAddress">Primary address of the records.</param>
        /// <param name="nextAddress">Primary address after the records.</param>
        /// <param name="syncReplay">Whether the records are applied before the session goes on.</param>
        /// <param name="handedOff">Set when the task returned gives up the replay that the caller took.</param>
        /// <returns>As for <see cref="ProcessPrimaryStreamAsync"/>.</returns>
        ValueTask AppendPrimaryStreamAsync(int physicalSublogIdx, nint record, int recordLength, long previousAddress, long currentAddress, long nextAddress, bool syncReplay, out bool handedOff)
        {
            handedOff = false;

            // Injection for a "something went wrong with THIS Replica's AOF file"
            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Divergent_AOF_Stream);

            var tail = clusterProvider.storeWrapper.appendOnlyFile.Log.GetTailAddress(physicalSublogIdx);
            var nextPageBeginAddress = ((tail >> clusterProvider.replicationManager.PageSizeBits) + 1) << clusterProvider.replicationManager.PageSizeBits;
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
            if (syncReplay && clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx) != tail)
            {
                logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx), tail);
                throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx)}, aof.TailAddress {tail}", LogLevel.Warning, clientResponse: false);
            }

            // Initialize sublog ref if first time
            physicalSublog ??= clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(physicalSublogIdx);

            // Enqueue to AOF
            _ = physicalSublog.UnsafeEnqueueRaw(new Span<byte>((byte*)record, recordLength), noCommit: true);

            var driver = replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx);
            ValueTask pending;
            var pulseFollows = false;
            if (syncReplay)
            {
                // Synchronous replay
                pending = driver.ConsumeFromSessionAsync((byte*)record, recordLength, currentAddress, nextAddress, isProtected: false);
                if (pending.IsCompletedSuccessfully)
                    pending = driver.ThrottleAsync();
                else
                    pulseFollows = true;
            }
            else
            {
                // Initialize iterator and run background task once
                driver.InitializeBackgroundReplayTask(previousAddress);

                // Throttle to give the opportunity to the background replay task to catch up
                pending = driver.ThrottlePrimaryAsync();
            }

            if (pending.IsCompletedSuccessfully)
                return default;

            handedOff = true;
            return FinishReplayAsync(driver, pending, pulseFollows, ownsReplay: syncReplay);
        }
    }
}