// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
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
                if (clusterProvider.replicationManager.Recovering)
                {
                    logger?.LogWarning("Replica is recovering cannot sync AOF");
                    throw new GarnetException("Replica is recovering cannot sync AOF", LogLevel.Warning);
                }

                if (currentConfig.LocalNodeRole != NodeRole.REPLICA)
                {
                    logger?.LogWarning("This node {nodeId} is not a replica", currentConfig.LocalNodeId);
                    throw new GarnetException($"This node {currentConfig.LocalNodeId} is not a replica", LogLevel.Warning);
                }

                if (clusterProvider.serverOptions.MainMemoryReplication)
                {
                    var firstRecordLength = GetFirstAofEntryLength(record);
                    if (previousAddress > ReplicationOffset ||
                        currentAddress > previousAddress + firstRecordLength)
                    {
                        logger?.LogWarning("MainMemoryReplication: Skipping from {ReplicaReplicationOffset} to {currentAddress}", ReplicationOffset, currentAddress);
                        storeWrapper.appendOnlyFile.Initialize(currentAddress, currentAddress);
                        ReplicationOffset = currentAddress;
                    }
                }

                // Address check
                if (ReplicationOffset != storeWrapper.appendOnlyFile.TailAddress)
                {
                    logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
                    logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", ReplicationOffset, storeWrapper.appendOnlyFile.TailAddress);
                    throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.TailAddress}", LogLevel.Warning);
                }

                // Enqueue to AOF
                _ = clusterProvider.storeWrapper.appendOnlyFile?.UnsafeEnqueueRaw(new Span<byte>(record, recordLength), noCommit: clusterProvider.serverOptions.EnableFastCommit);

                // TODO: rest of the processing can be moved off the critical path

                ReplicationOffset = currentAddress;
                var ptr = record;
                while (ptr < record + recordLength)
                {
                    var entryLength = storeWrapper.appendOnlyFile.HeaderSize;
                    var payloadLength = storeWrapper.appendOnlyFile.UnsafeGetLength(ptr);
                    if (payloadLength > 0)
                    {
                        aofProcessor.ProcessAofRecordInternal(null, ptr + entryLength, payloadLength, true);
                        entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                    }
                    else if (payloadLength < 0)
                    {
                        if (!clusterProvider.serverOptions.EnableFastCommit)
                        {
                            throw new Exception("Received FastCommit request at replica AOF processor, but FastCommit is not enabled");
                        }
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                        storeWrapper.appendOnlyFile?.UnsafeCommitMetadataOnly(info);
                        entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                    }
                    ptr += entryLength;
                    ReplicationOffset += entryLength;
                }

                if (ReplicationOffset != nextAddress)
                {
                    logger?.LogWarning("Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, nextAddress {nextAddress}", ReplicationOffset, nextAddress);
                    throw new GarnetException($"Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, nextAddress {nextAddress}", LogLevel.Warning);
                }

                if (ReplicationOffset != storeWrapper.appendOnlyFile.TailAddress)
                {
                    logger?.LogWarning("After ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", ReplicationOffset, storeWrapper.appendOnlyFile.TailAddress);
                    throw new GarnetException($"After ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.TailAddress}", LogLevel.Warning);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                throw new GarnetException(ex.Message, ex, LogLevel.Warning);
            }
        }

        unsafe int GetFirstAofEntryLength(byte* ptr)
        {
            int entryLength = storeWrapper.appendOnlyFile.HeaderSize;
            int payloadLength = storeWrapper.appendOnlyFile.UnsafeGetLength(ptr);
            if (payloadLength > 0)
            {
                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
            }
            else if (payloadLength < 0)
            {
                entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
            }
            return entryLength;
        }
    }
}