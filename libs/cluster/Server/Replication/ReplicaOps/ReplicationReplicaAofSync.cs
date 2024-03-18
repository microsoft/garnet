// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
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
                if (clusterProvider.replicationManager.recovering)
                {
                    logger?.LogWarning("Replica is recovering cannot sync AOF");
                    throw new Exception("Replica is recovering cannot sync AOF");
                }

                if (currentConfig.GetLocalNodeRole() != NodeRole.REPLICA)
                {
                    logger?.LogWarning("This node {nodeId} is not a replica", currentConfig.GetLocalNodeId());
                    throw new Exception($"This node {currentConfig.GetLocalNodeId()} is not a replica");
                }

                if (clusterProvider.serverOptions.MainMemoryReplication)
                {
                    int firstRecordLength = GetFirstAofEntryLength(record);
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
                    throw new Exception($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.TailAddress}");
                }

                // Enqueue to AOF
                clusterProvider.storeWrapper.appendOnlyFile?.UnsafeEnqueueRaw(new Span<byte>(record, recordLength), noCommit: clusterProvider.serverOptions.EnableFastCommit);

                // TODO: rest of the processing can be moved off the critical path

                ReplicationOffset = currentAddress;
                byte* ptr = record;
                while (ptr < record + recordLength)
                {
                    int entryLength = storeWrapper.appendOnlyFile.HeaderSize;
                    int payloadLength = storeWrapper.appendOnlyFile.UnsafeGetLength(ptr);
                    if (payloadLength > 0)
                    {
                        aofProcessor.ProcessAofRecordInternal(null, ptr + entryLength, payloadLength, true);
                        entryLength += storeWrapper.appendOnlyFile.UnsafeAlign(payloadLength);
                    }
                    else if (payloadLength < 0)
                    {
                        if (!clusterProvider.serverOptions.EnableFastCommit)
                        {
                            throw new Exception("Received FastCommit request at replica AOF processor, but FastCommit is not enabled");
                        }
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new BinaryReader(new UnmanagedMemoryStream(ptr + entryLength, -payloadLength)));
                        storeWrapper.appendOnlyFile?.UnsafeCommitMetadataOnly(info);
                        entryLength += storeWrapper.appendOnlyFile.UnsafeAlign(-payloadLength);
                    }
                    ptr += entryLength;
                    ReplicationOffset += entryLength;
                }

                if (ReplicationOffset != nextAddress)
                {
                    logger?.LogError("Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, nextAddress {nextAddress}", ReplicationOffset, nextAddress);
                    throw new Exception($"Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, nextAddress {nextAddress}");
                }

                if (ReplicationOffset != storeWrapper.appendOnlyFile.TailAddress)
                {
                    logger?.LogError("After ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", ReplicationOffset, storeWrapper.appendOnlyFile.TailAddress);
                    throw new Exception($"After ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicationOffset}, aof.TailAddress {storeWrapper.appendOnlyFile.TailAddress}");
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                throw;
            }
        }

        unsafe int GetFirstAofEntryLength(byte* ptr)
        {
            int entryLength = storeWrapper.appendOnlyFile.HeaderSize;
            int payloadLength = storeWrapper.appendOnlyFile.UnsafeGetLength(ptr);
            if (payloadLength > 0)
            {
                entryLength += storeWrapper.appendOnlyFile.UnsafeAlign(payloadLength);
            }
            else if (payloadLength < 0)
            {
                entryLength += storeWrapper.appendOnlyFile.UnsafeAlign(-payloadLength);
            }
            return entryLength;
        }
    }
}