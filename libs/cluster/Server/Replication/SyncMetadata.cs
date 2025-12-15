// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal static class SyncMetadataLoggingExtensions
    {
        /// <summary>
        /// Log sync metadata
        /// </summary>
        /// <param name="log"></param>
        /// <param name="logLevel"></param>
        /// <param name="msg"></param>
        /// <param name="syncMetadata"></param>
        public static void LogSyncMetadata(this ILogger log, LogLevel logLevel, string msg, SyncMetadata syncMetadata)
        {
            log.Log(logLevel,
                "\n" +
                "[{msg}]\n" +
                "fullSync:{fullSync}\n" +
                "originNodeRole:{originNodeRole}\n" +
                "originNodeId:{originNodeId}\n" +
                "currentPrimaryReplId:{currentPrimaryReplId}\n" +
                "currentStoreVersion:{currentStoreVersion}\n" +
                "currentAofBeginAddress:{currentAofBeginAddress}\n" +
                "currentAofTailAddress:{currentAofTailAddress}\n" +
                "currentReplicationOffset:{currentReplicationOffset}\n" +
                "checkpointEntry:{checkpointEntry}",
                msg,
                syncMetadata.fullSync,
                syncMetadata.originNodeRole,
                syncMetadata.originNodeId,
                syncMetadata.currentPrimaryReplId,
                syncMetadata.currentStoreVersion,
                syncMetadata.currentAofBeginAddress,
                syncMetadata.currentAofTailAddress,
                syncMetadata.currentReplicationOffset,
                syncMetadata.checkpointEntry);
        }

        /// <summary>
        /// Log sync metadata
        /// </summary>
        /// <param name="log"></param>
        /// <param name="logLevel"></param>
        /// <param name="msg"></param>
        /// <param name="origin"></param>
        /// <param name="local"></param>
        public static void LogSyncMetadata(this ILogger log, LogLevel logLevel, string msg, SyncMetadata origin, SyncMetadata local)
        {
            log.Log(logLevel,
                "\n" +
                "[{msg}]\n" +
                "<<< SyncMetadataReceivedFromReplicaToPrimary >>>\n" +
                "fullSync:{fullSync}\n" +
                "originNodeRole:{originNodeRole}\n" +
                "originNodeId:{originNodeId}\n" +
                "currentPrimaryReplId:{currentPrimaryReplId}\n" +
                "currentStoreVersion:{currentStoreVersion}\n" +
                "currentAofBeginAddress:{currentAofBeginAddress}\n" +
                "currentAofTailAddress:{currentAofTailAddress}\n" +
                "currentReplicationOffset:{currentReplicationOffset}\n" +
                "checkpointEntry:{checkpointEntry}\n" +
                "\n<<< SyncMetadataSendFromPrimaryToReplica >>\n" +
                "recoverFullSync:{fullSync}\n" +
                "recoverOriginNodeRole:{originNodeRole}\n" +
                "recoverOriginNodeId:{originNodeId}\n" +
                "recoverCurrentPrimaryReplId:{currentPrimaryReplId}\n" +
                "recoverCurrentStoreVersion:{currentStoreVersion}\n" +
                "recoverCurrentAofBeginAddress:{currentAofBeginAddress}\n" +
                "recoverCurrentAofTailAddress:{currentAofTailAddress}\n" +
                "recoverCurrentReplicationOffset:{currentReplicationOffset}\n" +
                "recoverCheckpointEntry:{checkpointEntry}",
                msg,
                origin.fullSync,
                origin.originNodeRole,
                origin.originNodeId,
                origin.currentPrimaryReplId,
                origin.currentStoreVersion,
                origin.currentAofBeginAddress,
                origin.currentAofTailAddress,
                origin.currentReplicationOffset,
                origin.checkpointEntry,
                local.fullSync,
                local.originNodeRole,
                local.originNodeId,
                local.currentPrimaryReplId,
                local.currentStoreVersion,
                local.currentAofBeginAddress,
                local.currentAofTailAddress,
                local.currentReplicationOffset,
                local.checkpointEntry);
        }
    }

    internal sealed class SyncMetadata(
        bool fullSync,
        NodeRole originNodeRole,
        string originNodeId,
        string currentPrimaryReplId,
        long currentStoreVersion,
        long currentAofBeginAddress,
        long currentAofTailAddress,
        long currentReplicationOffset,
        CheckpointEntry checkpointEntry)
    {
        public readonly bool fullSync = fullSync;
        public readonly NodeRole originNodeRole = originNodeRole;
        public readonly string originNodeId = originNodeId;
        public readonly string currentPrimaryReplId = currentPrimaryReplId;
        public readonly long currentStoreVersion = currentStoreVersion;
        public readonly long currentAofBeginAddress = currentAofBeginAddress;
        public readonly long currentAofTailAddress = currentAofTailAddress;
        public readonly long currentReplicationOffset = currentReplicationOffset;
        public readonly CheckpointEntry checkpointEntry = checkpointEntry;

        public byte[] ToByteArray()
        {
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.ASCII);

            writer.Write(fullSync);
            writer.Write((byte)originNodeRole);
            writer.Write(originNodeId);
            writer.Write(currentPrimaryReplId);

            writer.Write(currentStoreVersion);

            writer.Write(currentAofBeginAddress);
            writer.Write(currentAofTailAddress);
            writer.Write(currentReplicationOffset);

            if (checkpointEntry != null)
            {
                var bb = checkpointEntry.ToByteArray();
                writer.Write(bb.Length);
                writer.Write(bb);
            }
            else
            {
                writer.Write(0);
            }

            return ms.ToArray();
        }

        public static SyncMetadata FromByteArray(byte[] serialized)
        {
            using var ms = new MemoryStream(serialized);
            using var reader = new BinaryReader(ms);
            var syncMetadata = new SyncMetadata
            (
                fullSync: reader.ReadBoolean(),
                originNodeRole: (NodeRole)reader.ReadByte(),
                originNodeId: reader.ReadString(),
                currentPrimaryReplId: reader.ReadString(),
                currentStoreVersion: reader.ReadInt64(),
                currentAofBeginAddress: reader.ReadInt64(),
                currentAofTailAddress: reader.ReadInt64(),
                currentReplicationOffset: reader.ReadInt64(),
                checkpointEntry: CheckpointEntry.FromByteArray(reader.ReadBytes(reader.ReadInt32()))
            );
            return syncMetadata;
        }
    }
}