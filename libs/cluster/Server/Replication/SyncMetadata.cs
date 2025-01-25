// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Text;

namespace Garnet.cluster
{
    sealed class SyncMetadata(
        bool fullSync,
        NodeRole originNodeRole,
        string originNodeId,
        string currentPrimaryReplId,
        long currentStoreVersion,
        long currentObjectStoreVersion,
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
        public readonly long currentObjectStoreVersion = currentObjectStoreVersion;
        public readonly long currentAofBeginAddress = currentAofBeginAddress;
        public readonly long currentAofTailAddress = currentAofTailAddress;
        public readonly long currentReplicationOffset = currentReplicationOffset;
        public readonly CheckpointEntry checkpointEntry = checkpointEntry;

        public byte[] ToByteArray()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms, Encoding.ASCII);

            writer.Write(fullSync);
            writer.Write((byte)originNodeRole);
            writer.Write(originNodeId);
            writer.Write(currentPrimaryReplId);

            writer.Write(currentStoreVersion);
            writer.Write(currentObjectStoreVersion);

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

            var byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        public static SyncMetadata FromByteArray(byte[] serialized)
        {
            var ms = new MemoryStream(serialized);
            var reader = new BinaryReader(ms);
            var syncMetadata = new SyncMetadata
            (
                fullSync: reader.ReadBoolean(),
                originNodeRole: (NodeRole)reader.ReadByte(),
                originNodeId: reader.ReadString(),
                currentPrimaryReplId: reader.ReadString(),
                currentStoreVersion: reader.ReadInt64(),
                currentObjectStoreVersion: reader.ReadInt64(),
                currentAofBeginAddress: reader.ReadInt64(),
                currentAofTailAddress: reader.ReadInt64(),
                currentReplicationOffset: reader.ReadInt64(),
                checkpointEntry: CheckpointEntry.FromByteArray(reader.ReadBytes(reader.ReadInt32()))
            );

            reader.Dispose();
            ms.Dispose();
            return syncMetadata;
        }
    }
}