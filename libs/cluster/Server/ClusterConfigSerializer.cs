// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Garnet.cluster
{
    internal sealed partial class ClusterConfig
    {
        /// <summary>
        /// Peek the serialization version from a config byte array without full deserialization.
        /// </summary>
        /// <param name="data">Serialized cluster config payload.</param>
        /// <param name="version">The version byte at the start of the payload.</param>
        /// <returns>True if the payload is large enough to contain a version byte; false otherwise.</returns>
        public static bool TryPeekVersion(ReadOnlySpan<byte> data, out byte version)
        {
            if (data.Length < 1)
            {
                version = 0;
                return false;
            }
            version = data[0];
            return true;
        }

        /// <summary>
        /// Check whether a cluster config serialization version can be read.
        /// </summary>
        /// <param name="version">Serialization version.</param>
        public static bool IsSupportedVersion(byte version) => version >= MinimumSupportedClusterConfigVersion && version <= MaximumSupportedClusterConfigVersion;

        /// <summary>
        /// Serialize config for persistence, including peer endpoints.
        /// </summary>
        public byte[] ToByteArray() => ToByteArray(MaximumSupportedClusterConfigVersion);

        internal bool CanUseGossipVersion(byte version)
        {
            if (!IsSupportedVersion(version))
                return false;
            if (version >= 2)
                return true;
            for (int index = 1; index < workers.Length; index++)
            {
                if (workers[index].Address != workers[index].PeerAddress || workers[index].Port != workers[index].PeerPort)
                    return false;
            }
            return true;
        }

        internal byte[] ToGossipByteArray(byte version)
        {
            if (version == 1 && !CanUseGossipVersion(version))
                throw new InvalidDataException("Gossip version one cannot represent separate client and peer endpoints.");
            return ToByteArray(version);
        }

        /// <summary>
        /// Serialize config using the specified format version.
        /// </summary>
        /// <param name="version">Serialization version.</param>
        public byte[] ToByteArray(byte version)
        {
            if (!IsSupportedVersion(version))
                throw new ArgumentOutOfRangeException(nameof(version), version, "Unsupported ClusterConfig version");

            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);

            // Write serialization format version
            writer.Write(version);

            SerializeSlotMap(ref ms, ref writer);

            //Serialize worker info
            //4 bytes + 400 * N
            writer.Write(workers.Length);
            for (int i = 1; i < workers.Length; i++)
            {
                var worker = workers[i];
                //40 bytes
                writer.Write(worker.Nodeid);

                //16 bytes
                writer.Write(worker.Address);

                //29 bytes
                writer.Write(worker.Port);
                writer.Write(worker.ConfigEpoch);
                writer.Write((byte)worker.Role);

                //1 byte
                writer.Write(worker.ReplicaOfNodeId == null ? (byte)0 : (byte)1);

                if (worker.ReplicaOfNodeId != null)
                    //40 bytes
                    writer.Write(worker.ReplicaOfNodeId);

                //4 bytes
                writer.Write(worker.ReplicationOffset);
                //1 byte
                writer.Write(worker.hostname == null ? (byte)0 : (byte)1);
                if (worker.hostname != null)
                    //256 bytes
                    writer.Write(worker.hostname);

                if (version >= 2)
                {
                    // An empty address preserves missing metadata across upgraded relays and recovery.
                    writer.Write(worker.ClusterAddress ?? "");
                    writer.Write(worker.ClusterPort);
                }
            }

            byte[] byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        private void SerializeSlotMap(ref MemoryStream ms, ref BinaryWriter writer)
        {
            //serialize slotMap
            var segmentCountPosition = ms.Position;
            ms.Position += 2;
            ushort segmentCount = 0;
            ushort count = 1;
            ushort workerId = slotMap[0]._workerId;
            byte state = (byte)slotMap[0]._state;

            for (int i = 1; i < slotMap.Length; i++)
            {
                var _state = (byte)slotMap[i]._state;

                if (slotMap[i]._workerId != workerId || _state != state)
                {
                    segmentCount++;
                    //Write continuous segment
                    writer.Write(count);
                    writer.Write(workerId);
                    writer.Write(state);

                    //reset segment info
                    count = 1;
                    workerId = slotMap[i]._workerId;
                    state = (byte)slotMap[i]._state;
                    continue;
                }
                count++;
            }

            //Count and write last continuous segment
            segmentCount++;
            writer.Write(count);
            writer.Write(workerId);
            writer.Write(state);

            //Write segment count at the reserved position
            var _position = ms.Position;
            ms.Position = segmentCountPosition;
            writer.Write(segmentCount);
            ms.Position = _position;
        }

        /// <summary>
        /// Deserialize config from byte array
        /// </summary>
        public static ClusterConfig FromByteArray(byte[] other)
        {
            var ms = new MemoryStream(other);
            var reader = new BinaryReader(ms);

            // Read and validate serialization format version
            if (other.Length < 1)
                throw new InvalidDataException("Invalid ClusterConfig payload: too short to contain a version");
            var version = reader.ReadByte();
            if (!IsSupportedVersion(version))
                throw new InvalidDataException($"Incompatible ClusterConfig version: expected {MinimumSupportedClusterConfigVersion} through {MaximumSupportedClusterConfigVersion}, got {version}");

            var newSlotMap = DeserializeSlotMap(ref reader);

            int numWorkers = reader.ReadInt32();
            ValidateWorkerCount(numWorkers, version, other.Length - ms.Position);
            var newWorkers = new Worker[numWorkers];
            for (int i = 1; i < numWorkers; i++)
            {
                newWorkers[i].Nodeid = reader.ReadString();
                newWorkers[i].Address = reader.ReadString();
                newWorkers[i].Port = reader.ReadInt32();
                newWorkers[i].ConfigEpoch = reader.ReadInt64();
                newWorkers[i].Role = (NodeRole)reader.ReadByte();

                byte isNull = reader.ReadByte();
                if (isNull > 0)
                    newWorkers[i].ReplicaOfNodeId = reader.ReadString();

                newWorkers[i].ReplicationOffset = reader.ReadInt64();

                isNull = reader.ReadByte();
                if (isNull > 0)
                    newWorkers[i].hostname = reader.ReadString();

                if (version >= 2)
                {
                    var clusterAddress = reader.ReadString();
                    newWorkers[i].ClusterAddress = clusterAddress.Length == 0 ? null : clusterAddress;
                    newWorkers[i].ClusterPort = reader.ReadInt32();
                }
            }

            reader.Dispose();
            ms.Dispose();
            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Rejects a worker count that the payload cannot hold before the worker array is sized from it.
        /// Slots refer to workers by a 16-bit id, and every serialized worker takes at least a fixed number of
        /// bytes, so a count outside either bound can only come from a malformed or malicious payload.
        /// </summary>
        /// <param name="numWorkers">Declared worker count, including the unused entry at index 0.</param>
        /// <param name="version">Serialization version of the payload.</param>
        /// <param name="remainingBytes">Payload bytes that follow the worker count.</param>
        private static void ValidateWorkerCount(int numWorkers, byte version, long remainingBytes)
        {
            // Empty Nodeid and Address strings (1 byte each), Port, ConfigEpoch, Role, ReplicaOf flag,
            // ReplicationOffset and hostname flag; version 2 adds an empty ClusterAddress and ClusterPort.
            var minWorkerBytes = version >= 2 ? 30 : 25;

            if (numWorkers < 1 || numWorkers > ushort.MaxValue + 1 || (long)(numWorkers - 1) * minWorkerBytes > remainingBytes)
                throw new InvalidDataException($"Invalid ClusterConfig payload: worker count {numWorkers} does not fit the payload");
        }

        private static HashSlot[] DeserializeSlotMap(ref BinaryReader reader)
        {
            var newSlotMap = new HashSlot[16384];
            ushort segmentCount = reader.ReadUInt16();
            int slotOffset = 0;
            for (int i = 0; i < segmentCount; i++)
            {
                int count = reader.ReadUInt16();
                ushort workerId = reader.ReadUInt16();
                SlotState state = (SlotState)reader.ReadByte();

                count += slotOffset;
                if (count > newSlotMap.Length)
                    throw new InvalidDataException("Invalid ClusterConfig payload: slot segments exceed the slot range");
                while (slotOffset < count)
                {
                    newSlotMap[slotOffset]._workerId = workerId;
                    newSlotMap[slotOffset]._state = state;
                    slotOffset++;
                }
            }
            return newSlotMap;
        }
    }
}