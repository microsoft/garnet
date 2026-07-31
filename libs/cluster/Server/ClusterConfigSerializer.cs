// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Garnet.cluster
{
    internal sealed partial class ClusterConfig
    {
        private const byte ClientEndpointExtensionVersion = 1;

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
        /// Serialize config to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);

            // Write serialization format version
            writer.Write(ClusterConfigVersion);

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
            }

            if (HasClientEndpointMetadata())
            {
                writer.Write(ClientEndpointExtensionVersion);
                for (int i = 1; i < workers.Length; i++)
                {
                    Worker worker = workers[i];
                    writer.Write(worker.HasClientEndpointMetadata);
                    if (!worker.HasClientEndpointMetadata)
                        continue;

                    WriteNullableString(writer, worker.ClientAddress);
                    writer.Write(worker.ClientPort);
                    WriteNullableString(writer, worker.ClientHostname);
                }
            }

            byte[] byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        private bool HasClientEndpointMetadata()
        {
            for (int i = 1; i < workers.Length; i++)
            {
                if (workers[i].HasClientEndpointMetadata)
                    return true;
            }

            return false;
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
            if (version != ClusterConfigVersion)
                throw new InvalidDataException($"Incompatible ClusterConfig version: expected {ClusterConfigVersion}, got {version}");

            var newSlotMap = DeserializeSlotMap(ref reader);

            int numWorkers = reader.ReadInt32();
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
            }

            if (ms.Position < ms.Length)
            {
                byte extensionVersion = reader.ReadByte();
                if (extensionVersion != ClientEndpointExtensionVersion)
                    throw new InvalidDataException($"Incompatible client endpoint extension version: expected {ClientEndpointExtensionVersion}, got {extensionVersion}");

                for (int i = 1; i < numWorkers; i++)
                {
                    newWorkers[i].HasClientEndpointMetadata = reader.ReadBoolean();
                    if (!newWorkers[i].HasClientEndpointMetadata)
                        continue;

                    newWorkers[i].ClientAddress = ReadNullableString(reader);
                    newWorkers[i].ClientPort = reader.ReadInt32();
                    newWorkers[i].ClientHostname = ReadNullableString(reader);
                }
            }

            if (ms.Position != ms.Length)
                throw new InvalidDataException("Invalid ClusterConfig payload: trailing data after client endpoint extension");

            reader.Dispose();
            ms.Dispose();
            return new ClusterConfig(newSlotMap, newWorkers);
        }

        private static void WriteNullableString(BinaryWriter writer, string value)
        {
            writer.Write(value != null);
            if (value != null)
                writer.Write(value);
        }

        private static string ReadNullableString(BinaryReader reader)
            => reader.ReadBoolean() ? reader.ReadString() : null;

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