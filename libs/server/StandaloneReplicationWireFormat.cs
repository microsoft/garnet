// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Garnet.server
{
    /// <summary>
    /// Payload type carried by a standalone Garnet replication frame.
    /// </summary>
    internal enum StandaloneReplicationFrameType
    {
        AofRecord = 1,
        CheckpointFile = 2,
        CheckpointMetadata = 3,
        CheckpointFileEnd = 4,
        CheckpointComplete = 5,
        StreamReady = 6,
        AofBatch = 7
    }

    /// <summary>
    /// Parsed standalone replication frame header.
    /// </summary>
    internal readonly record struct StandaloneReplicationFrameHeader(
        StandaloneReplicationFrameType Type,
        CheckpointFileType CheckpointFileType,
        int PayloadLength,
        Guid Token,
        long Address);

    /// <summary>
    /// Wire framing for Garnet checkpoint and AOF data sent after the Redis-compatible PSYNC preamble.
    /// </summary>
    internal static class StandaloneReplicationWireFormat
    {
        internal const int HeaderLength = 48;
        internal const int MaxPayloadLength = 512 * 1024 * 1024;

        const uint Magic = 0x464F4147;
        const int Version = 2;

        internal static byte[] SerializeCheckpointMetadata(CheckpointMetadata metadata)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream, Encoding.ASCII);
            writer.Write(metadata.storeVersion);
            writer.Write(metadata.storeHlogToken.ToByteArray());
            writer.Write(metadata.storeIndexToken.ToByteArray());
            metadata.storeCheckpointCoveredAofAddress.Serialize(writer);
            writer.Write(metadata.storePrimaryReplId ?? string.Empty);
            return stream.ToArray();
        }

        internal static CheckpointMetadata DeserializeCheckpointMetadata(ReadOnlySpan<byte> payload, int physicalSublogCount)
        {
            using var stream = new MemoryStream(payload.ToArray());
            using var reader = new BinaryReader(stream, Encoding.ASCII);
            var metadata = new CheckpointMetadata(physicalSublogCount)
            {
                storeVersion = reader.ReadInt64(),
                storeHlogToken = new Guid(reader.ReadBytes(16)),
                storeIndexToken = new Guid(reader.ReadBytes(16)),
                storeCheckpointCoveredAofAddress = AofAddress.Deserialize(reader),
                storePrimaryReplId = reader.ReadString()
            };
            if (stream.Position != stream.Length)
                throw new InvalidDataException("Checkpoint metadata frame contains trailing data");
            return metadata;
        }

        internal static void WriteHeader(
            Span<byte> header,
            StandaloneReplicationFrameType type,
            CheckpointFileType checkpointFileType,
            int payloadLength,
            Guid token,
            long address)
        {
            if (header.Length < HeaderLength)
                throw new ArgumentException($"Replication frame header must be at least {HeaderLength} bytes", nameof(header));

            BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
            BinaryPrimitives.WriteInt32LittleEndian(header[4..], Version);
            BinaryPrimitives.WriteInt32LittleEndian(header[8..], (int)type);
            BinaryPrimitives.WriteInt32LittleEndian(header[12..], (int)checkpointFileType);
            BinaryPrimitives.WriteInt32LittleEndian(header[16..], payloadLength);
            BinaryPrimitives.WriteInt32LittleEndian(header[20..], 0);
            token.TryWriteBytes(header[24..40]);
            BinaryPrimitives.WriteInt64LittleEndian(header[40..], address);
        }

        internal static bool TryReadHeader(ReadOnlySpan<byte> header, out StandaloneReplicationFrameHeader frameHeader)
        {
            frameHeader = default;

            if (header.Length != HeaderLength ||
                BinaryPrimitives.ReadUInt32LittleEndian(header) != Magic ||
                BinaryPrimitives.ReadInt32LittleEndian(header[4..]) != Version)
                return false;

            var type = (StandaloneReplicationFrameType)BinaryPrimitives.ReadInt32LittleEndian(header[8..]);
            var checkpointFileType = (CheckpointFileType)BinaryPrimitives.ReadInt32LittleEndian(header[12..]);
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header[16..]);
            var token = new Guid(header[24..40]);
            var address = BinaryPrimitives.ReadInt64LittleEndian(header[40..]);

            if (payloadLength < 0 || payloadLength > MaxPayloadLength)
                return false;

            var valid = type switch
            {
                StandaloneReplicationFrameType.AofRecord =>
                    payloadLength > 0 && checkpointFileType == CheckpointFileType.NONE && token == default && address >= 0,
                StandaloneReplicationFrameType.AofBatch =>
                    payloadLength >= sizeof(long) + sizeof(int) && checkpointFileType == CheckpointFileType.NONE &&
                    token == default && address >= 0,
                StandaloneReplicationFrameType.CheckpointFile =>
                    payloadLength > 0 && checkpointFileType != CheckpointFileType.NONE && token != default && address >= 0,
                StandaloneReplicationFrameType.CheckpointMetadata =>
                    payloadLength > 0 && checkpointFileType is CheckpointFileType.STORE_INDEX or CheckpointFileType.STORE_SNAPSHOT &&
                    token != default && address == -1,
                StandaloneReplicationFrameType.CheckpointFileEnd =>
                    payloadLength == 0 && checkpointFileType != CheckpointFileType.NONE && token != default && address >= 0,
                StandaloneReplicationFrameType.CheckpointComplete =>
                    payloadLength > 0 && checkpointFileType == CheckpointFileType.NONE && token != default && address >= 0,
                StandaloneReplicationFrameType.StreamReady =>
                    payloadLength == 0 && checkpointFileType == CheckpointFileType.NONE && token == default && address >= 0,
                _ => false
            };

            if (!valid)
                return false;

            frameHeader = new(type, checkpointFileType, payloadLength, token, address);
            return true;
        }

        internal static void WriteAofBatchRecord(
            IBufferWriter<byte> writer,
            long currentAddress,
            ReadOnlySpan<byte> record)
        {
            var recordHeader = writer.GetSpan(sizeof(long) + sizeof(int));
            BinaryPrimitives.WriteInt64LittleEndian(recordHeader, currentAddress);
            BinaryPrimitives.WriteInt32LittleEndian(recordHeader[sizeof(long)..], record.Length);
            writer.Advance(sizeof(long) + sizeof(int));

            record.CopyTo(writer.GetSpan(record.Length));
            writer.Advance(record.Length);
        }

        internal static bool TryReadAofBatchRecord(
            ref ReadOnlySpan<byte> payload,
            out long currentAddress,
            out ReadOnlySpan<byte> record)
        {
            currentAddress = 0;
            record = default;

            if (payload.Length < sizeof(long) + sizeof(int))
                return false;

            currentAddress = BinaryPrimitives.ReadInt64LittleEndian(payload);
            var recordLength = BinaryPrimitives.ReadInt32LittleEndian(payload[sizeof(long)..]);
            if (currentAddress < 0 || recordLength <= 0 ||
                recordLength > payload.Length - sizeof(long) - sizeof(int))
                return false;

            record = payload.Slice(sizeof(long) + sizeof(int), recordLength);
            payload = payload[(sizeof(long) + sizeof(int) + recordLength)..];
            return true;
        }
    }
}