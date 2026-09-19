// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;

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
        CheckpointComplete = 5
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
                StandaloneReplicationFrameType.CheckpointFile =>
                    payloadLength > 0 && checkpointFileType != CheckpointFileType.NONE && token != default && address >= 0,
                StandaloneReplicationFrameType.CheckpointMetadata =>
                    payloadLength > 0 && checkpointFileType is CheckpointFileType.STORE_INDEX or CheckpointFileType.STORE_SNAPSHOT &&
                    token != default && address == -1,
                StandaloneReplicationFrameType.CheckpointFileEnd =>
                    payloadLength == 0 && checkpointFileType != CheckpointFileType.NONE && token != default && address >= 0,
                StandaloneReplicationFrameType.CheckpointComplete =>
                    payloadLength > 0 && checkpointFileType == CheckpointFileType.NONE && token != default && address >= 0,
                _ => false
            };

            if (!valid)
                return false;

            frameHeader = new(type, checkpointFileType, payloadLength, token, address);
            return true;
        }
    }
}