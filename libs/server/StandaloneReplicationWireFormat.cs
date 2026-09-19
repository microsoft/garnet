// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;

namespace Garnet.server
{
    /// <summary>
    /// Wire framing for Garnet AOF records sent after the Redis-compatible PSYNC snapshot.
    /// </summary>
    internal static class StandaloneReplicationWireFormat
    {
        internal const int HeaderLength = 20;
        internal const int MaxPayloadLength = 512 * 1024 * 1024;

        const uint Magic = 0x464F4147;
        const int Version = 1;

        internal static void WriteHeader(Span<byte> header, int payloadLength, long currentAddress)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
            BinaryPrimitives.WriteInt32LittleEndian(header[4..], Version);
            BinaryPrimitives.WriteInt32LittleEndian(header[8..], payloadLength);
            BinaryPrimitives.WriteInt64LittleEndian(header[12..], currentAddress);
        }

        internal static bool TryReadHeader(ReadOnlySpan<byte> header, out int payloadLength, out long currentAddress)
        {
            payloadLength = 0;
            currentAddress = 0;

            if (header.Length != HeaderLength ||
                BinaryPrimitives.ReadUInt32LittleEndian(header) != Magic ||
                BinaryPrimitives.ReadInt32LittleEndian(header[4..]) != Version)
                return false;

            payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header[8..]);
            currentAddress = BinaryPrimitives.ReadInt64LittleEndian(header[12..]);
            return payloadLength > 0 && payloadLength <= MaxPayloadLength && currentAddress >= 0;
        }
    }
}