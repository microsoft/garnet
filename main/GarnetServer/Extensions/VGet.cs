// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// VGET key
    ///
    /// Returns RESP array: [ dim (bulk string), dtype (bulk string), payload (bulk string) ]
    ///
    /// Value layout in store (written by VADD):
    /// [0]    : byte version = 1
    /// [1]    : byte dtype   (1=f32, 2=f16, 3=u8)
    /// [2..5] : int32 dim (LE)
    /// [6.. ] : payload bytes
    /// </summary>
    sealed class VGetCustomCommand : CustomRawStringFunctions
    {
        const int HeaderLen = 1 + 1 + 4;

        // Read-only command: only Reader is used
        public override bool Reader(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            // Key not found → RESP null
            if (value.Length == 0)
            {
                writer.WriteNull();
                return true;
            }

            if (value.Length < HeaderLen)
            {
                writer.WriteError("ERR VGET malformed value");
                return true;
            }

            byte version = value[0]; // currently unused, version=1
            byte dtypeCode = value[1];
            int dim = BinaryPrimitives.ReadInt32LittleEndian(value.Slice(2, 4));
            var payload = value.Slice(HeaderLen);

            // RESP: [ dim, dtype, payload ]
            writer.WriteArrayLength(3);
            WriteIntAsBulkString(ref writer, dim);           // dim as bulk string (ASCII)
            writer.WriteBulkString(GetDTypeName(dtypeCode)); // "f32", "f16", "u8" or "unk"
            writer.WriteBulkString(payload);
            return true;
        }

        // Unused for a pure read command
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ref RespMemoryWriter writer) => false;
        public override int GetInitialLength(ref RawStringInput input) => throw new InvalidOperationException();
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new InvalidOperationException();
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new InvalidOperationException();
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer) => throw new InvalidOperationException();
        public override int GetLength(ReadOnlySpan<byte> value, ref RawStringInput input) => throw new InvalidOperationException();
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new InvalidOperationException();

        private static ReadOnlySpan<byte> GetDTypeName(byte code) => code switch
        {
            1 => "f32"u8,
            2 => "f16"u8,
            3 => "u8"u8,
            _ => "unk"u8
        };

        // Writes an int as ASCII bulk string using a small stack buffer
        private static void WriteIntAsBulkString(ref RespMemoryWriter writer, int value)
        {
            Span<byte> buf = stackalloc byte[20]; // plenty for int
            int i = buf.Length;
            uint v = (uint)(value < 0 ? -value : value);
            do
            {
                buf[--i] = (byte)('0' + (v % 10));
                v /= 10;
            } while (v != 0);
            if (value < 0) buf[--i] = (byte)'-';
            writer.WriteBulkString(buf.Slice(i));
        }
    }
}

