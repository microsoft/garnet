// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Text;
using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.Extensions.RoaringBitmap
{
    /// <summary>
    /// Helpers shared by all RoaringBitmap commands. Centralises argument parsing
    /// so error messages stay consistent.
    /// </summary>
    internal static class RoaringBitmapArgs
    {
        /// <summary>Parse an unsigned 32-bit decimal integer offset. Sets a generic error and returns false on bad input.</summary>
        public static bool TryParseUInt32(ReadOnlySpan<byte> raw, out uint value)
        {
            if (Utf8Parser.TryParse(raw, out long signed, out int consumed) && consumed == raw.Length && signed >= 0 && signed <= uint.MaxValue)
            {
                value = (uint)signed;
                return true;
            }
            value = 0;
            return false;
        }

        public static bool TryParseBit(ReadOnlySpan<byte> raw, out bool bit)
        {
            if (raw.Length == 1 && raw[0] == (byte)'0') { bit = false; return true; }
            if (raw.Length == 1 && raw[0] == (byte)'1') { bit = true; return true; }
            bit = false;
            return false;
        }
    }

    /// <summary>
    /// R.SETBIT key offset value
    ///
    /// Sets or clears the bit at <c>offset</c> (a 32-bit unsigned integer) in the
    /// Roaring bitmap stored at <c>key</c>. Returns the previous bit value (0 or 1).
    /// </summary>
    public sealed class RSetBit : CustomObjectFunctions
    {
        private static ReadOnlySpan<byte> ErrOffset => "ERR bit offset is not an unsigned 32-bit integer"u8;
        private static ReadOnlySpan<byte> ErrValue => "ERR bit value must be 0 or 1"u8;

        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref RespMemoryWriter writer)
        {
            // Validate args BEFORE the framework allocates the empty object so that
            // a malformed R.SETBIT does not leave a tombstone-style empty key behind.
            // Walk a copy of the input so Updater can re-parse from offset 0.
            var validation = input;
            int offset = 0;
            var offsetArg = GetNextArg(ref validation, ref offset);
            var bitArg = GetNextArg(ref validation, ref offset);

            if (!RoaringBitmapArgs.TryParseUInt32(offsetArg, out _))
            {
                writer.WriteError(ErrOffset);
                return false;
            }
            if (!RoaringBitmapArgs.TryParseBit(bitArg, out _))
            {
                writer.WriteError(ErrValue);
                return false;
            }
            return true;
        }

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is RoaringBitmapObject);
            var rb = (RoaringBitmapObject)value;

            int offset = 0;
            var offsetArg = GetNextArg(ref input, ref offset);
            var bitArg = GetNextArg(ref input, ref offset);

            if (!RoaringBitmapArgs.TryParseUInt32(offsetArg, out uint bitOffset))
                return AbortWithErrorMessage(ref writer, ErrOffset);
            if (!RoaringBitmapArgs.TryParseBit(bitArg, out bool bit))
                return AbortWithErrorMessage(ref writer, ErrValue);

            int previous = rb.SetBit(bitOffset, bit);
            writer.WriteInt32(previous);
            return true;
        }
    }

    /// <summary>
    /// R.GETBIT key offset
    ///
    /// Returns the bit at <c>offset</c> in the Roaring bitmap. Returns 0 if the key
    /// does not exist or the offset has never been set. Registered as
    /// ReadModifyWrite so that missing-key handling can route through
    /// <see cref="NeedInitialUpdate"/> without creating an empty key.
    /// </summary>
    public sealed class RGetBit : CustomObjectFunctions
    {
        private static ReadOnlySpan<byte> ErrOffset => "ERR bit offset is not an unsigned 32-bit integer"u8;

        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref RespMemoryWriter writer)
        {
            // Missing key: validate args, write 0, decline to create a key.
            var offsetArg = GetFirstArg(ref input);
            if (!RoaringBitmapArgs.TryParseUInt32(offsetArg, out _))
            {
                writer.WriteError(ErrOffset);
                return false;
            }
            writer.WriteInt32(0);
            return false;
        }

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is RoaringBitmapObject);
            var rb = (RoaringBitmapObject)value;

            var offsetArg = GetFirstArg(ref input);
            if (!RoaringBitmapArgs.TryParseUInt32(offsetArg, out uint bitOffset))
                return AbortWithErrorMessage(ref writer, ErrOffset);
            writer.WriteInt32(rb.GetBit(bitOffset));
            return true;
        }
    }

    /// <summary>
    /// R.BITCOUNT key
    ///
    /// Returns the number of bits set to 1 in the Roaring bitmap. Returns 0 if the
    /// key does not exist. Registered as ReadModifyWrite (no actual mutation) for
    /// the same NeedInitialUpdate-on-miss reason as <see cref="RGetBit"/>.
    /// </summary>
    public sealed class RBitCount : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref RespMemoryWriter writer)
        {
            writer.WriteInt64(0);
            return false;
        }

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is RoaringBitmapObject);
            var rb = (RoaringBitmapObject)value;
            writer.WriteInt64(rb.BitCount());
            return true;
        }
    }

    /// <summary>
    /// R.BITPOS key bit [from]
    ///
    /// Returns the position of the first bit equal to <c>bit</c> (0 or 1) at or
    /// after the optional <c>from</c> offset (default 0). Returns -1 when no match
    /// exists in the uint32 universe. Registered as ReadModifyWrite (no actual
    /// mutation) for the same NeedInitialUpdate-on-miss reason as <see cref="RGetBit"/>.
    /// </summary>
    public sealed class RBitPos : CustomObjectFunctions
    {
        private static ReadOnlySpan<byte> ErrBit => "ERR bit must be 0 or 1"u8;
        private static ReadOnlySpan<byte> ErrFrom => "ERR from offset is not an unsigned 32-bit integer"u8;

        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref RespMemoryWriter writer)
        {
            int offset = 0;
            var bitArg = GetNextArg(ref input, ref offset);
            if (!RoaringBitmapArgs.TryParseBit(bitArg, out bool bit))
            {
                writer.WriteError(ErrBit);
                return false;
            }

            uint from = 0;
            if (input.parseState.Count > offset)
            {
                var fromArg = GetNextArg(ref input, ref offset);
                if (!RoaringBitmapArgs.TryParseUInt32(fromArg, out from))
                {
                    writer.WriteError(ErrFrom);
                    return false;
                }
            }

            // Missing key: bit==1 -> -1; bit==0 -> first unset is `from` (0 by default).
            if (bit) writer.WriteInt64(-1);
            else writer.WriteInt64(from);
            return false;
        }

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is RoaringBitmapObject);
            var rb = (RoaringBitmapObject)value;

            int offset = 0;
            var bitArg = GetNextArg(ref input, ref offset);
            if (!RoaringBitmapArgs.TryParseBit(bitArg, out bool bit))
                return AbortWithErrorMessage(ref writer, ErrBit);

            uint from = 0;
            if (input.parseState.Count > offset)
            {
                var fromArg = GetNextArg(ref input, ref offset);
                if (!RoaringBitmapArgs.TryParseUInt32(fromArg, out from))
                    return AbortWithErrorMessage(ref writer, ErrFrom);
            }

            long pos = rb.BitPos(bit ? 1 : 0, from);
            writer.WriteInt64(pos);
            return true;
        }
    }
}