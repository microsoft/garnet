// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using Garnet.common;

namespace Garnet.server
{
    internal sealed class RdbWriter : IMemoryOwner<byte>
    {
        const int PrefixReserve = 16;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(256);
        int position = PrefixReserve;
        int start;

        public Memory<byte> Memory => buffer.AsMemory(start, position - start);

        internal void WriteByte(byte value) => Reserve(1)[0] = value;

        internal void Write(ReadOnlySpan<byte> value) => value.CopyTo(Reserve(value.Length));

        internal void WriteLength(int length)
        {
            Span<byte> encoded = stackalloc byte[5];
            if (!RespLengthEncodingUtils.TryWriteLength(length, encoded, out var count))
                throw new RdbException(RdbSerializer.SizeError);
            Write(encoded[..count]);
        }

        internal void WriteString(ReadOnlySpan<byte> value)
        {
            WriteLength(value.Length);
            Write(value);
        }

        internal void WriteDouble(double value) => BinaryPrimitives.WriteDoubleLittleEndian(Reserve(8), value);

        internal void FinishPayload()
        {
            BinaryPrimitives.WriteUInt16LittleEndian(Reserve(2), RdbSerializer.Version);
            Reserve(8).Clear();
        }

        internal int FrameBulkString()
        {
            var length = position - PrefixReserve;
            Span<byte> digits = stackalloc byte[10];
            _ = Utf8Formatter.TryFormat(length, digits, out var count);
            start = PrefixReserve - count - 3;
            buffer[start] = (byte)'$';
            digits[..count].CopyTo(buffer.AsSpan(start + 1));
            buffer[PrefixReserve - 2] = (byte)'\r';
            buffer[PrefixReserve - 1] = (byte)'\n';
            Write("\r\n"u8);
            return position - start;
        }

        Span<byte> Reserve(int length)
        {
            if (length < 0 || length > RdbSerializer.MaxBytes + PrefixReserve + 2 - position)
                throw new RdbException(RdbSerializer.SizeError);

            var required = position + length;
            if (required > buffer.Length)
            {
                var replacement = ArrayPool<byte>.Shared.Rent(Math.Max(required, buffer.Length * 2));
                buffer.AsSpan(0, position).CopyTo(replacement);
                ArrayPool<byte>.Shared.Return(buffer);
                buffer = replacement;
            }

            var result = buffer.AsSpan(position, length);
            position = required;
            return result;
        }

        public void Dispose()
        {
            if (buffer is null)
                return;
            ArrayPool<byte>.Shared.Return(buffer);
            buffer = null;
        }
    }

    internal sealed class RdbException(string message) : Exception(message);
}
