// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using Garnet.common;

namespace Garnet.server
{
    internal sealed class RdbReadBudget
    {
        int bytes;
        int entries;

        internal void AddBytes(int count)
        {
            if (count < 0 || count > RdbSerializer.MaxBytes - bytes)
                throw new RdbException(RdbSerializer.SizeError);
            bytes += count;
        }

        internal void AddEntry()
        {
            if (++entries > RdbSerializer.MaxEntries)
                throw new RdbException(RdbSerializer.SizeError);
        }
    }

    internal ref struct RdbReader(ReadOnlySpan<byte> data, RdbReadBudget budget)
    {
        readonly ReadOnlySpan<byte> data = data;
        readonly RdbReadBudget budget = budget;
        internal int Position { get; private set; }
        internal int Remaining => data.Length - Position;

        internal ReadOnlySpan<byte> Read(int count)
        {
            if (count < 0 || count > Remaining)
                throw new RdbException(RdbSerializer.FormatError);
            var result = data.Slice(Position, count);
            Position += count;
            return result;
        }

        internal byte ReadByte() => Read(1)[0];

        internal int ReadLength()
        {
            if (!RespLengthEncodingUtils.TryReadLength(data[Position..], out var length, out var count))
                throw new RdbException(RdbSerializer.FormatError);
            Position += count;
            return length;
        }

        internal int ReadCount(int minimumEntryBytes)
        {
            var count = ReadLength();
            if (count == 0 || count > Remaining / minimumEntryBytes)
                throw new RdbException(RdbSerializer.FormatError);
            if (count > RdbSerializer.MaxEntries)
                throw new RdbException(RdbSerializer.SizeError);
            return count;
        }

        internal byte[] ReadString()
        {
            if (Remaining == 0)
                throw new RdbException(RdbSerializer.FormatError);
            if ((data[Position] & 0xC0) != 0xC0)
            {
                var length = ReadLength();
                var value = Read(length);
                budget.AddBytes(length);
                return value.ToArray();
            }

            var encoding = ReadByte() & 0x3F;
            switch (encoding)
            {
                case 0:
                    return Integer((sbyte)ReadByte());
                case 1:
                    return Integer(BinaryPrimitives.ReadInt16LittleEndian(Read(2)));
                case 2:
                    return Integer(BinaryPrimitives.ReadInt32LittleEndian(Read(4)));
                case 3:
                    var compressedLength = ReadLength();
                    var length = ReadLength();
                    var compressed = Read(compressedLength);
                    budget.AddBytes(length);
                    return Decompress(compressed, length);
                default:
                    throw new RdbException(RdbSerializer.FormatError);
            }
        }

        internal byte[] Integer(long value)
        {
            Span<byte> bytes = stackalloc byte[20];
            _ = Utf8Formatter.TryFormat(value, bytes, out var count);
            budget.AddBytes(count);
            return bytes[..count].ToArray();
        }

        internal byte[] Copy(ReadOnlySpan<byte> value)
        {
            budget.AddBytes(value.Length);
            return value.ToArray();
        }

        internal void RequireEnd()
        {
            if (Remaining != 0)
                throw new RdbException(RdbSerializer.FormatError);
        }

        static byte[] Decompress(ReadOnlySpan<byte> input, int length)
        {
            // Each LZF back reference produces at most 264 bytes from at least two input bytes.
            if (length > (long)input.Length * 132)
                throw new RdbException(RdbSerializer.FormatError);
            var result = new byte[length];
            var source = 0;
            var target = 0;
            while (source < input.Length)
            {
                var control = input[source++];
                if (control < 32)
                {
                    var count = control + 1;
                    if (count > input.Length - source || count > length - target)
                        throw new RdbException(RdbSerializer.FormatError);
                    input.Slice(source, count).CopyTo(result.AsSpan(target));
                    source += count;
                    target += count;
                }
                else
                {
                    var count = control >> 5;
                    var offset = (control & 31) << 8;
                    if (count == 7)
                    {
                        if (source == input.Length)
                            throw new RdbException(RdbSerializer.FormatError);
                        count += input[source++];
                    }
                    if (source == input.Length)
                        throw new RdbException(RdbSerializer.FormatError);
                    offset += input[source++] + 1;
                    count += 2;
                    if (offset > target || count > length - target)
                        throw new RdbException(RdbSerializer.FormatError);
                    for (var i = 0; i < count; i++)
                    {
                        result[target] = result[target - offset];
                        target++;
                    }
                }
            }
            if (target != length)
                throw new RdbException(RdbSerializer.FormatError);
            return result;
        }
    }
}
