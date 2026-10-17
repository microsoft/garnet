// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal static class RdbSerializer
    {
        internal const ushort Version = 11;
        internal const int MaxBytes = 512 * 1024 * 1024;
        internal const int MaxEntries = 1_000_000;
        internal const string FormatError = "ERR Bad data format";
        internal const string FooterError = "ERR DUMP payload version or checksum are wrong";
        internal const string SizeError = "ERR DUMP/RESTORE value exceeds the supported size limit";
        internal const string ItemExpirationError = "ERR DUMP does not support live item expiration";

        internal static void SerializeString(ReadOnlySpan<byte> value, ref SpanByteAndMemory output)
        {
            Span<byte> encodedLength = stackalloc byte[5];
            if (!RespLengthEncodingUtils.TryWriteLength(value.Length, encodedLength, out var lengthBytes) ||
                value.Length > MaxBytes - lengthBytes - 11)
                throw new RdbException(SizeError);

            var payloadLength = value.Length + lengthBytes + 11;
            var totalLength = payloadLength + NumUtils.CountDigits(payloadLength) + 5;
            if (!output.IsSpanByte || totalLength > output.Length)
                output.EnsureHeapMemorySize(totalLength);

            var buffer = output.Span;
            buffer[0] = (byte)'$';
            _ = Utf8Formatter.TryFormat(payloadLength, buffer[1..], out var digits);
            var offset = digits + 1;
            "\r\n"u8.CopyTo(buffer[offset..]);
            offset += 2;
            buffer[offset++] = 0;
            encodedLength[..lengthBytes].CopyTo(buffer[offset..]);
            offset += lengthBytes;
            value.CopyTo(buffer[offset..]);
            offset += value.Length;
            BinaryPrimitives.WriteUInt16LittleEndian(buffer[offset..], Version);
            offset += 2;
            "\r\n"u8.CopyTo(buffer[(offset + 8)..]);
            output.Length = totalLength;
        }

        internal static void FinishReply(ref SpanByteAndMemory output)
        {
            var buffer = output.Span;
            if (buffer.Length < 18 || buffer[0] != (byte)'$' || buffer[1] == (byte)'-')
                return;

            var payloadStart = buffer.IndexOf((byte)'\n') + 1;
            var checksum = Crc64.Hash(buffer[payloadStart..^10]);
            if (!BitConverter.IsLittleEndian)
                Array.Reverse(checksum);
            checksum.CopyTo(buffer[^10..]);
        }

        internal static bool TrySerialize<TSourceLogRecord>(in TSourceLogRecord record, RdbWriter writer)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!record.DataHeader.ValueIsObject)
            {
                if (record.RecordType != 0)
                    return false;
                writer.WriteByte(0);
                writer.WriteString(record.ValueSpan);
                return true;
            }

            var now = DateTimeOffset.UtcNow.Ticks;
            switch (record.ValueObject)
            {
                case ListObject list:
                    writer.WriteByte(1);
                    WriteCount(writer, list.LnkList.Count);
                    foreach (var item in list.LnkList)
                        writer.WriteString(item);
                    return list.LnkList.Count != 0;
                case SetObject set:
                    writer.WriteByte(2);
                    WriteCount(writer, set.Set.Count);
                    foreach (var item in set.Set)
                        writer.WriteString(item);
                    return set.Set.Count != 0;
                case HashObject hash:
                    return hash.WriteRdb(writer, now);
                case SortedSetObject sortedSet:
                    return sortedSet.WriteRdb(writer, now);
                default:
                    return false;
            }
        }

        internal static void WriteCount(RdbWriter writer, int count)
        {
            if (count > MaxEntries)
                throw new RdbException(SizeError);
            writer.WriteLength(count);
        }

        internal static bool TryDeserialize(ReadOnlySpan<byte> payload, out ReadOnlySpan<byte> value, out IGarnetObject obj,
            out string error)
        {
            value = default;
            obj = null;
            error = null;
            if (payload.Length < 12 || BinaryPrimitives.ReadUInt16LittleEndian(payload[^10..]) > Version)
            {
                error = FooterError;
                return false;
            }
            if (payload.Length > MaxBytes)
            {
                error = SizeError;
                return false;
            }
            var checksum = Crc64.Hash(payload[..^8]);
            if (!BitConverter.IsLittleEndian)
                Array.Reverse(checksum);
            if (!payload[^8..].SequenceEqual(checksum))
            {
                error = FooterError;
                return false;
            }

            if (payload[0] == 0 && (payload[1] & 0xC0) != 0xC0)
            {
                if (!RespLengthEncodingUtils.TryReadLength(payload[1..^10], out var length, out var lengthBytes) ||
                    length != payload.Length - lengthBytes - 11)
                {
                    error = FormatError;
                    return false;
                }
                value = payload.Slice(1 + lengthBytes, length);
                return true;
            }

            var budget = new RdbReadBudget();
            var reader = new RdbReader(payload[1..^10], budget);
            try
            {
                switch (payload[0])
                {
                    case 0:
                        value = reader.ReadString();
                        break;
                    case 1:
                    case 2:
                    case 4:
                    case 5:
                        obj = CreateObject(payload[0]);
                        var count = reader.ReadCount(payload[0] switch { 4 => 2, 5 => 9, _ => 1 });
                        for (var i = 0; i < count; i++)
                        {
                            budget.AddEntry();
                            var key = reader.ReadString();
                            var fieldValue = payload[0] == 4 ? reader.ReadString() : null;
                            var score = payload[0] == 5 ? BinaryPrimitives.ReadDoubleLittleEndian(reader.Read(8)) : 0;
                            Add(obj, key, fieldValue, score);
                        }
                        break;
                    case 11:
                        obj = new SetObject();
                        ReadIntset(reader.ReadString(), obj, budget);
                        break;
                    case 16:
                    case 17:
                    case 20:
                        obj = CreateObject(payload[0] switch { 16 => 4, 17 => 5, _ => 2 });
                        ReadListpack(reader.ReadString(), obj, budget);
                        break;
                    case 18:
                        obj = new ListObject();
                        var nodes = reader.ReadCount(2);
                        for (var i = 0; i < nodes; i++)
                        {
                            var container = reader.ReadLength();
                            var node = reader.ReadString();
                            if (container == 1)
                            {
                                budget.AddEntry();
                                Add(obj, node, null, 0);
                            }
                            else if (container == 2)
                            {
                                ReadListpack(node, obj, budget);
                            }
                            else
                            {
                                throw new RdbException(FormatError);
                            }
                        }
                        break;
                    default:
                        throw new RdbException(FormatError);
                }
                reader.RequireEnd();
                return true;
            }
            catch (RdbException ex)
            {
                obj?.Dispose();
                obj = null;
                value = default;
                error = ex.Message;
                return false;
            }
        }

        static IGarnetObject CreateObject(int type) => type switch
        {
            1 => new ListObject(),
            2 => new SetObject(),
            4 => new HashObject(),
            5 => new SortedSetObject(),
            _ => throw new RdbException(FormatError)
        };

        static void Add(IGarnetObject obj, byte[] key, byte[] value, double score)
        {
            switch (obj)
            {
                case ListObject list:
                    list.LnkList.AddLast(key);
                    list.UpdateSize(key);
                    return;
                case SetObject set when set.Set.Add(key):
                    set.UpdateSize(key);
                    return;
                case HashObject hash when hash.TryAddRdb(key, value):
                    return;
                case SortedSetObject sortedSet when !double.IsNaN(score) && sortedSet.TryAddRdb(key, score):
                    return;
                default:
                    throw new RdbException(FormatError);
            }
        }

        static void ReadIntset(ReadOnlySpan<byte> bytes, IGarnetObject obj, RdbReadBudget budget)
        {
            var reader = new RdbReader(bytes, budget);
            var width = BinaryPrimitives.ReadUInt32LittleEndian(reader.Read(4));
            var count = BinaryPrimitives.ReadUInt32LittleEndian(reader.Read(4));
            if (width is not (2 or 4 or 8) || count == 0 || count * (ulong)width != (ulong)reader.Remaining)
                throw new RdbException(FormatError);
            long previous = 0;
            for (uint i = 0; i < count; i++)
            {
                budget.AddEntry();
                var number = width switch
                {
                    2 => BinaryPrimitives.ReadInt16LittleEndian(reader.Read(2)),
                    4 => BinaryPrimitives.ReadInt32LittleEndian(reader.Read(4)),
                    _ => BinaryPrimitives.ReadInt64LittleEndian(reader.Read(8))
                };
                if (i > 0 && number <= previous)
                    throw new RdbException(FormatError);
                previous = number;
                Add(obj, reader.Integer(number), null, 0);
            }
        }

        static void ReadListpack(ReadOnlySpan<byte> bytes, IGarnetObject obj, RdbReadBudget budget)
        {
            var reader = new RdbReader(bytes, budget);
            if (BinaryPrimitives.ReadUInt32LittleEndian(reader.Read(4)) != bytes.Length)
                throw new RdbException(FormatError);
            var declared = BinaryPrimitives.ReadUInt16LittleEndian(reader.Read(2));
            var entries = 0;
            byte[] pending = null;
            while (reader.Remaining > 0)
            {
                var start = reader.Position;
                var tag = reader.ReadByte();
                if (tag == 255)
                {
                    reader.RequireEnd();
                    if (entries == 0 || pending is not null || (declared != ushort.MaxValue && declared != entries))
                        throw new RdbException(FormatError);
                    return;
                }

                byte[] item;
                if (tag < 128)
                    item = reader.Integer(tag);
                else if (tag < 192)
                    item = reader.Copy(reader.Read(tag & 63));
                else if (tag < 224)
                {
                    var integer = ((tag & 31) << 8) | reader.ReadByte();
                    item = reader.Integer(integer >= 4096 ? integer - 8192 : integer);
                }
                else if (tag < 240)
                    item = reader.Copy(reader.Read(((tag & 15) << 8) | reader.ReadByte()));
                else if (tag == 240)
                {
                    var length = BinaryPrimitives.ReadUInt32LittleEndian(reader.Read(4));
                    if (length > int.MaxValue)
                        throw new RdbException(FormatError);
                    item = reader.Copy(reader.Read((int)length));
                }
                else
                {
                    long integer;
                    switch (tag)
                    {
                        case 241:
                            integer = BinaryPrimitives.ReadInt16LittleEndian(reader.Read(2));
                            break;
                        case 242:
                            var encoded = reader.Read(3);
                            var int24 = encoded[0] | (encoded[1] << 8) | (encoded[2] << 16);
                            integer = (int24 << 8) >> 8;
                            break;
                        case 243:
                            integer = BinaryPrimitives.ReadInt32LittleEndian(reader.Read(4));
                            break;
                        case 244:
                            integer = BinaryPrimitives.ReadInt64LittleEndian(reader.Read(8));
                            break;
                        default:
                            throw new RdbException(FormatError);
                    }
                    item = reader.Integer(integer);
                }

                var entryLength = reader.Position - start;
                var backBytes = entryLength <= 127 ? 1 : entryLength < 16383 ? 2 :
                    entryLength < 2097151 ? 3 : entryLength < 268435455 ? 4 : 5;
                for (var i = backBytes - 1; i >= 0; i--)
                {
                    var expected = (byte)(((entryLength >> (7 * i)) & 127) | (i == backBytes - 1 ? 0 : 128));
                    if (reader.ReadByte() != expected)
                        throw new RdbException(FormatError);
                }
                entries++;
                if (obj is HashObject or SortedSetObject)
                {
                    if (pending is null)
                    {
                        budget.AddEntry();
                        pending = item;
                    }
                    else
                    {
                        var score = obj is SortedSetObject ? ReadScore(item) : 0;
                        Add(obj, pending, item, score);
                        pending = null;
                    }
                }
                else
                {
                    budget.AddEntry();
                    Add(obj, item, null, 0);
                }
            }
            throw new RdbException(FormatError);
        }

        static double ReadScore(ReadOnlySpan<byte> bytes)
        {
            if (bytes.SequenceEqual("inf"u8) || bytes.SequenceEqual("+inf"u8))
                return double.PositiveInfinity;
            if (bytes.SequenceEqual("-inf"u8))
                return double.NegativeInfinity;
            if (!Utf8Parser.TryParse(bytes, out double score, out var count) || count != bytes.Length || double.IsNaN(score))
                throw new RdbException(FormatError);
            return score;
        }
    }
}