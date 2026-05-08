// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Garnet.server
{
    /// <summary>
    /// Represents a fixed-size collection of addresses used for append-only file (AOF) operations, supporting efficient
    /// serialization, comparison, and manipulation of address sequences.
    /// </summary>
    public unsafe struct AofAddress
    {
        readonly byte length;
        fixed long addresses[MaxSublogCount];

        /// <summary>
        /// Maximum number of sublogs supported
        /// </summary>
        public const int MaxSublogCount = 4;

        /// <summary>
        /// AofAddress length
        /// </summary>
        public readonly int Length => length;

        /// <summary>
        /// Provides a span of bytes representing the underlying addresses array.
        /// </summary>
        public Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => MemoryMarshal.CreateSpan(ref Unsafe.As<long, byte>(ref addresses[0]), sizeof(long) * length);
        }

        /// <summary>
        /// Indexer
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public long this[int i]
        {
            readonly get
            {
                return addresses[i];
            }
            set
            {
                addresses[i] = value;
            }
        }

        /// <summary>
        /// Determines whether the current instance and the specified <see cref="AofAddress"/> instance represent the
        /// same address sequence.
        /// </summary>
        /// <param name="other">The <see cref="AofAddress"/> instance to compare with the current instance. This parameter cannot be null
        /// and must have the same length as the current instance.</param>
        /// <returns><see langword="true"/> if the specified <see cref="AofAddress"/> is equal to the current instance;
        /// otherwise, <see langword="false"/>.</returns>
        public bool Equals(in AofAddress other)
        {
            Debug.Assert(other.Length == Length);
            for (var i = 0; i < Length; i++)
                if (addresses[i] != other[i]) return false;
            return true;
        }

        /// <summary>
        /// AofAddress constructor
        /// </summary>
        /// <param name="length"></param>
        internal AofAddress(int length)
        {
            Debug.Assert(length <= MaxSublogCount);
            this.length = (byte)length;
        }

        /// <summary>
        /// Convert to byte array
        /// </summary>
        /// <returns></returns>
        public byte[] ToByteArray()
        {
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.ASCII);
            Serialize(writer);
            return ms.ToArray();
        }

        /// <summary>
        /// Convert to AofAddress from byte array
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static AofAddress FromByteArray(byte[] data)
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms, Encoding.ASCII);
            return Deserialize(reader);
        }

        /// <summary>
        /// Create AofAddress from span
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static AofAddress FromSpan(Span<byte> span)
        {
            var length = span.Length >> 3;
            var aofAddress = new AofAddress(length);
            fixed (byte* ptr = span)
            {
                var curr = ptr;
                for (var i = 0; i < length; i++)
                {
                    aofAddress[i] = *(long*)curr;
                    curr += sizeof(long);
                }
            }
            return aofAddress;
        }

        /// <summary>
        /// Comma separate string of valid addresses in this AofAddress
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            _ = sb.Append(addresses[0]);
            for (var i = 1; i < Length; i++)
            {
                _ = sb.Append(',');
                _ = sb.Append(addresses[i]);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Create AofAddress from command separated string of addresses
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static AofAddress FromString(string input)
        {
            var span = input.AsSpan();

            // Count commas to determine array size
            var count = 1;
            for (var i = 0; i < span.Length; i++)
                if (span[i] == ',') count++;

            var aofAddress = new AofAddress(count);
            var idx = 0;
            var value = 0L;
            var negative = false;
            for (var i = 0; i < span.Length; i++)
            {
                var c = span[i];
                if (c == ',')
                {
                    aofAddress[idx++] = value;
                    value = 0;
                }
                else if (c >= '0' && c <= '9')
                {
                    value = value * 10 + (c - '0');
                }
                else if (c == '-')
                {
                    negative = true;
                }
                else
                {
                    throw new FormatException($"Invalid character '{c}' in AofAddress string.");
                }
            }

            // Handle last value
            aofAddress[idx] = value * (negative ? -1 : 1);
            return aofAddress;
        }

        /// <summary>
        /// Serialize contents using provided BinaryWriter
        /// </summary>
        /// <param name="writer"></param>
        public void Serialize(BinaryWriter writer)
        {
            writer.Write(length);
            for (var i = 0; i < Length; i++)
                writer.Write(addresses[i]);
        }

        /// <summary>
        /// Deserialize contents and allocate a new instance using provided BinaryReader
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static AofAddress Deserialize(BinaryReader reader)
        {
            var length = reader.ReadByte();
            var aofAddress = new AofAddress(length);
            for (var i = 0; i < length; i++)
                aofAddress[i] = reader.ReadInt64();
            return aofAddress;
        }

        /// <summary>
        /// Set to value if address equals to comparand
        /// </summary>
        /// <param name="value"></param>
        /// <param name="comparand"></param>
        public void SetValueIf(long value, long comparand)
        {
            for (var i = 0; i < Length; i++)
                if (addresses[i] == comparand)
                    addresses[i] = value;
        }

        /// <summary>
        /// Set to value if address equals to comparand
        /// </summary>
        /// <param name="value"></param>
        /// <param name="comparand"></param>
        public void SetValueIf(in AofAddress value, long comparand)
        {
            for (var i = 0; i < Length; i++)
            {
                if (addresses[i] == comparand)
                    addresses[i] = value[i];
            }
        }

        /// <summary>
        /// Set to value from aofAddress
        /// </summary>
        /// <param name="aofAddress"></param>
        public void SetValue(ref AofAddress aofAddress)
        {
            for (var i = 0; i < Length; i++)
                addresses[i] = aofAddress[i];
        }

        /// <summary>
        /// Set to value
        /// </summary>
        /// <param name="value"></param>
        public void SetValue(long value)
        {
            for (var i = 0; i < Length; i++)
                addresses[i] = value;
        }

        /// <summary>
        /// Allocate AofAddress of provided length and set to value
        /// </summary>
        /// <param name="length"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static AofAddress Create(int length, long value)
        {
            var aofAddress = new AofAddress(length);
            for (var i = 0; i < length; i++)
                aofAddress[i] = value;
            return aofAddress;
        }

        /// <summary>
        /// Allocate AofAddress and assign its contents with the min-pairwise value of the provided inputs
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static AofAddress Min(ref AofAddress a, ref AofAddress b)
        {
            var aofAddress = new AofAddress(a.Length);
            for (var i = 0; i < a.Length; i++)
                aofAddress[i] = Math.Min(a[i], b[i]);
            return aofAddress;
        }

        public void MonotonicUpdate(ref AofAddress update)
        {
            for (var i = 0; i < Length; i++)
                _ = Tsavorite.core.Utility.MonotonicUpdate(ref addresses[i], update[i], out _);
        }

        public void MonotonicUpdate(long update, int physicalSublogIdx)
        {
            _ = Tsavorite.core.Utility.MonotonicUpdate(ref addresses[physicalSublogIdx], update, out _);
        }

        public void MinExchange(in AofAddress address)
        {
            for (var i = 0; i < Length; i++)
                addresses[i] = Math.Min(addresses[i], address[i]);
        }

        public void MaxExchange(long address)
        {
            for (var i = 0; i < Length; i++)
                addresses[i] = Math.Max(addresses[i], address);
        }

        public bool AnyLesser(in AofAddress address)
        {
            for (var i = 0; i < Length; i++)
                if (addresses[i] < address[i]) return true;
            return false;
        }

        public bool AnyGreater(in AofAddress address)
        {
            for (var i = 0; i < Length; i++)
                if (addresses[i] > address[i]) return true;
            return false;
        }

        public bool AnyGreater(long value)
        {
            for (var i = 0; i < Length; i++)
                if (addresses[i] > value) return false;
            return true;
        }

        public AofAddress Diff(in AofAddress other)
        {
            Debug.Assert(other.Length == Length);
            var aofAddress = new AofAddress(other.Length);
            for (var i = 0; i < other.Length; i++)
                aofAddress[i] = this.addresses[i] - other[i];
            return aofAddress;
        }

        public long AggregateDiff(in AofAddress aofAddress)
        {
            var diff = 0L;
            for (var i = 0; i < Length; i++)
                diff += addresses[i] - aofAddress[i];
            return diff;
        }

        public long AggregateDiff(long value)
        {
            var diff = 0L;
            for (var i = 0; i < Length; i++)
                diff += addresses[i] - value;
            return diff;
        }

        public bool EqualsAll(in AofAddress input)
        {
            for (var i = 0; i < Length; i++)
                if (addresses[i] != input[i])
                    return false;
            return true;
        }

        public bool IsOutOfRange(in AofAddress begin, in AofAddress end)
        {
            for (var i = 0; i < Length; i++)
            {
                if (addresses[i] < begin[i] || addresses[i] > end[i])
                    return true;
            }
            return false;
        }

        public long Max()
        {
            var max = 0L;
            for (var i = 0; i < Length; i++)
                max = Math.Max(max, addresses[i]);
            return max;
        }

        public long Min()
        {
            var max = 0L;
            for (var i = 0; i < Length; i++)
                max = Math.Min(max, addresses[i]);
            return max;
        }
    }
}