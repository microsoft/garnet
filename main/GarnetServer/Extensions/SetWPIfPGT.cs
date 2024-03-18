// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom command SETPIFPGT - set with prefix, if existing prefix greater than specified (8 byte) long
    /// 
    /// Format: SETWPIFPGT key value 8-byte-prefix
    /// 
    /// Description: Update key to given "prefix + value" only if the given prefix (interpreted as 8 byte long) is greater than the 
    /// existing value's 8-byte prefix, or there is no existing value. Otherwise, do nothing.
    /// </summary>
    sealed class SetWPIFPGTCustomCommand : CustomRawStringFunctions
    {
        public const string PrefixError = "Invalid prefix length, should be 8 bytes";

        /// <inheritdoc />
        public override int GetInitialLength(ReadOnlySpan<byte> input)
        {
            var newVal = GetFirstArg(input);
            return newVal.Length + 8;
        }

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ReadOnlySpan<byte> input)
        {
            var newVal = GetFirstArg(input);
            return newVal.Length + 8;
        }

        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output)
        {
            int offset = 0;
            var newVal = GetNextArg(input, ref offset);
            var prefix = GetNextArg(input, ref offset);
            if (prefix.Length != 8)
            {
                WriteError(ref output, PrefixError);
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> oldValue, ref (IMemoryOwner<byte>, int) output)
        {
            int offset = 0;
            var newVal = GetNextArg(input, ref offset);
            var prefix = GetNextArg(input, ref offset);
            if (prefix.Length != 8)
            {
                WriteError(ref output, PrefixError);
                return false;
            }
            return BitConverter.ToUInt64(prefix) > BitConverter.ToUInt64(oldValue.Slice(0, 8));
        }

        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(input, ref offset);
            var prefix = GetNextArg(input, ref offset);

            prefix.CopyTo(value);
            newVal.CopyTo(value.Slice(8));

            return true;
        }

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> value, ref int valueLength, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(input, ref offset);
            var prefix = GetNextArg(input, ref offset);

            if (prefix.Length != 8)
            {
                WriteError(ref output, PrefixError);
                return true;
            }
            if (BitConverter.ToUInt64(prefix) > BitConverter.ToUInt64(value.Slice(0, 8)))
            {
                if (8 + newVal.Length > value.Length)
                    return false;
                if (prefix.Length < 8)
                {
                    value.Slice(0, 8).Fill(0);
                    prefix.CopyTo(value.Slice(8 - prefix.Length));
                }
                else
                    prefix.Slice(0, 8).CopyTo(value);
                newVal.CopyTo(value.Slice(8));
                valueLength = prefix.Length + newVal.Length;
            }

            return true;
        }

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(input, ref offset);
            var prefix = GetNextArg(input, ref offset);

            prefix.CopyTo(newValue);
            newVal.CopyTo(newValue.Slice(8));

            return true;
        }

        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
            => throw new InvalidOperationException();
    }
}