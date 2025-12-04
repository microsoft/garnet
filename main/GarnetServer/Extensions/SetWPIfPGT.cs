// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
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
        public override int GetInitialLength(ref StringInput input)
        {
            var newVal = GetFirstArg(ref input);
            return newVal.Length + 8;
        }

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input)
        {
            var newVal = GetFirstArg(ref input);
            return newVal.Length + 8;
        }

        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer)
        {
            int offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);
            if (prefix.Length != 8)
            {
                writer.WriteError(PrefixError);
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer)
        {
            int offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);
            if (prefix.Length != 8)
            {
                writer.WriteError(PrefixError);
                return false;
            }
            return BitConverter.ToUInt64(prefix) > BitConverter.ToUInt64(oldValue.Slice(0, 8));
        }

        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);

            prefix.CopyTo(value);
            newVal.CopyTo(value.Slice(8));

            return true;
        }

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);

            if (prefix.Length != 8)
            {
                writer.WriteError(PrefixError);
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
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            int offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);

            prefix.CopyTo(newValue);
            newVal.CopyTo(newValue.Slice(8));

            return true;
        }

        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
            => throw new InvalidOperationException();
    }
}