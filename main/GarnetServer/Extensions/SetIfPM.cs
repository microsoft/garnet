// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom command SETIFPM - set if prefix match
    /// 
    /// Format: SETIFPM key value prefix
    /// 
    /// Description: Update key to given value only if the given prefix matches the 
    /// existing value's prefix. If it does not match (or there is no existing value), 
    /// then do nothing.
    /// </summary>
    sealed class SetIfPMCustomCommand : CustomRawStringFunctions
    {
        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ref (IMemoryOwner<byte>, int) output)
            => false;
        /// <inheritdoc />
        public override int GetInitialLength(ref RawStringInput input)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref int valueLength, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            var offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);
            if (prefix.SequenceEqual(newVal.Slice(0, prefix.Length)))
            {
                if (newVal.Length > value.Length) return false;
                newVal.CopyTo(value);
                valueLength = newVal.Length;
            }
            // +OK is sent as response, by default
            return true;
        }

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, ref (IMemoryOwner<byte>, int) output)
        {
            var offset = 0;
            var newVal = GetNextArg(ref input, ref offset);
            var prefix = GetNextArg(ref input, ref offset);
            return prefix.SequenceEqual(newVal.Slice(0, prefix.Length));
        }

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref RawStringInput input)
            => GetFirstArg(ref input).Length;

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            var newVal = GetFirstArg(ref input);
            Debug.Assert(newVal.Length == newValue.Length);
            newVal.CopyTo(newValue);

            // +OK is sent as response, by default
            return true;
        }
    }
}