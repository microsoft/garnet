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
    /// Functions to implement custom command DELIFMATCH - del if value matches exactly (used for unlocking)
    /// 
    /// Format: DELIFMATCH key value
    /// 
    /// Description: Delete key only if the given value matches the 
    /// existing value for that key. If it does not match (or there is no existing value), 
    /// then do nothing.
    /// </summary>
    sealed class DeleteIfMatchCustomCommand : CustomRawStringFunctions
    {
        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output)
            => false;
        /// <inheritdoc />
        public override int GetInitialLength(ReadOnlySpan<byte> input)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> value, ref int valueLength, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            var expectedVal = GetFirstArg(input);
            if (value.SequenceEqual(expectedVal))
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }
            // +OK is sent as response, by default
            return true;
        }

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> oldValue, ref (IMemoryOwner<byte>, int) output)
        {
            var expectedVal = GetFirstArg(input);
            return oldValue.SequenceEqual(expectedVal);
        }

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ReadOnlySpan<byte> input)
            => value.Length;

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            rmwInfo.Action = RMWAction.ExpireAndStop;
            Debug.Assert(oldValue.Length == newValue.Length);
            oldValue.CopyTo(newValue);

            return false;
        }
    }
}