// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
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
        public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer)
            => false;
        /// <inheritdoc />
        public override int GetInitialLength(ref StringInput input)
            => throw new InvalidOperationException();
        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            var expectedVal = GetFirstArg(ref input);
            if (value.SequenceEqual(expectedVal))
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }
            // +OK is sent as response, by default
            return true;
        }

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer)
        {
            var expectedVal = GetFirstArg(ref input);
            return oldValue.SequenceEqual(expectedVal);
        }

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input)
            => value.Length;

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            rmwInfo.Action = RMWAction.ExpireAndStop;
            Debug.Assert(oldValue.Length == newValue.Length);
            oldValue.CopyTo(newValue);

            return false;
        }
    }
}