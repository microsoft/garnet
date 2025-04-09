// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Garnet.server;
using Tsavorite.core;

namespace NoOpModule
{
    /// <summary>
    /// Represents a raw string no-op RMW operation
    /// </summary>
    public class NoOpCommandRMW : CustomRawStringFunctions
    {
        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> value,
            ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ref (IMemoryOwner<byte>, int) output)
            => false;

        /// <inheritdoc />
        public override int GetInitialLength(ref RawStringInput input)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value,
            ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value,
            ref int valueLength, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc />
        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref RawStringInput input,
            ReadOnlySpan<byte> oldValue, ref (IMemoryOwner<byte>, int) output) => false;

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref RawStringInput input)
            => 0;

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue,
            Span<byte> newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => true;
    }
}