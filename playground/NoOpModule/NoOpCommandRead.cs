// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Garnet.server;
using Tsavorite.core;

namespace NoOpModule
{
    /// <summary>
    /// Represents a raw string no-op read operation
    /// </summary>
    public class NoOpCommandRead : CustomRawStringFunctions
    {
        /// <inheritdoc />
        public override int GetInitialLength(ref RawStringInput input) => throw new NotImplementedException();

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref RawStringInput input) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value,
            ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value,
            ref int valueLength, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue,
            Span<byte> newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> value,
            ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            return true;
        }
    }
}