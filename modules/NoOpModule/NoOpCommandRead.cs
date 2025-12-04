// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
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
        public override int GetInitialLength(ref StringInput input) => throw new NotImplementedException();

        /// <inheritdoc />
        public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value,
            ref RespMemoryWriter writer, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value,
            ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue,
            Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value,
            ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            return true;
        }
    }
}