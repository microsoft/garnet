// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// <see cref="SpanByteAndMemory"/> equality comparer.
    /// </summary>
    public sealed class SpanByteAndMemoryComparer : IEqualityComparer<SpanByteAndMemory>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly SpanByteAndMemoryComparer Instance = new();

        private SpanByteAndMemoryComparer() { }

        /// <inheritdoc />
        public bool Equals(SpanByteAndMemory left, SpanByteAndMemory right)
        => left.AsReadOnlySpan().SequenceEqual(right.AsReadOnlySpan());

        /// <inheritdoc />
        public unsafe int GetHashCode(SpanByteAndMemory key)
        {
            var hash = new HashCode();
            hash.AddBytes(key.AsReadOnlySpan());

            var ret = hash.ToHashCode();

            // DEBUG
            var txt = System.Text.Encoding.ASCII.GetString(key.AsReadOnlySpan());
            // END DEBUG

            return ret;
        }
    }
}