// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Byte array equality comparer
    /// </summary>
    public sealed class ByteArrayWrapperComparer : IEqualityComparer<ByteArrayWrapper>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly ByteArrayWrapperComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ByteArrayWrapper left, ByteArrayWrapper right)
            => left.ReadOnlySpan.SequenceEqual(right.ReadOnlySpan);

        private ByteArrayWrapperComparer() { }

        /// <inheritdoc />
        public int GetHashCode(ByteArrayWrapper key)
            => key.GetHashCode();
    }
}