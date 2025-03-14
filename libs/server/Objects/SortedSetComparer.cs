// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    internal sealed class SortedSetComparer : IComparer<(double Score, byte[] Element)>
#if NET9_0_OR_GREATER 
        , Garnet.common.Collections.IAlternateComparer<SortedSetComparer.AlternateEntry, (double Score, byte[] Element)>
#endif
    {
        /// <summary>
        /// Represents a alternate stack-only sorted set entry used to perform lookups.
        /// </summary>
        internal readonly ref struct AlternateEntry(double score, ReadOnlySpan<byte> element)
        {
            public double Score { get; } = score;
            public ReadOnlySpan<byte> Element { get; } = element;
        }

        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly SortedSetComparer Instance = new();

        /// <inheritdoc/>
        public int Compare((double Score, byte[] Element) x, (double Score, byte[] Element) y)
        {
            var ret = x.Score.CompareTo(y.Score);
            if (ret == 0)
                return new ReadOnlySpan<byte>(x.Element).SequenceCompareTo(y.Element);
            return ret;
        }

        public (double Score, byte[] Element) Create(AlternateEntry alternate) => (alternate.Score, alternate.Element.ToArray());

        public int Compare(AlternateEntry alternate, (double Score, byte[] Element) y)
        {
            var ret = alternate.Score.CompareTo(y.Score);
            if (ret == 0)
                return alternate.Element.SequenceCompareTo(y.Element);
            return ret;
        }
    }
}