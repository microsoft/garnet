// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Options for SORT and SORT_RO.
    /// </summary>
    public sealed class SortOptions
    {
        /// <summary>
        /// The BY pattern, or null if BY was not given.
        /// </summary>
        public byte[] ByPattern { get; set; }

        /// <summary>
        /// Sort lexicographically instead of numerically (ALPHA).
        /// </summary>
        public bool Alpha { get; set; }

        /// <summary>
        /// Sort in descending order (DESC).
        /// </summary>
        public bool Descending { get; set; }

        /// <summary>
        /// LIMIT offset. Negative values are treated as 0.
        /// </summary>
        public long LimitOffset { get; set; }

        /// <summary>
        /// LIMIT count. Negative means no limit.
        /// </summary>
        public long LimitCount { get; set; } = -1;

        /// <summary>
        /// The GET patterns, in the order given. Each sorted element produces one reply entry per pattern.
        /// </summary>
        public List<byte[]> GetPatterns { get; } = [];

        /// <summary>
        /// True when BY was given a pattern with no '*' (for example "BY nosort"), which skips sorting.
        /// </summary>
        public bool DontSort => ByPattern != null && Array.IndexOf(ByPattern, (byte)'*') < 0;

        /// <summary>
        /// True when sorting uses values read through the BY pattern rather than the elements themselves.
        /// </summary>
        public bool SortsByPattern => ByPattern != null && !DontSort;

        /// <summary>
        /// True when the BY or GET patterns read keys other than the one being sorted.
        /// </summary>
        public bool ReadsOtherKeys
        {
            get
            {
                if (SortsByPattern)
                    return true;

                foreach (var pattern in GetPatterns)
                {
                    if (!IsElementPattern(pattern) && Array.IndexOf(pattern, (byte)'*') >= 0)
                        return true;
                }

                return false;
            }
        }

        /// <summary>
        /// True for the "#" pattern, which refers to the element itself.
        /// </summary>
        public static bool IsElementPattern(ReadOnlySpan<byte> pattern) => pattern.Length == 1 && pattern[0] == (byte)'#';

        /// <summary>
        /// Builds the key a BY/GET pattern refers to for <paramref name="element"/>, following Redis: the first '*' is
        /// replaced by the element, and a "->field" suffix after it selects a hash field.
        /// </summary>
        /// <param name="pattern">The pattern.</param>
        /// <param name="element">The element being substituted.</param>
        /// <param name="key">The resulting key.</param>
        /// <param name="field">The hash field, or null if the pattern refers to a string key.</param>
        /// <returns>False if the pattern does not refer to another key ("#" or no '*').</returns>
        public static bool TryGetPatternKey(byte[] pattern, byte[] element, out byte[] key, out byte[] field)
        {
            key = null;
            field = null;

            if (IsElementPattern(pattern))
                return false;

            var star = Array.IndexOf(pattern, (byte)'*');
            if (star < 0)
                return false;

            var span = pattern.AsSpan();
            var postfixEnd = span.Length;
            var arrow = span[(star + 1)..].IndexOf("->"u8);
            if (arrow >= 0)
            {
                arrow += star + 1;
                // "->" at the very end does not name a field, so it stays part of the key
                if (arrow + 2 < span.Length)
                {
                    field = span[(arrow + 2)..].ToArray();
                    postfixEnd = arrow;
                }
            }

            var prefix = span[..star];
            var postfix = span[(star + 1)..postfixEnd];

            key = new byte[prefix.Length + element.Length + postfix.Length];
            prefix.CopyTo(key);
            element.CopyTo(key, prefix.Length);
            postfix.CopyTo(key.AsSpan(prefix.Length + element.Length));
            return true;
        }

        /// <summary>
        /// Hash slot every key formed by <paramref name="pattern"/> is guaranteed to map to, or -1 if the keys can map
        /// to different slots. Matches Redis's patternHashSlot: the pattern needs a non-empty hash tag that appears
        /// before any glob character.
        /// </summary>
        public static int PatternHashSlot(ReadOnlySpan<byte> pattern)
        {
            var tagStart = -1;
            for (var i = 0; i < pattern.Length; i++)
            {
                var c = pattern[i];
                if (c is (byte)'*' or (byte)'?' or (byte)'[')
                    return -1;

                if (c == (byte)'\\')
                {
                    i++;
                }
                else if (tagStart == -1 && c == (byte)'{')
                {
                    tagStart = i;
                }
                else if (tagStart >= 0 && c == (byte)'}')
                {
                    if (i == tagStart + 1)
                        return -1;

                    return Garnet.common.HashSlotUtils.HashSlot(pattern[(tagStart + 1)..i]);
                }
            }

            return -1;
        }
    }
}