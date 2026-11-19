// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Glob utils
    /// </summary>
    public static class GlobUtils
    {
        /// <summary>
        /// Glob-style ASCII pattern matching using constant stack space.
        /// </summary>
        /// <remarks>
        /// Only the most recent wildcard needs a retry position: every other token consumes one byte.
        /// Matching takes at most O(patternLen * stringLen) work, without recursive backtracking.
        /// </remarks>
        /// <returns>Whether match was found</returns>
        public static unsafe bool Match(byte* pattern, int patternLen, byte* key, int stringLen, bool ignoreCase = false)
        {
            byte* retryPattern = null;
            var retryPatternLen = 0;
            byte* retryKey = null;
            var retryStringLen = 0;

            while (patternLen > 0 && stringLen > 0)
            {
                switch (pattern[0])
                {
                    case (byte)'*':
                        // Adjacent stars are equivalent to a single wildcard.
                        do
                        {
                            pattern++;
                            patternLen--;
                        }
                        while (patternLen > 0 && pattern[0] == '*');

                        if (patternLen == 0)
                            return true;

                        // Try an empty match first; only the latest wildcard needs retry state.
                        retryPattern = pattern;
                        retryPatternLen = patternLen;
                        retryKey = key;
                        retryStringLen = stringLen;
                        continue;

                    default:
                        if (MatchByte(pattern, patternLen, key[0], ignoreCase, out var consumed))
                        {
                            pattern += consumed;
                            patternLen -= consumed;
                            key++;
                            stringLen--;
                            // An exhausted pattern with remaining key bytes must retry the wildcard.
                            if (patternLen > 0 || stringLen == 0)
                                continue;
                        }
                        break;
                }

                if (retryPattern == null)
                    return false;

                // Extend the most recent wildcard by one byte, then retry its suffix.
                key = ++retryKey;
                stringLen = --retryStringLen;
                pattern = retryPattern;
                patternLen = retryPatternLen;
            }

            // Only stars can match the empty remainder of the key.
            while (patternLen > 0 && pattern[0] == '*')
            {
                pattern++;
                patternLen--;
            }
            return patternLen == 0 && stringLen == 0;
        }

        /// <summary>
        /// Matches one non-star glob token against a single byte.
        /// </summary>
        /// <param name="pattern">Pointer to the first byte of the token.</param>
        /// <param name="patternLen">Number of available pattern bytes; must be positive.</param>
        /// <param name="value">Input byte to match.</param>
        /// <param name="ignoreCase">Whether to use ASCII case-insensitive comparisons.</param>
        /// <param name="consumed">Number of pattern bytes occupied by the token.</param>
        /// <returns>Whether the token matches the input byte.</returns>
        private static unsafe bool MatchByte(byte* pattern, int patternLen, byte value, bool ignoreCase, out int consumed)
        {
            consumed = 1;
            switch (pattern[0])
            {
                case (byte)'?':
                    return true;

                case (byte)'[':
                    {
                        var remaining = patternLen - 1;
                        var current = pattern + 1;
                        var not = remaining > 0 && current[0] == '^';
                        if (not)
                        {
                            current++;
                            remaining--;
                        }

                        var match = false;
                        while (remaining > 0 && current[0] != ']')
                        {
                            if (current[0] == '\\' && remaining >= 2)
                            {
                                current++;
                                remaining--;
                                if (current[0] == value)
                                    match = true;
                            }
                            else if (remaining >= 3 && current[1] == '-')
                            {
                                var start = current[0];
                                var end = current[2];
                                var c = value;
                                if (start > end)
                                    (end, start) = (start, end);

                                if (ignoreCase)
                                {
                                    start = AsciiUtils.ToLower(start);
                                    end = AsciiUtils.ToLower(end);
                                    c = AsciiUtils.ToLower(c);
                                }
                                current += 2;
                                remaining -= 2;
                                if (c >= start && c <= end)
                                    match = true;
                            }
                            else if (ignoreCase
                                ? AsciiUtils.ToLower(current[0]) == AsciiUtils.ToLower(value)
                                : current[0] == value)
                            {
                                match = true;
                            }
                            current++;
                            remaining--;
                        }

                        // Include the closing bracket when present; otherwise consume the remaining pattern.
                        consumed = patternLen - remaining + (remaining > 0 ? 1 : 0);
                        return not ? !match : match;
                    }

                case (byte)'\\':
                    // A trailing backslash matches itself.
                    if (patternLen >= 2)
                    {
                        pattern++;
                        consumed++;
                    }
                    break;
            }

            return ignoreCase
                ? AsciiUtils.ToLower(pattern[0]) == AsciiUtils.ToLower(value)
                : pattern[0] == value;
        }
    }
}