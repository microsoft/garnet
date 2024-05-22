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
        /// Glob-style ASCII pattern matching
        /// </summary>
        /// <returns>Whether match was found</returns>
        public static unsafe bool Match(byte* pattern, int patternLen, byte* key, int stringLen, bool ignoreCase = false)
        {
            while (patternLen > 0 && stringLen > 0)
            {
                switch (pattern[0])
                {
                    case (byte)'*':
                        while (patternLen > 0 && pattern[1] == '*')
                        {
                            pattern++;
                            patternLen--;
                        }
                        if (patternLen == 1)
                            return true; /* match */
                        while (stringLen > 0)
                        {
                            if (Match(pattern + 1, patternLen - 1, key, stringLen, ignoreCase))
                                return true; /* match */
                            key++;
                            stringLen--;
                        }
                        return false; /* no match */

                    case (byte)'?':
                        key++;
                        stringLen--;
                        break;

                    case (byte)'[':
                        {
                            pattern++;
                            patternLen--;
                            var not = pattern[0] == '^';
                            if (not)
                            {
                                pattern++;
                                patternLen--;
                            }
                            var match = false;
                            while (true)
                            {
                                if (pattern[0] == '\\' && patternLen >= 2)
                                {
                                    pattern++;
                                    patternLen--;
                                    if (pattern[0] == key[0])
                                        match = true;
                                }
                                else if (pattern[0] == ']')
                                {
                                    break;
                                }
                                else if (patternLen == 0)
                                {
                                    pattern--;
                                    patternLen++;
                                    break;
                                }
                                else if (patternLen >= 3 && pattern[1] == '-')
                                {
                                    byte start = pattern[0];
                                    byte end = pattern[2];
                                    byte c = key[0];
                                    if (start > end)
                                    {
                                        (end, start) = (start, end);
                                    }

                                    if (ignoreCase)
                                    {
                                        start = AsciiUtils.ToLower(start);
                                        end = AsciiUtils.ToLower(end);
                                        c = AsciiUtils.ToLower(c);
                                    }
                                    pattern += 2;
                                    patternLen -= 2;
                                    if (c >= start && c <= end)
                                        match = true;
                                }
                                else
                                {
                                    if (!ignoreCase)
                                    {
                                        if (pattern[0] == key[0])
                                            match = true;
                                    }
                                    else
                                    {
                                        if (AsciiUtils.ToLower(pattern[0]) == AsciiUtils.ToLower(key[0]))
                                            match = true;
                                    }
                                }
                                pattern++;
                                patternLen--;
                            }

                            if (not)
                                match = !match;
                            if (!match)
                                return false; /* no match */
                            key++;
                            stringLen--;
                            break;
                        }

                    case (byte)'\\':
                        if (patternLen >= 2)
                        {
                            pattern++;
                            patternLen--;
                        }
                        goto default;

                    /* fall through */
                    default:
                        if (!ignoreCase)
                        {
                            if (pattern[0] != key[0])
                                return false; /* no match */
                        }
                        else
                        {
                            if (AsciiUtils.ToLower(pattern[0]) != AsciiUtils.ToLower(key[0]))
                                return false; /* no match */
                        }
                        key++;
                        stringLen--;
                        break;
                }
                pattern++;
                patternLen--;
                if (stringLen == 0)
                {
                    while (*pattern == '*')
                    {
                        pattern++;
                        patternLen--;
                    }
                    break;
                }
            }
            if (patternLen == 0 && stringLen == 0)
                return true;
            return false;
        }
    }
}