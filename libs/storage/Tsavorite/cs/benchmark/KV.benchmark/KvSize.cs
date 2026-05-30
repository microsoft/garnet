// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Size string parsing/formatting helpers ("4mb", "16GB", "1024" → bytes).
    /// </summary>
    internal static class KvSize
    {
        /// <summary>
        /// Parse a size string like "4mb", "64g", "256m", "1024" into bytes.
        /// Returns -1 on parse failure. Suffix is case-insensitive.
        /// </summary>
        public static long ParseSize(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return -1;
            var span = s.Trim().ToLowerInvariant();
            long mult = 1;
            int suffixLen = 0;
            if (span.EndsWith("tb")) { mult = 1L << 40; suffixLen = 2; }
            else if (span.EndsWith("gb")) { mult = 1L << 30; suffixLen = 2; }
            else if (span.EndsWith("mb")) { mult = 1L << 20; suffixLen = 2; }
            else if (span.EndsWith("kb")) { mult = 1L << 10; suffixLen = 2; }
            else if (span.EndsWith('t')) { mult = 1L << 40; suffixLen = 1; }
            else if (span.EndsWith('g')) { mult = 1L << 30; suffixLen = 1; }
            else if (span.EndsWith('m')) { mult = 1L << 20; suffixLen = 1; }
            else if (span.EndsWith('k')) { mult = 1L << 10; suffixLen = 1; }
            var numeric = span.Substring(0, span.Length - suffixLen);
            if (!double.TryParse(numeric, NumberStyles.Float, CultureInfo.InvariantCulture, out var raw) || raw < 0)
                return -1;
            return (long)(raw * mult);
        }

        /// <summary>Format bytes as "64MB" / "32GB" / etc.</summary>
        public static string FormatSize(long bytes)
        {
            if (bytes <= 0) return bytes.ToString(CultureInfo.InvariantCulture);
            if ((bytes & ((1L << 40) - 1)) == 0) return (bytes >> 40).ToString(CultureInfo.InvariantCulture) + "TB";
            if ((bytes & ((1L << 30) - 1)) == 0) return (bytes >> 30).ToString(CultureInfo.InvariantCulture) + "GB";
            if ((bytes & ((1L << 20) - 1)) == 0) return (bytes >> 20).ToString(CultureInfo.InvariantCulture) + "MB";
            if ((bytes & ((1L << 10) - 1)) == 0) return (bytes >> 10).ToString(CultureInfo.InvariantCulture) + "KB";
            return bytes.ToString(CultureInfo.InvariantCulture) + "B";
        }

        /// <summary>Round n up to the next power of two. Returns n if it is already a power of two.</summary>
        public static long NextPow2(long n)
        {
            if (n <= 1) return 1;
            long v = n - 1;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v + 1;
        }
    }
}