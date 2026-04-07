// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;

namespace Garnet.server
{
    /// <summary>
    /// Ultra-lightweight top-level JSON field extractor.
    /// Returns fields directly as <see cref="ExprToken"/> values.
    ///
    /// All string values are zero-copy byte-range references (Utf8Start, Utf8Length)
    /// into the source JSON span — no string allocations.
    /// </summary>
    internal static class AttributeExtractor
    {
        /// <summary>
        /// Extract multiple top-level fields from a JSON object in a single pass.
        /// Selectors are byte-ranges in <paramref name="filterBytes"/>.
        /// Extracted values are byte-ranges in <paramref name="json"/>.
        /// Returns the number of fields successfully extracted.
        /// </summary>
        public static int ExtractFields(
            ReadOnlySpan<byte> json,
            ReadOnlySpan<byte> filterBytes,
            ReadOnlySpan<(int Start, int Length)> selectorRanges,
            Span<ExprToken> results,
            ref ExprProgram program)
        {
            for (var i = 0; i < selectorRanges.Length; i++)
                results[i] = default;

            var s = TrimWhiteSpace(json);
            if (s.IsEmpty || s[0] != (byte)'{') return 0;
            s = s[1..];

            var found = 0;
            var needed = selectorRanges.Length;

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;
                if (s[0] == (byte)'}') return found;

                if (s[0] != (byte)'"') return found;

                var afterOpenQuote = s[1..];
                if (!SkipString(ref s)) return found;
                var keyContent = afterOpenQuote[..(afterOpenQuote.Length - s.Length - 1)];

                var matchIndex = -1;
                for (var i = 0; i < selectorRanges.Length; i++)
                {
                    if (results[i].IsNone &&
                        keyContent.SequenceEqual(filterBytes.Slice(selectorRanges[i].Start, selectorRanges[i].Length)))
                    {
                        matchIndex = i;
                        break;
                    }
                }

                s = TrimWhiteSpace(s);
                if (s.IsEmpty || s[0] != (byte)':') return found;
                s = s[1..];

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;

                if (matchIndex >= 0)
                {
                    results[matchIndex] = ParseValueToken(json, ref s, ref program);
                    found++;
                    if (found == needed) return found;
                }
                else
                {
                    if (!SkipValue(ref s)) return found;
                }

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;
                if (s[0] == (byte)',') { s = s[1..]; continue; }
                if (s[0] == (byte)'}') return found;
                return found;
            }
        }

        /// <summary>
        /// Extract a single top-level field from a JSON object.
        /// <paramref name="fieldNameUtf8"/> is the raw UTF-8 bytes of the field name.
        /// Returns default (IsNone) if not found.
        /// </summary>
        public static ExprToken ExtractField(ReadOnlySpan<byte> json, ReadOnlySpan<byte> fieldNameUtf8)
        {
            var s = TrimWhiteSpace(json);
            if (s.IsEmpty || s[0] != (byte)'{') return default;
            s = s[1..];

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (s[0] == (byte)'}') return default;

                if (s[0] != (byte)'"') return default;

                var afterOpenQuote = s[1..];
                if (!SkipString(ref s)) return default;
                var keyContent = afterOpenQuote[..(afterOpenQuote.Length - s.Length - 1)];

                var match = keyContent.SequenceEqual(fieldNameUtf8);

                s = TrimWhiteSpace(s);
                if (s.IsEmpty || s[0] != (byte)':') return default;
                s = s[1..];

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;

                if (match)
                    return ParseValueToken(json, ref s);

                if (!SkipValue(ref s)) return default;

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (s[0] == (byte)',') { s = s[1..]; continue; }
                if (s[0] == (byte)'}') return default;
                return default;
            }
        }

        // ======================== Value parsing ========================

        /// <summary>Max array elements before rejecting.</summary>
        private const int MaxArrayElements = 64;

        private static ExprToken ParseValueToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s, ref ExprProgram program)
        {
            s = TrimWhiteSpace(s);
            if (s.IsEmpty) return default;

            var c = s[0];
            if (c == (byte)'"') return ParseStringToken(json, ref s);
            if (c == (byte)'[') return ParseArrayToken(json, ref s, ref program);
            if (c == (byte)'{') return default;
            if (c == (byte)'t') return ParseLiteralToken(ref s, "true"u8, ExprTokenType.Num, 1);
            if (c == (byte)'f') return ParseLiteralToken(ref s, "false"u8, ExprTokenType.Num, 0);
            if (c == (byte)'n') return ParseLiteralToken(ref s, "null"u8, ExprTokenType.Null, 0);
            if (IsDigit(c) || c == (byte)'-' || c == (byte)'+')
                return ParseNumberToken(ref s);

            return default;
        }

        private static ExprToken ParseValueToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s)
        {
            s = TrimWhiteSpace(s);
            if (s.IsEmpty) return default;

            var c = s[0];
            if (c == (byte)'"') return ParseStringToken(json, ref s);
            if (c == (byte)'[') return ParseArrayTokenNoPool(json, ref s);
            if (c == (byte)'{') return default; // Nested objects not supported
            if (c == (byte)'t') return ParseLiteralToken(ref s, "true"u8, ExprTokenType.Num, 1);
            if (c == (byte)'f') return ParseLiteralToken(ref s, "false"u8, ExprTokenType.Num, 0);
            if (c == (byte)'n') return ParseLiteralToken(ref s, "null"u8, ExprTokenType.Null, 0);
            if (IsDigit(c) || c == (byte)'-' || c == (byte)'+')
                return ParseNumberToken(ref s);

            return default;
        }

        private static ExprToken ParseStringToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s)
        {
            if (s.IsEmpty || s[0] != (byte)'"') return default;
            s = s[1..]; // Skip opening quote
            var body = s;
            var hasEscape = false;

            while (!s.IsEmpty)
            {
                if (s[0] == (byte)'\\')
                {
                    hasEscape = true;
                    s = s[2..];
                    continue;
                }
                if (s[0] == (byte)'"')
                {
                    var contentLen = body.Length - s.Length;
                    var absoluteStart = json.Length - body.Length;
                    s = s[1..]; // Skip closing quote
                    return ExprToken.NewStr(absoluteStart, contentLen, hasEscape);
                }
                s = s[1..];
            }
            return default;
        }

        private static ExprToken ParseNumberToken(ref ReadOnlySpan<byte> s)
        {
            var original = s;
            while (!s.IsEmpty && IsNumberChar(s[0])) s = s[1..];

            var numSpan = original[..(original.Length - s.Length)];
            if (numSpan.IsEmpty) return default;

            if (!Utf8Parser.TryParse(numSpan, out double value, out var bytesConsumed) || bytesConsumed != numSpan.Length)
            {
                s = original;
                return default;
            }
            return ExprToken.NewNum(value);
        }

        private static ExprToken ParseLiteralToken(ref ReadOnlySpan<byte> s,
            ReadOnlySpan<byte> literal, ExprTokenType type, double num)
        {
            if (s.Length < literal.Length) return default;
            if (!s[..literal.Length].SequenceEqual(literal)) return default;

            if (s.Length > literal.Length)
            {
                var next = (char)s[literal.Length];
                if (!char.IsWhiteSpace(next) && next != ',' && next != ']' && next != '}')
                    return default;
            }

            s = s[literal.Length..];
            return type == ExprTokenType.Null ? ExprToken.NewNull() : ExprToken.NewNum(num);
        }

        /// <summary>
        /// Parse a JSON array and return a Tuple token. Elements are parsed and stored
        /// in the caller's <paramref name="program"/> runtime pool so the runner can
        /// iterate them during IN evaluation.
        /// </summary>
        internal static ExprToken ParseArrayToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s, ref ExprProgram program)
        {
            if (s.IsEmpty || s[0] != (byte)'[') return default;
            s = s[1..];
            s = TrimWhiteSpace(s);

            // Empty array
            if (!s.IsEmpty && s[0] == (byte)']') { s = s[1..]; return ExprToken.NewTuple(0, 0); }

            Span<ExprToken> localBuf = stackalloc ExprToken[MaxArrayElements];
            var count = 0;

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (count >= MaxArrayElements) { SkipBracketed(ref s, (byte)'[', (byte)']'); return ExprToken.NewNull(); }

                var elem = ParseValueToken(json, ref s);
                if (elem.IsNone) return default;
                localBuf[count++] = elem;

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (s[0] == (byte)']') { s = s[1..]; break; }
                if (s[0] != (byte)',') return default;
                s = s[1..];
            }

            if (program.RuntimePool.Length > 0)
            {
                // Copy directly into the runtime pool — zero heap allocation.
                var start = program.RuntimePoolLength;
                if (start + count > program.RuntimePool.Length)
                    return ExprToken.NewNull(); // Pool exhausted — skip array gracefully
                for (var i = 0; i < count; i++)
                    program.RuntimePool[start + i] = localBuf[i];
                program.RuntimePoolLength += count;
                return ExprToken.NewRuntimeTuple(start, count);
            }

            // No program available — return null (shouldn't happen in normal flow)
            return ExprToken.NewNull();
        }

        private static ExprToken ParseArrayTokenNoPool(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s)
        {
            // Standalone extraction without a program — just skip the array
            if (!SkipValue(ref s)) return default;
            return ExprToken.NewNull();
        }

        // ======================== Fast skipping (non-allocating) ========================

        private static bool SkipValue(ref ReadOnlySpan<byte> s)
        {
            s = TrimWhiteSpace(s);
            if (s.IsEmpty) return false;

            return (char)s[0] switch
            {
                '"' => SkipString(ref s),
                '{' => SkipBracketed(ref s, (byte)'{', (byte)'}'),
                '[' => SkipBracketed(ref s, (byte)'[', (byte)']'),
                't' => SkipLiteral(ref s, "true"u8),
                'f' => SkipLiteral(ref s, "false"u8),
                'n' => SkipLiteral(ref s, "null"u8),
                _ => SkipNumber(ref s),
            };
        }

        private static bool SkipString(ref ReadOnlySpan<byte> s)
        {
            if (s.IsEmpty || s[0] != (byte)'"') return false;
            s = s[1..];
            while (!s.IsEmpty)
            {
                if (s[0] == (byte)'\\') { s = s[2..]; continue; }
                if (s[0] == (byte)'"') { s = s[1..]; return true; }
                s = s[1..];
            }
            return false;
        }

        private static bool SkipBracketed(ref ReadOnlySpan<byte> s, byte opener, byte closer)
        {
            var depth = 1;
            s = s[1..];
            while (!s.IsEmpty && depth > 0)
            {
                if (s[0] == (byte)'"')
                {
                    if (!SkipString(ref s)) return false;
                    continue;
                }
                if (s[0] == opener) depth++;
                else if (s[0] == closer) depth--;
                s = s[1..];
            }
            return depth == 0;
        }

        private static bool SkipLiteral(ref ReadOnlySpan<byte> s, ReadOnlySpan<byte> literal)
        {
            if (s.Length < literal.Length) return false;
            if (!s[..literal.Length].SequenceEqual(literal)) return false;
            s = s[literal.Length..];
            return true;
        }

        private static bool SkipNumber(ref ReadOnlySpan<byte> s)
        {
            var original = s;
            while (!s.IsEmpty && IsNumberChar(s[0])) s = s[1..];
            return s.Length < original.Length;
        }

        // ======================== Shared byte-level helpers ========================

        internal static bool IsDigit(byte b) => b >= (byte)'0' && b <= (byte)'9';

        internal static bool IsLetter(byte b) => (b >= (byte)'a' && b <= (byte)'z') || (b >= (byte)'A' && b <= (byte)'Z');

        internal static bool IsLetterOrDigit(byte b) => IsLetter(b) || IsDigit(b);

        internal static bool IsWhiteSpace(byte b) => b == (byte)' ' || b == (byte)'\t' || b == (byte)'\n' || b == (byte)'\r';

        internal static ReadOnlySpan<byte> TrimWhiteSpace(ReadOnlySpan<byte> s)
        {
            var i = 0;
            while (i < s.Length && IsWhiteSpace(s[i])) i++;
            return s[i..];
        }

        private static bool IsNumberChar(byte b) =>
            IsDigit(b) || b == (byte)'-' || b == (byte)'+' ||
            b == (byte)'.' || b == (byte)'e' || b == (byte)'E';
    }
}