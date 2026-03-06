// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Text;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Ultra-lightweight top-level JSON field extractor.
    /// Returns fields directly as <see cref="ExprToken"/> values.
    ///
    /// 1. Zero heap allocations while seeking the requested key.
    /// 2. A single parse (and allocation) when the key matches.
    /// 3. Supports: strings (with \n \r \t \\ \" escapes), numbers, booleans, null,
    ///    and flat arrays of these primitives. Nested objects return null.
    /// 4. Operates on raw UTF-8 bytes (ReadOnlySpan&lt;byte&gt;) — no JsonDocument DOM.
    /// </summary>
    internal static class AttributeExtractor
    {
        /// <summary>
        /// Extract multiple top-level fields from a JSON object in a single pass.
        /// <paramref name="fieldNames"/> lists the fields to extract.
        /// <paramref name="results"/> must be at least <paramref name="fieldNames"/>.Length long.
        /// Entries for fields not found are set to default (IsNone).
        /// Returns the number of fields successfully extracted.
        /// </summary>
        public static int ExtractFields(ReadOnlySpan<byte> json, string[] fieldNames, ExprToken[] results)
        {
            // Clear results
            for (var i = 0; i < fieldNames.Length; i++)
                results[i] = default;

            var s = TrimWhiteSpace(json);
            if (s.IsEmpty || s[0] != (byte)'{') return 0;
            s = s[1..]; // Skip '{'

            var found = 0;
            var needed = fieldNames.Length;

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;
                if (s[0] == (byte)'}') return found;

                // Expect a key string
                if (s[0] != (byte)'"') return found;

                var afterOpenQuote = s[1..];
                if (!SkipString(ref s)) return found;
                var keyContent = afterOpenQuote[..(afterOpenQuote.Length - s.Length - 1)];

                // Check against all requested field names
                var matchIndex = -1;
                for (var i = 0; i < fieldNames.Length; i++)
                {
                    if (results[i].IsNone && MatchKey(keyContent, fieldNames[i]))
                    {
                        matchIndex = i;
                        break;
                    }
                }

                // Expect ':'
                s = TrimWhiteSpace(s);
                if (s.IsEmpty || s[0] != (byte)':') return found;
                s = s[1..];

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;

                if (matchIndex >= 0)
                {
                    results[matchIndex] = ParseValueToken(json, ref s);
                    found++;
                    if (found == needed) return found; // All fields found — early exit
                }
                else
                {
                    if (!SkipValue(ref s)) return found;
                }

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return found;
                if (s[0] == (byte)',') { s = s[1..]; continue; }
                if (s[0] == (byte)'}') return found;
                return found; // Malformed JSON
            }
        }

        /// <summary>
        /// Extract a top-level field from a JSON object and return it as an ExprToken.
        /// Returns default (IsNone) if the field is not found or the JSON is malformed.
        /// </summary>
        public static ExprToken ExtractField(ReadOnlySpan<byte> json, string fieldName)
        {
            var s = TrimWhiteSpace(json);
            if (s.IsEmpty || s[0] != (byte)'{') return default;
            s = s[1..]; // Skip '{'

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (s[0] == (byte)'}') return default; // End of object, field not found

                // Expect a key string
                if (s[0] != (byte)'"') return default;

                // Extract key content (between quotes)
                var afterOpenQuote = s[1..];
                if (!SkipString(ref s)) return default;
                // Key content is between afterOpenQuote and s (minus the closing quote byte)
                var keyContent = afterOpenQuote[..(afterOpenQuote.Length - s.Length - 1)];

                var match = MatchKey(keyContent, fieldName);

                // Expect ':'
                s = TrimWhiteSpace(s);
                if (s.IsEmpty || s[0] != (byte)':') return default;
                s = s[1..]; // Skip ':'

                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;

                if (match)
                {
                    // Found the field — parse the value into a token
                    return ParseValueToken(json, ref s);
                }
                else
                {
                    // Skip the value
                    if (!SkipValue(ref s)) return default;
                }

                // Look for ',' or '}'
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (s[0] == (byte)',') { s = s[1..]; continue; }
                if (s[0] == (byte)'}') return default; // End of object, not found
                return default; // Malformed JSON
            }
        }

        // ======================== Value parsing (allocating) ========================

        private static ExprToken ParseValueToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s)
        {
            s = TrimWhiteSpace(s);
            if (s.IsEmpty) return default;

            var c = s[0];
            if (c == (byte)'"') return ParseStringToken(json, ref s);
            if (c == (byte)'[') return ParseArrayToken(json, ref s);
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
                    s = s[2..]; // Skip escape sequence
                    continue;
                }
                if (s[0] == (byte)'"')
                {
                    var content = body[..(body.Length - s.Length)];
                    if (!hasEscape)
                    {
                        // Zero-allocation: store byte offset+length into the source JSON
                        var absoluteStart = json.Length - body.Length;
                        s = s[1..]; // Skip closing quote
                        return ExprToken.NewJsonStr(absoluteStart, content.Length);
                    }
                    else
                    {
                        // Escaped strings must be materialized (rare path)
                        var value = UnescapeJsonString(content);
                        s = s[1..]; // Skip closing quote
                        return ExprToken.NewStr(value);
                    }
                }
                s = s[1..];
            }
            return default; // Unterminated string
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

            // Verify delimiter follows (space, comma, bracket, brace, or end)
            if (s.Length > literal.Length)
            {
                var next = (char)s[literal.Length];
                if (!char.IsWhiteSpace(next) && next != ',' && next != ']' && next != '}')
                    return default;
            }

            s = s[literal.Length..];
            return type == ExprTokenType.Null ? ExprToken.NewNull() : ExprToken.NewNum(num);
        }

        /// <summary>Max array elements before rejecting.</summary>
        private const int MaxArrayElements = 64;

        private static ExprToken ParseArrayToken(ReadOnlySpan<byte> json, ref ReadOnlySpan<byte> s)
        {
            if (s.IsEmpty || s[0] != (byte)'[') return default;
            s = s[1..]; // Skip '['
            s = TrimWhiteSpace(s);

            // Handle empty array
            if (!s.IsEmpty && s[0] == (byte)']')
            {
                s = s[1..];
                return ExprToken.NewTuple([], 0);
            }

            // Rent from pool instead of allocating a new scratch array every call
            var elements = ArrayPool<ExprToken>.Shared.Rent(MaxArrayElements);
            var count = 0;

            try
            {
                while (true)
                {
                    s = TrimWhiteSpace(s);
                    if (s.IsEmpty || count >= MaxArrayElements) return default;

                    var ele = ParseValueToken(json, ref s);
                    if (ele.IsNone) return default;
                    elements[count++] = ele;

                    s = TrimWhiteSpace(s);
                    if (s.IsEmpty) return default;
                    if (s[0] == (byte)',') { s = s[1..]; continue; }
                    if (s[0] == (byte)']') { s = s[1..]; break; }
                    return default; // Malformed
                }

                var result = new ExprToken[count];
                Array.Copy(elements, result, count);
                return ExprToken.NewTuple(result, count);
            }
            finally
            {
                ArrayPool<ExprToken>.Shared.Return(elements, clearArray: true);
            }
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
            s = s[1..]; // Skip opening quote
            while (!s.IsEmpty)
            {
                if (s[0] == (byte)'\\') { s = s[2..]; continue; }
                if (s[0] == (byte)'"') { s = s[1..]; return true; }
                s = s[1..];
            }
            return false; // Unterminated
        }

        private static bool SkipBracketed(ref ReadOnlySpan<byte> s, byte opener, byte closer)
        {
            var depth = 1;
            s = s[1..]; // Skip opener
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
        // These are used by both AttributeExtractor and ExprCompiler.

        internal static bool IsDigit(byte b) => b >= (byte)'0' && b <= (byte)'9';

        internal static bool IsLetter(byte b) => (b >= (byte)'a' && b <= (byte)'z') || (b >= (byte)'A' && b <= (byte)'Z');

        internal static bool IsLetterOrDigit(byte b) => IsLetter(b) || IsDigit(b);

        internal static bool IsWhiteSpace(byte b) => b == (byte)' ' || b == (byte)'\t' || b == (byte)'\n' || b == (byte)'\r';

        /// <summary>
        /// Returns the span with leading whitespace removed.
        /// </summary>
        internal static ReadOnlySpan<byte> TrimWhiteSpace(ReadOnlySpan<byte> s)
        {
            var i = 0;
            while (i < s.Length && IsWhiteSpace(s[i])) i++;
            return s[i..];
        }

        private static bool IsNumberChar(byte b) =>
            IsDigit(b) || b == (byte)'-' || b == (byte)'+' ||
            b == (byte)'.' || b == (byte)'e' || b == (byte)'E';

        private static bool MatchKey(ReadOnlySpan<byte> key, string fieldName)
        {
            if (key.Length != fieldName.Length) return false;
            for (var i = 0; i < key.Length; i++)
            {
                if (key[i] != (byte)fieldName[i]) return false;
            }
            return true;
        }

        private static string UnescapeJsonString(ReadOnlySpan<byte> content)
        {
            // Worst case: each byte is a character
            var chars = new char[content.Length];
            var len = 0;
            var i = 0;
            while (i < content.Length)
            {
                if (content[i] == (byte)'\\' && i + 1 < content.Length)
                {
                    i++;
                    chars[len++] = (char)content[i] switch
                    {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '"' => '"',
                        '/' => '/',
                        _ => (char)content[i],
                    };
                    i++;
                }
                else
                {
                    chars[len++] = (char)content[i];
                    i++;
                }
            }
            return new string(chars, 0, len);
        }
    }
}