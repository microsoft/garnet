// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Text;

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
        /// Extract a top-level field from a JSON object and return it as an ExprToken.
        /// Returns default (IsNone) if the field is not found or the JSON is malformed.
        /// </summary>
        public static ExprToken ExtractField(ReadOnlySpan<byte> json, string fieldName)
        {
            var p = 0;
            SkipWhiteSpace(json, ref p);
            if (p >= json.Length || json[p] != (byte)'{') return default;
            p++; // Skip '{'

            while (true)
            {
                SkipWhiteSpace(json, ref p);
                if (p >= json.Length) return default;
                if (json[p] == (byte)'}') return default; // End of object, field not found

                // Expect a key string
                if (json[p] != (byte)'"') return default;

                var keyStart = p + 1;
                if (!SkipString(json, ref p)) return default;
                var keyEnd = p - 1; // p is now past the closing quote

                // Compare key with field name
                var match = MatchKey(json, keyStart, keyEnd, fieldName);

                // Expect ':'
                SkipWhiteSpace(json, ref p);
                if (p >= json.Length || json[p] != (byte)':') return default;
                p++; // Skip ':'

                SkipWhiteSpace(json, ref p);
                if (p >= json.Length) return default;

                if (match)
                {
                    // Found the field — parse the value into a token
                    return ParseValueToken(json, ref p);
                }
                else
                {
                    // Skip the value
                    if (!SkipValue(json, ref p)) return default;
                }

                // Look for ',' or '}'
                SkipWhiteSpace(json, ref p);
                if (p >= json.Length) return default;
                if (json[p] == (byte)',') { p++; continue; }
                if (json[p] == (byte)'}') return default; // End of object, not found
                return default; // Malformed JSON
            }
        }

        // ======================== Value parsing (allocating) ========================

        private static ExprToken ParseValueToken(ReadOnlySpan<byte> json, ref int p)
        {
            SkipWhiteSpace(json, ref p);
            if (p >= json.Length) return default;

            var c = json[p];
            if (c == (byte)'"') return ParseStringToken(json, ref p);
            if (c == (byte)'[') return ParseArrayToken(json, ref p);
            if (c == (byte)'{') return default; // Nested objects not supported
            if (c == (byte)'t') return ParseLiteralToken(json, ref p, "true"u8, ExprTokenType.Num, 1);
            if (c == (byte)'f') return ParseLiteralToken(json, ref p, "false"u8, ExprTokenType.Num, 0);
            if (c == (byte)'n') return ParseLiteralToken(json, ref p, "null"u8, ExprTokenType.Null, 0);
            if ((c >= (byte)'0' && c <= (byte)'9') || c == (byte)'-' || c == (byte)'+')
                return ParseNumberToken(json, ref p);

            return default;
        }

        private static ExprToken ParseStringToken(ReadOnlySpan<byte> json, ref int p)
        {
            if (p >= json.Length || json[p] != (byte)'"') return default;
            p++; // Skip opening quote
            var start = p;
            var hasEscape = false;

            while (p < json.Length)
            {
                if (json[p] == (byte)'\\')
                {
                    hasEscape = true;
                    p += 2; // Skip escape sequence
                    continue;
                }
                if (json[p] == (byte)'"')
                {
                    string value;
                    if (!hasEscape)
                    {
                        // Zero-copy: decode directly from the span
                        value = Encoding.UTF8.GetString(json.Slice(start, p - start));
                    }
                    else
                    {
                        // Process escapes
                        value = UnescapeJsonString(json, start, p);
                    }
                    p++; // Skip closing quote
                    return ExprToken.NewStr(value);
                }
                p++;
            }
            return default; // Unterminated string
        }

        private static ExprToken ParseNumberToken(ReadOnlySpan<byte> json, ref int p)
        {
            var start = p;
            while (p < json.Length && IsNumberChar(json[p])) p++;
            if (p == start) return default;

            var numSpan = json.Slice(start, p - start);
            if (!Utf8Parser.TryParse(numSpan, out double value, out var bytesConsumed) || bytesConsumed != numSpan.Length)
            {
                p = start;
                return default;
            }
            return ExprToken.NewNum(value);
        }

        private static ExprToken ParseLiteralToken(ReadOnlySpan<byte> json, ref int p,
            ReadOnlySpan<byte> literal, ExprTokenType type, double num)
        {
            if (p + literal.Length > json.Length) return default;
            if (!json.Slice(p, literal.Length).SequenceEqual(literal)) return default;

            // Verify delimiter follows (space, comma, bracket, brace, or end)
            if (p + literal.Length < json.Length)
            {
                var next = (char)json[p + literal.Length];
                if (!char.IsWhiteSpace(next) && next != ',' && next != ']' && next != '}')
                    return default;
            }

            p += literal.Length;
            var t = type == ExprTokenType.Null ? ExprToken.NewNull() : ExprToken.NewNum(num);
            return t;
        }

        private static ExprToken ParseArrayToken(ReadOnlySpan<byte> json, ref int p)
        {
            if (p >= json.Length || json[p] != (byte)'[') return default;
            p++; // Skip '['
            SkipWhiteSpace(json, ref p);

            var elements = new ExprToken[64];
            var count = 0;

            // Handle empty array
            if (p < json.Length && json[p] == (byte)']')
            {
                p++;
                return ExprToken.NewTuple([], 0);
            }

            while (true)
            {
                SkipWhiteSpace(json, ref p);
                if (p >= json.Length || count >= elements.Length) return default;

                var ele = ParseValueToken(json, ref p);
                if (ele.IsNone) return default;
                elements[count++] = ele;

                SkipWhiteSpace(json, ref p);
                if (p >= json.Length) return default;
                if (json[p] == (byte)',') { p++; continue; }
                if (json[p] == (byte)']') { p++; break; }
                return default; // Malformed
            }

            var result = new ExprToken[count];
            Array.Copy(elements, result, count);
            return ExprToken.NewTuple(result, count);
        }

        // ======================== Fast skipping (non-allocating) ========================

        private static bool SkipValue(ReadOnlySpan<byte> json, ref int p)
        {
            SkipWhiteSpace(json, ref p);
            if (p >= json.Length) return false;

            var c = (char)json[p];
            return c switch
            {
                '"' => SkipString(json, ref p),
                '{' => SkipBracketed(json, ref p, (byte)'{', (byte)'}'),
                '[' => SkipBracketed(json, ref p, (byte)'[', (byte)']'),
                't' => SkipLiteral(json, ref p, "true"u8),
                'f' => SkipLiteral(json, ref p, "false"u8),
                'n' => SkipLiteral(json, ref p, "null"u8),
                _ => SkipNumber(json, ref p),
            };
        }

        private static bool SkipString(ReadOnlySpan<byte> json, ref int p)
        {
            if (p >= json.Length || json[p] != (byte)'"') return false;
            p++; // Skip opening quote
            while (p < json.Length)
            {
                if (json[p] == (byte)'\\') { p += 2; continue; }
                if (json[p] == (byte)'"') { p++; return true; }
                p++;
            }
            return false; // Unterminated
        }

        private static bool SkipBracketed(ReadOnlySpan<byte> json, ref int p, byte opener, byte closer)
        {
            var depth = 1;
            p++; // Skip opener
            while (p < json.Length && depth > 0)
            {
                if (json[p] == (byte)'"')
                {
                    if (!SkipString(json, ref p)) return false;
                    continue;
                }
                if (json[p] == opener) depth++;
                else if (json[p] == closer) depth--;
                p++;
            }
            return depth == 0;
        }

        private static bool SkipLiteral(ReadOnlySpan<byte> json, ref int p, ReadOnlySpan<byte> literal)
        {
            if (p + literal.Length > json.Length) return false;
            if (!json.Slice(p, literal.Length).SequenceEqual(literal)) return false;
            p += literal.Length;
            return true;
        }

        private static bool SkipNumber(ReadOnlySpan<byte> json, ref int p)
        {
            var start = p;
            while (p < json.Length && IsNumberChar(json[p])) p++;
            return p > start;
        }

        // ======================== Utility ========================

        private static void SkipWhiteSpace(ReadOnlySpan<byte> json, ref int p)
        {
            while (p < json.Length && IsWhiteSpace(json[p])) p++;
        }

        private static bool IsWhiteSpace(byte b) => b == (byte)' ' || b == (byte)'\t' || b == (byte)'\n' || b == (byte)'\r';

        private static bool IsNumberChar(byte b) =>
            (b >= (byte)'0' && b <= (byte)'9') || b == (byte)'-' || b == (byte)'+' ||
            b == (byte)'.' || b == (byte)'e' || b == (byte)'E';

        private static bool MatchKey(ReadOnlySpan<byte> json, int keyStart, int keyEnd, string fieldName)
        {
            var keyLen = keyEnd - keyStart;
            if (keyLen != fieldName.Length) return false;
            for (var i = 0; i < keyLen; i++)
            {
                if (json[keyStart + i] != (byte)fieldName[i]) return false;
            }
            return true;
        }

        private static string UnescapeJsonString(ReadOnlySpan<byte> json, int start, int end)
        {
            // Worst case: each byte is a character
            var chars = new char[end - start];
            var len = 0;
            var i = start;
            while (i < end)
            {
                if (json[i] == (byte)'\\' && i + 1 < end)
                {
                    i++;
                    chars[len++] = (char)json[i] switch
                    {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '"' => '"',
                        '/' => '/',
                        _ => (char)json[i],
                    };
                    i++;
                }
                else
                {
                    chars[len++] = (char)json[i];
                    i++;
                }
            }
            return new string(chars, 0, len);
        }
    }
}