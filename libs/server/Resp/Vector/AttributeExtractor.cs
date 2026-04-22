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

        // ======================== Binary attribute format ========================
        //
        // Pre-extracted binary format for fast filter evaluation:
        //   [0xFF marker]
        //   [num_fields: u8]
        //   For each field:
        //     [field_name_len: u8]
        //     [field_name: N bytes]         ← raw UTF-8
        //     [value_type: u8]              ← 0=string, 1=number, 2=bool_true, 3=bool_false, 4=null
        //     [value_len: u16 LE]
        //     [value_bytes: N bytes]        ← UTF-8 string or 8-byte f64 LE

        internal const byte BinaryMarker = 0xFF;

        private const byte BinTypeString = 0;
        private const byte BinTypeNumber = 1;
        private const byte BinTypeBoolTrue = 2;
        private const byte BinTypeBoolFalse = 3;
        private const byte BinTypeNull = 4;

        /// <summary>
        /// Convert a top-level JSON object to pre-extracted binary format.
        /// Returns total bytes written, or -1 if output is too small.
        /// </summary>
        public static int ConvertJsonToBinary(ReadOnlySpan<byte> json, Span<byte> output)
        {
            var s = TrimWhiteSpace(json);
            if (s.IsEmpty || s[0] != (byte)'{') return -1;
            s = s[1..];

            if (output.Length < 2) return -1;
            output[0] = BinaryMarker;
            // output[1] = num_fields — written at the end
            var pos = 2;
            byte fieldCount = 0;

            while (true)
            {
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return -1;
                if (s[0] == (byte)'}') break;

                if (s[0] != (byte)'"') return -1;

                // Parse key
                var afterOpenQuote = s[1..];
                if (!SkipString(ref s)) return -1;
                var keyContent = afterOpenQuote[..(afterOpenQuote.Length - s.Length - 1)];

                // Check for escape sequences in key (rare)
                var keyHasEscape = false;
                for (var ki = 0; ki < keyContent.Length; ki++)
                {
                    if (keyContent[ki] == (byte)'\\') { keyHasEscape = true; break; }
                }
                if (keyHasEscape) return -1; // keys with escapes not supported

                // Write field_name_len + field_name
                if (keyContent.Length > 255) return -1;
                if (pos + 1 + keyContent.Length + 1 + 2 > output.Length) return -1;
                output[pos++] = (byte)keyContent.Length;
                keyContent.CopyTo(output[pos..]);
                pos += keyContent.Length;

                // Skip colon
                s = TrimWhiteSpace(s);
                if (s.IsEmpty || s[0] != (byte)':') return -1;
                s = s[1..];

                // Parse value
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return -1;

                var c = s[0];
                if (c == (byte)'"')
                {
                    // String value — need to unescape
                    s = s[1..]; // skip opening quote
                    var body = s;
                    var hasEscape = false;
                    while (!s.IsEmpty)
                    {
                        if (s[0] == (byte)'\\') { hasEscape = true; s = s[2..]; continue; }
                        if (s[0] == (byte)'"') break;
                        s = s[1..];
                    }
                    if (s.IsEmpty) return -1;
                    var strContent = body[..(body.Length - s.Length)];
                    s = s[1..]; // skip closing quote

                    output[pos++] = BinTypeString;

                    if (!hasEscape)
                    {
                        // No escapes — direct copy
                        if (pos + 2 + strContent.Length > output.Length) return -1;
                        output[pos] = (byte)(strContent.Length & 0xFF);
                        output[pos + 1] = (byte)((strContent.Length >> 8) & 0xFF);
                        pos += 2;
                        strContent.CopyTo(output[pos..]);
                        pos += strContent.Length;
                    }
                    else
                    {
                        // Unescape into output
                        var valueLenPos = pos;
                        pos += 2; // reserve for value_len
                        var valueStart = pos;
                        for (var si = 0; si < strContent.Length; si++)
                        {
                            if (pos >= output.Length) return -1;
                            if (strContent[si] == (byte)'\\' && si + 1 < strContent.Length)
                            {
                                si++;
                                output[pos++] = strContent[si] switch
                                {
                                    (byte)'n' => (byte)'\n',
                                    (byte)'r' => (byte)'\r',
                                    (byte)'t' => (byte)'\t',
                                    _ => strContent[si], // \", \\, \/ etc.
                                };
                            }
                            else
                            {
                                output[pos++] = strContent[si];
                            }
                        }
                        var valueLen = pos - valueStart;
                        output[valueLenPos] = (byte)(valueLen & 0xFF);
                        output[valueLenPos + 1] = (byte)((valueLen >> 8) & 0xFF);
                    }
                }
                else if (IsDigit(c) || c == (byte)'-' || c == (byte)'+')
                {
                    // Number value — store as 8-byte f64 LE
                    var numStart = s;
                    while (!s.IsEmpty && IsNumberChar(s[0])) s = s[1..];
                    var numSpan = numStart[..(numStart.Length - s.Length)];
                    if (!Utf8Parser.TryParse(numSpan, out double numVal, out var consumed) || consumed != numSpan.Length)
                        return -1;

                    output[pos++] = BinTypeNumber;
                    if (pos + 2 + 8 > output.Length) return -1;
                    output[pos] = 8;
                    output[pos + 1] = 0;
                    pos += 2;
                    System.BitConverter.TryWriteBytes(output[pos..], numVal);
                    pos += 8;
                }
                else if (c == (byte)'t')
                {
                    if (!s.StartsWith("true"u8)) return -1;
                    s = s[4..];
                    output[pos++] = BinTypeBoolTrue;
                    if (pos + 2 > output.Length) return -1;
                    output[pos] = 0; output[pos + 1] = 0;
                    pos += 2;
                }
                else if (c == (byte)'f')
                {
                    if (!s.StartsWith("false"u8)) return -1;
                    s = s[5..];
                    output[pos++] = BinTypeBoolFalse;
                    if (pos + 2 > output.Length) return -1;
                    output[pos] = 0; output[pos + 1] = 0;
                    pos += 2;
                }
                else if (c == (byte)'n')
                {
                    if (!s.StartsWith("null"u8)) return -1;
                    s = s[4..];
                    output[pos++] = BinTypeNull;
                    if (pos + 2 > output.Length) return -1;
                    output[pos] = 0; output[pos + 1] = 0;
                    pos += 2;
                }
                else
                {
                    // Nested objects/arrays — not supported in binary format
                    return -1;
                }

                fieldCount++;

                // Next field or end
                s = TrimWhiteSpace(s);
                if (s.IsEmpty) return -1;
                if (s[0] == (byte)',') { s = s[1..]; continue; }
                if (s[0] == (byte)'}') break;
                return -1;
            }

            output[1] = fieldCount;
            return pos;
        }

        /// <summary>
        /// Extract fields from pre-extracted binary attribute data.
        /// Same contract as ExtractFields but ~10x faster (no JSON parsing).
        /// </summary>
        public static int ExtractFieldsBinary(
            ReadOnlySpan<byte> binary,
            ReadOnlySpan<byte> filterBytes,
            ReadOnlySpan<(int Start, int Length)> selectorRanges,
            Span<ExprToken> results,
            ref ExprProgram program)
        {
            for (var i = 0; i < selectorRanges.Length; i++)
                results[i] = default;

            if (binary.Length < 2 || binary[0] != BinaryMarker)
                return 0;

            var numFields = binary[1];
            var pos = 2;
            var found = 0;
            var needed = selectorRanges.Length;

            for (var f = 0; f < numFields && pos < binary.Length; f++)
            {
                // Read field name
                if (pos >= binary.Length) break;
                var nameLen = binary[pos++];
                if (pos + nameLen > binary.Length) break;
                var fieldName = binary.Slice(pos, nameLen);
                pos += nameLen;

                // Read value type
                if (pos >= binary.Length) break;
                var valueType = binary[pos++];

                // Read value length
                if (pos + 2 > binary.Length) break;
                var valueLen = (int)(binary[pos] | (binary[pos + 1] << 8));
                pos += 2;

                // Read value bytes
                if (pos + valueLen > binary.Length) break;

                // Match against selectors
                var matchIndex = -1;
                for (var i = 0; i < selectorRanges.Length; i++)
                {
                    if (results[i].IsNone &&
                        fieldName.SequenceEqual(filterBytes.Slice(selectorRanges[i].Start, selectorRanges[i].Length)))
                    {
                        matchIndex = i;
                        break;
                    }
                }

                if (matchIndex >= 0)
                {
                    switch (valueType)
                    {
                        case BinTypeString:
                            // Create a Str token referencing the binary buffer offsets
                            results[matchIndex] = ExprToken.NewStr(pos, valueLen, hasEscape: false);
                            break;
                        case BinTypeNumber:
                            if (valueLen == 8)
                            {
                                var numVal = System.BitConverter.ToDouble(binary[pos..]);
                                results[matchIndex] = ExprToken.NewNum(numVal);
                            }
                            break;
                        case BinTypeBoolTrue:
                            results[matchIndex] = ExprToken.NewNum(1);
                            break;
                        case BinTypeBoolFalse:
                            results[matchIndex] = ExprToken.NewNum(0);
                            break;
                        case BinTypeNull:
                            results[matchIndex] = ExprToken.NewNull();
                            break;
                    }

                    found++;
                    if (found == needed) return found;
                }

                pos += valueLen;
            }

            return found;
        }
    }
}
