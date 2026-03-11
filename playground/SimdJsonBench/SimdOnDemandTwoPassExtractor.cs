// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// SimdOnDemandTwoPassExtractor — a pure managed simdjson-inspired two-pass field extractor.
//
// Two-pass structural index design:
//   Phase 1: Build a structural index using AVX2/SSE2 SIMD vectorized byte classification.
//            Records positions of { } [ ] : , and opening quotes outside strings.
//   Phase 2: Walk the structural index to find keys and extract values.

#nullable enable

using System.Buffers.Text;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Garnet.server.Vector.Filter;

/// <summary>
/// Pure-managed SIMD two-pass JSON field extractor.
/// Produces the same <see cref="ExprToken"/> output as <see cref="AttributeExtractor"/>.
///
/// Two-pass design:
///   Pass 1 — Build a structural index of positions for { } [ ] : , and opening "
///            using AVX2 (32 bytes/iter), SSE2 (16 bytes/iter), or scalar fallback.
///            Quote-state tracking ensures characters inside strings are ignored.
///   Pass 2 — Walk the structural index to locate keys, match by name, and extract values.
/// </summary>
internal static class SimdOnDemandTwoPassExtractor
{
    // Maximum structural index entries on the stack.
    private const int IndexBufSize = 512;

    // ════════════════════════════════════════════════════════════════════
    //  Public API
    // ════════════════════════════════════════════════════════════════════

    /// <summary>Extract a single top-level field.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static ExprToken ExtractField(ReadOnlySpan<byte> json, ReadOnlySpan<byte> fieldNameUtf8)
    {
        Span<int> indexBuf = stackalloc int[IndexBufSize];
        var indexLen = BuildStructuralIndex(json, indexBuf);

        // Walk the structural index to find the opening '{' then iterate key-value pairs.
        var idx = 0; // cursor into indexBuf

        // Skip to opening '{'
        while (idx < indexLen && json[indexBuf[idx]] != (byte)'{') idx++;
        if (idx >= indexLen) return default;
        idx++; // past '{'

        while (idx < indexLen)
        {
            var structChar = json[indexBuf[idx]];

            // End of object
            if (structChar == (byte)'}') return default;

            // Expect opening quote of key
            if (structChar != (byte)'"') return default;
            var keyQuotePos = indexBuf[idx];
            idx++;

            // The key content starts at keyQuotePos + 1 and ends at the closing quote
            var keyContentStart = keyQuotePos + 1;
            var keyClosingQuote = FindClosingQuote(json, keyContentStart);
            if (keyClosingQuote < 0) return default;
            var keySpan = json.Slice(keyContentStart, keyClosingQuote - keyContentStart);
            var isMatch = keySpan.SequenceEqual(fieldNameUtf8);

            // Expect ':' — advance structural index to find it
            while (idx < indexLen && json[indexBuf[idx]] != (byte)':') idx++;
            if (idx >= indexLen) return default;
            idx++; // past ':'

            if (idx >= indexLen) return default;

            if (isMatch)
            {
                // Parse the value starting at the byte position of the next structural char
                var valuePos = indexBuf[idx];
                return ParseValueAtPos(json, ref valuePos);
            }

            // Skip the value using structural index depth tracking
            SkipValueInIndex(json, indexBuf, indexLen, ref idx);

            // Skip comma if present
            if (idx < indexLen && json[indexBuf[idx]] == (byte)',') idx++;
        }

        return default;
    }

    /// <summary>Extract multiple top-level fields in a single structural pass.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static int ExtractFields(ReadOnlySpan<byte> json, byte[][] fieldNamesUtf8, ExprToken[] results)
    {
        var fieldCount = fieldNamesUtf8.Length;
        for (var i = 0; i < fieldCount; i++)
            results[i] = default;

        Span<int> indexBuf = stackalloc int[IndexBufSize];
        var indexLen = BuildStructuralIndex(json, indexBuf);

        var idx = 0;

        // Skip to opening '{'
        while (idx < indexLen && json[indexBuf[idx]] != (byte)'{') idx++;
        if (idx >= indexLen) return 0;
        idx++; // past '{'

        var found = 0;

        while (idx < indexLen)
        {
            var structChar = json[indexBuf[idx]];

            if (structChar == (byte)'}') return found;

            if (structChar != (byte)'"') return found;
            var keyQuotePos = indexBuf[idx];
            idx++;

            var keyContentStart = keyQuotePos + 1;
            var keyClosingQuote = FindClosingQuote(json, keyContentStart);
            if (keyClosingQuote < 0) return found;
            var keySpan = json.Slice(keyContentStart, keyClosingQuote - keyContentStart);

            var matchIdx = -1;
            for (var i = 0; i < fieldCount; i++)
            {
                if (results[i].IsNone && keySpan.SequenceEqual(fieldNamesUtf8[i]))
                {
                    matchIdx = i;
                    break;
                }
            }

            // Advance to ':'
            while (idx < indexLen && json[indexBuf[idx]] != (byte)':') idx++;
            if (idx >= indexLen) return found;
            idx++; // past ':'

            if (idx >= indexLen) return found;

            if (matchIdx >= 0)
            {
                var valuePos = indexBuf[idx];
                results[matchIdx] = ParseValueAtPos(json, ref valuePos);
                found++;
                if (found == fieldCount) return found;
                // Advance past the value in the structural index
                SkipValueInIndex(json, indexBuf, indexLen, ref idx);
            }
            else
            {
                SkipValueInIndex(json, indexBuf, indexLen, ref idx);
            }

            // Skip comma
            if (idx < indexLen && json[indexBuf[idx]] == (byte)',') idx++;
        }

        return found;
    }

    // ════════════════════════════════════════════════════════════════════
    //  Phase 1: Build Structural Index (AVX2 / SSE2 / Scalar)
    // ════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Scan the entire JSON buffer and record the byte offsets of structural characters
    /// ({ } [ ] : ,) and opening quote marks that are NOT inside a string.
    /// Returns the number of entries written to <paramref name="indexBuf"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe int BuildStructuralIndex(ReadOnlySpan<byte> json, Span<int> indexBuf)
    {
        var count = 0;
        var maxCount = indexBuf.Length;
        var length = json.Length;

        fixed (byte* pJson = json)
        fixed (int* pIndex = indexBuf)
        {
            var pos = 0;
            var inString = false;

            // ── AVX2 path: 32 bytes per iteration ──────────────────────
            if (Avx2.IsSupported && length >= 32)
            {
                var vQuote = Vector256.Create((byte)'"');
                var vBackslash = Vector256.Create((byte)'\\');
                var vLBrace = Vector256.Create((byte)'{');
                var vRBrace = Vector256.Create((byte)'}');
                var vLBracket = Vector256.Create((byte)'[');
                var vRBracket = Vector256.Create((byte)']');
                var vColon = Vector256.Create((byte)':');
                var vComma = Vector256.Create((byte)',');

                while (pos + 32 <= length && count < maxCount - 64)
                {
                    var chunk = Avx.LoadVector256(pJson + pos);

                    var mQuote = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vQuote));
                    var mBackslash = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vBackslash));
                    var mLBrace = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vLBrace));
                    var mRBrace = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vRBrace));
                    var mLBracket = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vLBracket));
                    var mRBracket = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vRBracket));
                    var mColon = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vColon));
                    var mComma = (uint)Avx2.MoveMask(Avx2.CompareEqual(chunk, vComma));

                    // Structural chars (everything except quotes and backslashes)
                    var mStructural = mLBrace | mRBrace | mLBracket | mRBracket | mColon | mComma;

                    // Process each bit position in order (low to high) to maintain
                    // correct quote-state tracking across the 32-byte window.
                    var mAll = mQuote | mStructural;

                    while (mAll != 0)
                    {
                        var bit = mAll & (uint)(-(int)mAll); // isolate lowest set bit
                        var bitPos = BitOperations.TrailingZeroCount(mAll);
                        var absPos = pos + bitPos;

                        if ((mQuote & bit) != 0)
                        {
                            // Quote character — check if it is escaped
                            if ((mBackslash & (bit >> 1)) != 0 && bitPos > 0)
                            {
                                // The preceding byte in the chunk is a backslash — but we
                                // need to handle runs of backslashes. Fall back to scalar check.
                                if (IsEscapedAt(pJson, absPos))
                                {
                                    // Escaped quote — skip it
                                    mAll &= mAll - 1;
                                    continue;
                                }
                            }
                            else if (bitPos == 0 && absPos > 0 && IsEscapedAt(pJson, absPos))
                            {
                                // First byte of chunk — preceding backslash is in the prior chunk
                                mAll &= mAll - 1;
                                continue;
                            }

                            if (!inString)
                            {
                                // Opening quote — record it as a structural position
                                if (count < maxCount)
                                    pIndex[count++] = absPos;
                            }
                            inString = !inString;
                        }
                        else
                        {
                            // Non-quote structural char — only record when outside a string
                            if (!inString && count < maxCount)
                                pIndex[count++] = absPos;
                        }

                        mAll &= mAll - 1; // clear lowest set bit
                    }

                    pos += 32;
                }
            }
            // ── SSE2 path: 16 bytes per iteration ──────────────────────
            else if (Sse2.IsSupported && length >= 16)
            {
                var vQuote = Vector128.Create((byte)'"');
                var vBackslash = Vector128.Create((byte)'\\');
                var vLBrace = Vector128.Create((byte)'{');
                var vRBrace = Vector128.Create((byte)'}');
                var vLBracket = Vector128.Create((byte)'[');
                var vRBracket = Vector128.Create((byte)']');
                var vColon = Vector128.Create((byte)':');
                var vComma = Vector128.Create((byte)',');

                while (pos + 16 <= length && count < maxCount - 32)
                {
                    var chunk = Sse2.LoadVector128(pJson + pos);

                    var mQuote = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vQuote));
                    var mBackslash = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vBackslash));
                    var mLBrace = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vLBrace));
                    var mRBrace = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vRBrace));
                    var mLBracket = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vLBracket));
                    var mRBracket = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vRBracket));
                    var mColon = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vColon));
                    var mComma = (uint)Sse2.MoveMask(Sse2.CompareEqual(chunk, vComma));

                    var mStructural = mLBrace | mRBrace | mLBracket | mRBracket | mColon | mComma;
                    var mAll = mQuote | mStructural;

                    while (mAll != 0)
                    {
                        var bit = mAll & (uint)(-(int)mAll);
                        var bitPos = BitOperations.TrailingZeroCount(mAll);
                        var absPos = pos + bitPos;

                        if ((mQuote & bit) != 0)
                        {
                            if ((mBackslash & (bit >> 1)) != 0 && bitPos > 0)
                            {
                                if (IsEscapedAt(pJson, absPos))
                                {
                                    mAll &= mAll - 1;
                                    continue;
                                }
                            }
                            else if (bitPos == 0 && absPos > 0 && IsEscapedAt(pJson, absPos))
                            {
                                mAll &= mAll - 1;
                                continue;
                            }

                            if (!inString)
                            {
                                if (count < maxCount)
                                    pIndex[count++] = absPos;
                            }
                            inString = !inString;
                        }
                        else
                        {
                            if (!inString && count < maxCount)
                                pIndex[count++] = absPos;
                        }

                        mAll &= mAll - 1;
                    }

                    pos += 16;
                }
            }

            // ── Scalar fallback for remaining bytes ────────────────────
            while (pos < length && count < maxCount)
            {
                var b = pJson[pos];

                if (b == (byte)'"')
                {
                    if (pos > 0 && IsEscapedAt(pJson, pos))
                    {
                        pos++;
                        continue;
                    }
                    if (!inString)
                    {
                        pIndex[count++] = pos;
                    }
                    inString = !inString;
                }
                else if (!inString)
                {
                    if (b == (byte)'{' || b == (byte)'}' || b == (byte)'[' || b == (byte)']' ||
                        b == (byte)':' || b == (byte)',')
                    {
                        pIndex[count++] = pos;
                    }
                }

                pos++;
            }
        }

        return count;
    }

    // ════════════════════════════════════════════════════════════════════
    //  Escape detection helpers
    // ════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Backward scan to determine whether the byte at <paramref name="pos"/> is preceded
    /// by an odd number of backslashes (i.e., it is escaped).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe bool IsEscapedAt(byte* pJson, int pos)
    {
        var backslashCount = 0;
        var i = pos - 1;
        while (i >= 0 && pJson[i] == (byte)'\\')
        {
            backslashCount++;
            i--;
        }
        return (backslashCount & 1) != 0;
    }

    // ════════════════════════════════════════════════════════════════════
    //  Phase 2 helpers: scalar string scanning
    // ════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Starting at <paramref name="contentStart"/> (the byte after the opening quote),
    /// find the position of the unescaped closing quote.
    /// Returns -1 if unterminated.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FindClosingQuote(ReadOnlySpan<byte> json, int contentStart)
    {
        var pos = contentStart;
        while (pos < json.Length)
        {
            var remaining = json.Slice(pos);
            var idx = remaining.IndexOfAny((byte)'"', (byte)'\\');
            if (idx < 0) return -1;

            pos += idx;
            if (json[pos] == (byte)'"') return pos;

            // Backslash — skip the escaped character
            pos += 2;
        }
        return -1;
    }

    // ════════════════════════════════════════════════════════════════════
    //  Phase 2: Value parsing — on-demand, zero-alloc where possible
    // ════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Parse a JSON value starting at byte position <paramref name="pos"/>.
    /// Advances <paramref name="pos"/> past the consumed value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ExprToken ParseValueAtPos(ReadOnlySpan<byte> json, ref int pos)
    {
        SkipWhiteSpace(json, ref pos);
        if (pos >= json.Length) return default;

        var c = json[pos];

        if (c == (byte)'"')
        {
            pos++; // skip opening quote
            var contentStart = pos;
            var hasEscape = false;

            while (pos < json.Length)
            {
                var remaining = json.Slice(pos);
                var idx = remaining.IndexOfAny((byte)'"', (byte)'\\');
                if (idx < 0) return default;

                pos += idx;
                if (json[pos] == (byte)'"')
                {
                    var contentLen = pos - contentStart;
                    pos++; // skip closing quote
                    if (!hasEscape)
                        return ExprToken.NewJsonStr(contentStart, contentLen);
                    return ExprToken.NewStr(UnescapeToString(json.Slice(contentStart, contentLen)));
                }

                // Backslash escape
                hasEscape = true;
                pos += 2;
            }
            return default;
        }

        if ((c >= (byte)'0' && c <= (byte)'9') || c == (byte)'-')
        {
            var numStart = pos;
            pos++;
            while (pos < json.Length && IsNumberChar(json[pos])) pos++;
            var numSpan = json.Slice(numStart, pos - numStart);
            if (Utf8Parser.TryParse(numSpan, out double dval, out var consumed) && consumed == numSpan.Length)
                return ExprToken.NewNum(dval);
            return default;
        }

        if (c == (byte)'t')
        {
            if (pos + 4 <= json.Length && json.Slice(pos, 4).SequenceEqual("true"u8))
            { pos += 4; return ExprToken.NewNum(1); }
            return default;
        }

        if (c == (byte)'f')
        {
            if (pos + 5 <= json.Length && json.Slice(pos, 5).SequenceEqual("false"u8))
            { pos += 5; return ExprToken.NewNum(0); }
            return default;
        }

        if (c == (byte)'n')
        {
            if (pos + 4 <= json.Length && json.Slice(pos, 4).SequenceEqual("null"u8))
            { pos += 4; return ExprToken.NewNull(); }
            return default;
        }

        if (c == (byte)'[' || c == (byte)'{')
        {
            SkipNestedValue(json, ref pos);
            return default;
        }

        return default;
    }

    // ════════════════════════════════════════════════════════════════════
    //  Phase 2: Value skipping — structural index depth tracking
    // ════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Advance <paramref name="idx"/> (the structural index cursor) past the current
    /// value (which may be a nested object/array, string, or primitive).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SkipValueInIndex(ReadOnlySpan<byte> json, Span<int> indexBuf, int indexLen, ref int idx)
    {
        if (idx >= indexLen) return;

        var c = json[indexBuf[idx]];

        // String value — the opening quote is in the index; skip past it.
        if (c == (byte)'"')
        {
            idx++;
            return;
        }

        // Nested object or array — depth tracking through structural entries.
        if (c == (byte)'{' || c == (byte)'[')
        {
            var depth = 1;
            idx++;
            while (idx < indexLen && depth > 0)
            {
                var sc = json[indexBuf[idx]];
                if (sc == (byte)'{' || sc == (byte)'[') depth++;
                else if (sc == (byte)'}' || sc == (byte)']') depth--;
                idx++;
            }
            return;
        }

        // Primitive (number, true, false, null) — no structural char marks them,
        // so the next structural char in the index is already past the value.
        // Do NOT advance idx — the caller will see the next comma or closing bracket.
    }

    /// <summary>
    /// Skip a nested object or array by scanning bytes with depth tracking.
    /// Used by <see cref="ParseValueAtPos"/> when it encounters [ or { as a value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SkipNestedValue(ReadOnlySpan<byte> json, ref int pos)
    {
        if (pos >= json.Length) return;
        var opener = json[pos];
        if (opener != (byte)'{' && opener != (byte)'[') return;

        var closer = opener == (byte)'{' ? (byte)'}' : (byte)']';
        var depth = 1;
        pos++;
        while (pos < json.Length && depth > 0)
        {
            var ch = json[pos];
            if (ch == (byte)'"')
            {
                pos++;
                // Skip string contents
                while (pos < json.Length)
                {
                    var remaining = json.Slice(pos);
                    var idx = remaining.IndexOfAny((byte)'"', (byte)'\\');
                    if (idx < 0) return;
                    pos += idx;
                    if (json[pos] == (byte)'"') { pos++; break; }
                    pos += 2; // skip escaped char
                }
                continue;
            }
            if (ch == opener) depth++;
            else if (ch == closer) depth--;
            pos++;
        }
    }

    // ════════════════════════════════════════════════════════════════════
    //  Helpers
    // ════════════════════════════════════════════════════════════════════

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SkipWhiteSpace(ReadOnlySpan<byte> json, ref int pos)
    {
        while (pos < json.Length)
        {
            var b = json[pos];
            if (b != (byte)' ' && b != (byte)'\t' && b != (byte)'\n' && b != (byte)'\r')
                return;
            pos++;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNumberChar(byte b) =>
        (b >= (byte)'0' && b <= (byte)'9') || b == (byte)'-' || b == (byte)'+' ||
        b == (byte)'.' || b == (byte)'e' || b == (byte)'E';

    private static string UnescapeToString(ReadOnlySpan<byte> content)
    {
        var chars = new char[content.Length];
        var len = 0;
        for (var i = 0; i < content.Length; i++)
        {
            if (content[i] == (byte)'\\' && i + 1 < content.Length)
            {
                i++;
                chars[len++] = (char)content[i] switch
                {
                    'n' => '\n', 'r' => '\r', 't' => '\t',
                    '\\' => '\\', '"' => '"', '/' => '/',
                    _ => (char)content[i],
                };
            }
            else
            {
                chars[len++] = (char)content[i];
            }
        }
        return new string(chars, 0, len);
    }
}
