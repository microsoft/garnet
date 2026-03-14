// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;

namespace Garnet.server
{
    /// <summary>
    /// Filter expression post-processing for vector similarity search results.
    /// </summary>
    public sealed partial class VectorManager
    {
        // ── Buffer size constants ────────────────────────────────────────
        //
        // ExprToken is 16 bytes (explicit layout, blittable).
        //
        // All ExprToken buffers are rented as a single contiguous array from
        // ArrayPool<ExprToken>.Shared, then sliced into sub-spans. This gives
        // cache-line-friendly locality (all slices adjacent in memory) with a
        // single Rent/Return pair and zero stack pressure.
        //
        // The selectorBuf ((int,int) tuples) remains on the stack since it is
        // a different element type and only 256 bytes.
        //
        // If any limit is exceeded, the filter either fails to compile
        // (returns 0 = no results pass) or the candidate is excluded
        // gracefully.
        //
        //  Buffer           Size     Bytes   Limitation
        //  ────────────────  ───────  ──────  ─────────────────────────────────
        //  instrBuf          128×16   2,048   Max compiled postfix instructions.
        //                                     128 supports ~18 AND/OR clauses.
        //
        //  tuplePoolBuf       64×16   1,024   Max compile-time tuple elements
        //                                     across all IN [...] literals.
        //
        //  tokensBuf         128×16   2,048   Compiler scratch: Phase 1 tokens.
        //
        //  opsStackBuf       128×16   2,048   Compiler scratch: shunting-yard
        //                                     operator stack.
        //
        //  runtimePoolBuf     64×16   1,024   Max runtime tuple elements from
        //                                     JSON array extraction (IN operator
        //                                     on JSON array fields).
        //
        //  extractedFields    32×16     512   Pre-extracted field values per
        //                                     candidate. Mirrors selectorBuf.
        //
        //  stackBuf           16×16     256   Evaluation stack depth.
        //                                     Postfix eval rarely exceeds 8.
        //
        //  Total pool: 560 ExprTokens = 8,960 bytes (single ArrayPool rental)
        //           + 32 (int,int) selectors = 256 bytes (separate ArrayPool rental).
        //  Stack: 0 bytes.

        /// <summary>Max compiled postfix instructions. Overflow → compile error.</summary>
        private const int MaxInstructions = 128;

        /// <summary>Max compile-time tuple pool elements (all IN [...] literals combined). Overflow → compile error.</summary>
        private const int MaxTuplePool = 64;

        /// <summary>Max runtime tuple pool elements (JSON array extraction). Overflow → array treated as null.</summary>
        private const int MaxRuntimePool = 64;

        /// <summary>Max unique field selectors (e.g. .year, .rating). Overflow → extra selectors silently ignored.</summary>
        private const int MaxSelectors = 32;

        /// <summary>Evaluation stack depth. Overflow → TryPush returns false → candidate excluded.</summary>
        private const int StackCapacity = 16;

        /// <summary>
        /// Total ExprToken count for the single pooled array.
        /// Layout: instrBuf(128) + tuplePoolBuf(64) + tokensBuf(128) + opsStackBuf(128)
        ///       + runtimePoolBuf(64) + extractedFields(32) + stackBuf(16) = 560
        /// </summary>
        private const int TotalPoolTokens = MaxInstructions + MaxTuplePool + MaxInstructions + MaxInstructions
                                          + MaxRuntimePool + MaxSelectors + StackCapacity;

        /// <summary>
        /// Apply post-filtering to vector search results using a compiled filter expression.
        ///
        /// <para><b>Output format</b></para>
        /// <para>
        /// The <paramref name="filterBitmap"/> is a packed bit array — one bit per result.
        /// bit i = 1 means result i passed the filter. Caller tests with:
        ///   <c>(filterBitmap[i &gt;&gt; 3] &amp; (1 &lt;&lt; (i &amp; 7))) != 0</c>
        /// No in-place compaction — the caller skips non-matching results using the bitmap.
        /// </para>
        /// </summary>
        internal static int ApplyPostFilter(
            ReadOnlySpan<byte> filter,
            int numResults,
            ReadOnlySpan<byte> attributesSpan,
            Span<byte> filterBitmap)
        {
            if (numResults == 0)
                return 0;

            // ── Rent a single contiguous ExprToken array from the pool ─────
            var pool = ArrayPool<ExprToken>.Shared.Rent(TotalPoolTokens);
            var selectorPool = ArrayPool<(int Start, int Length)>.Shared.Rent(MaxSelectors);
            try
            {
                var span = pool.AsSpan(0, TotalPoolTokens);
                var offset = 0;

                var instrBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                var tuplePoolBuf = span.Slice(offset, MaxTuplePool); offset += MaxTuplePool;
                var tokensBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                var opsStackBuf = span.Slice(offset, MaxInstructions); offset += MaxInstructions;
                var runtimePoolBuf = span.Slice(offset, MaxRuntimePool); offset += MaxRuntimePool;
                var extractedFields = span.Slice(offset, MaxSelectors); offset += MaxSelectors;
                var stackBuf = span.Slice(offset, StackCapacity);

                // ── Compile ────────────────────────────────────────────────
                var instrCount = ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
                if (instrCount < 0)
                    return 0;

                // ── Build ExprProgram — references slices of the pooled array ──
                var program = new ExprProgram
                {
                    Instructions = instrBuf[..instrCount],
                    Length = instrCount,
                    TuplePool = tuplePoolBuf[..tupleCount],
                    TuplePoolLength = tupleCount,
                    RuntimePool = runtimePoolBuf,
                    RuntimePoolLength = 0,
                };

                // Clear the bitmap
                filterBitmap.Clear();

                // ── Collect unique selectors (rented separately — different element type) ──
                Span<(int Start, int Length)> selectorBuf = selectorPool.AsSpan(0, MaxSelectors);
                var selectorCount = GetSelectorRanges(program.Instructions, program.Length, filter, selectorBuf);
                var selectorRanges = selectorBuf[..selectorCount];

                // Slice extractedFields to actual selector count
                var fields = extractedFields[..Math.Max(selectorCount, 1)];

                var filteredCount = 0;
                var stack = new ExprStack(stackBuf);
                var remaining = attributesSpan;

                for (var i = 0; i < numResults; i++)
                {
                    var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaining);
                    var attrData = remaining.Slice(sizeof(int), attrLen);

                    program.ResetRuntimePool();

                    AttributeExtractor.ExtractFields(attrData, filter, selectorRanges, fields, ref program);

                    if (ExprRunner.Run(ref program, attrData, filter, selectorRanges, fields, ref stack))
                    {
                        filterBitmap[i >> 3] |= (byte)(1 << (i & 7));
                        filteredCount++;
                    }

                    remaining = remaining[(sizeof(int) + attrLen)..];
                }

                return filteredCount;
            }
            finally
            {
                ArrayPool<ExprToken>.Shared.Return(pool);
                ArrayPool<(int Start, int Length)>.Shared.Return(selectorPool);
            }
        }

        /// <summary>
        /// Extract unique selector byte-ranges from compiled instructions.
        /// Returns the count written into <paramref name="output"/>.
        /// </summary>
        internal static int GetSelectorRanges(
            ReadOnlySpan<ExprToken> instructions,
            int length,
            ReadOnlySpan<byte> filterBytes,
            Span<(int Start, int Length)> output)
        {
            var count = 0;
            for (var i = 0; i < length; i++)
            {
                if (instructions[i].TokenType != ExprTokenType.Selector)
                    continue;

                var start = instructions[i].Utf8Start;
                var len = instructions[i].Utf8Length;
                var span = filterBytes.Slice(start, len);
                var found = false;
                for (var j = 0; j < count; j++)
                {
                    if (filterBytes.Slice(output[j].Start, output[j].Length).SequenceEqual(span))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found && count < output.Length)
                    output[count++] = (start, len);
            }
            return count;
        }
    }
}