// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

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
        // All ExprToken buffers are borrowed from the session-local
        // ScratchBufferBuilder via CreateArgSlice, then cast to
        // Span<ExprToken> via MemoryMarshal.Cast. The scratch buffer is a
        // pinned byte[] that persists for the session's lifetime — after the
        // first VSIM FILTER query it's already large enough, so subsequent
        // calls have zero allocation cost.
        //
        // The selectorBuf ((int,int) tuples) is borrowed from the same
        // scratch buffer as a second ArgSlice.
        //
        // Cleanup: RewindScratchBuffer in LIFO order in the finally block.
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
        //  Total: 560 ExprTokens × 16 = 8,960 bytes + 32 selectors × 8 = 256 bytes
        //         = ~9,216 bytes borrowed from session scratch buffer.
        //  Stack: 0 bytes.  Heap: 0 bytes (steady state).

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
            Span<byte> filterBitmap,
            ScratchBufferBuilder scratchBufferBuilder)
        {
            if (numResults == 0)
                return 0;

            // ── Borrow scratch space from the caller-provided ScratchBufferBuilder ──
            // Single CreateArgSlice for both ExprToken and selector buffers.
            // RewindScratchBuffer frees it on exit.
            var bufferSlice = scratchBufferBuilder.CreateArgSlice(
                TotalPoolTokens * ExprToken.Size + MaxSelectors * 2 * sizeof(int));
            var span = MemoryMarshal.Cast<byte, ExprToken>(bufferSlice.Span);
            var selectorBuf = MemoryMarshal.Cast<byte, (int Start, int Length)>(
                bufferSlice.Span.Slice(TotalPoolTokens * ExprToken.Size));

            try
            {
                // Clear the token region (ExprToken.None must be all-zeros)
                span.Clear();

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

                // ── Build ExprProgram — references slices of the scratch buffer ──
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

                // ── Collect unique selectors ──────────────────────────────
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
                scratchBufferBuilder.RewindScratchBuffer(bufferSlice);
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