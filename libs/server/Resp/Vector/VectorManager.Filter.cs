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
        // All buffers are stackalloc'd on the thread stack (~10 KB total).
        // ExprToken is 16 bytes (explicit layout, blittable).
        //
        // If any limit is exceeded, the filter either fails to compile
        // (returns 0 = no results pass) or the candidate is excluded
        // gracefully.
        //
        //  Buffer           Size     Stack bytes   Limitation
        //  ────────────────  ───────  ───────────   ─────────────────────────────────
        //  instrBuf          128×16   2,048 B       Max compiled postfix instructions.
        //                                           A filter like ".a > 1 and .b < 2"
        //                                           compiles to 7 instructions.
        //                                           128 supports ~18 AND/OR clauses.
        //
        //  tuplePoolBuf       64×16   1,024 B       Max compile-time tuple elements
        //                                           across all IN [...] literals.
        //                                           e.g. ".x in [1,2,...,64]".
        //
        //  tokensBuf         128×16   2,048 B       Compiler scratch: Phase 1 tokens.
        //                                           Reuses MaxInstructions size since
        //                                           every token → at most 1 instruction.
        //
        //  opsStackBuf       128×16   2,048 B       Compiler scratch: shunting-yard
        //                                           operator stack. Bounded by token
        //                                           count (parenthesis nesting depth).
        //
        //  runtimePoolBuf     64×16   1,024 B       Max runtime tuple elements from
        //                                           JSON array extraction (IN operator
        //                                           on JSON array fields). Shared
        //                                           across all candidates, reset each.
        //
        //  selectorBuf        32×8      256 B       Max unique field selectors.
        //                                           e.g. ".a > 1 and .b < 2" has 2.
        //                                           32 supports very complex filters.
        //
        //  extractedFields    32×16     512 B       Pre-extracted field values per
        //                                           candidate. Mirrors selectorBuf.
        //                                           Falls back to ArrayPool if >32.
        //
        //  stackBuf           16×16     256 B       Evaluation stack depth.
        //                                           Postfix eval rarely exceeds 8.
        //                                           16 supports deeply nested exprs.
        //
        //  Total: ~9,216 bytes on stack (safe for server threads with 1 MB stacks).

        /// <summary>Max compiled postfix instructions. Overflow → compile error.</summary>
        private const int MaxInstructions = 128;

        /// <summary>Max compile-time tuple pool elements (all IN [...] literals combined). Overflow → compile error.</summary>
        private const int MaxTuplePool = 64;

        /// <summary>Max runtime tuple pool elements (JSON array extraction). Overflow → array treated as null.</summary>
        private const int MaxRuntimePool = 64;

        /// <summary>Max unique field selectors (e.g. .year, .rating). Overflow → extra selectors silently ignored.</summary>
        private const int MaxSelectors = 32;

        /// <summary>Max selectors before falling back to ArrayPool for extractedFields. Keeps stackalloc safe.</summary>
        private const int MaxStackSelectors = 32;

        /// <summary>Evaluation stack depth. Overflow → TryPush returns false → candidate excluded.</summary>
        private const int StackCapacity = 16;

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

            // ── Compile: all buffers on the stack ─────────────────────────
            Span<ExprToken> instrBuf = stackalloc ExprToken[MaxInstructions];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[MaxTuplePool];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[MaxInstructions];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[MaxInstructions];

            var instrCount = ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
            if (instrCount < 0)
                return 0;

            // ── Build ExprProgram ref struct — references the stackalloc'd spans ──
            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[MaxRuntimePool];
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

            // ── Collect unique selectors into stack span ──────────────────
            Span<(int Start, int Length)> selectorBuf = stackalloc (int, int)[MaxSelectors];
            var selectorCount = GetSelectorRanges(program.Instructions, program.Length, filter, selectorBuf);
            var selectorRanges = selectorBuf[..selectorCount];

            // ── Pre-allocate extraction buffer on the stack ───────────────
            ExprToken[] rentedBuffer = null;
            Span<ExprToken> extractedFields = selectorCount <= MaxStackSelectors
                ? stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1]
                : (rentedBuffer = ArrayPool<ExprToken>.Shared.Rent(selectorCount));

            var filteredCount = 0;

            // ── Evaluation stack on the stack ─────────────────────────────
            Span<ExprToken> stackBuf = stackalloc ExprToken[StackCapacity];
            var stack = new ExprStack(stackBuf);

            var remaining = attributesSpan;

            try
            {
                for (var i = 0; i < numResults; i++)
                {
                    var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaining);
                    var attrData = remaining.Slice(sizeof(int), attrLen);

                    program.ResetRuntimePool();

                    AttributeExtractor.ExtractFields(attrData, filter, selectorRanges, extractedFields, ref program);

                    if (ExprRunner.Run(ref program, attrData, filter, selectorRanges, extractedFields, ref stack))
                    {
                        filterBitmap[i >> 3] |= (byte)(1 << (i & 7));
                        filteredCount++;
                    }

                    remaining = remaining[(sizeof(int) + attrLen)..];
                }
            }
            finally
            {
                if (rentedBuffer != null)
                    ArrayPool<ExprToken>.Shared.Return(rentedBuffer);
            }

            return filteredCount;
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