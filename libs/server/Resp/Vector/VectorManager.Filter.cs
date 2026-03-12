// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;

namespace Garnet.server
{
    /// <summary>
    /// Filter expression post-processing for vector similarity search results.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Apply post-filtering to vector search results using a compiled filter expression.
        ///
        /// Architecture
        /// 1. The filter string is compiled ONCE into a flat postfix program (ExprCompiler).
        /// 2. Unique selectors (field names) are collected from the program.
        /// 3. For each candidate, ALL needed fields are extracted in a single JSON pass
        ///    via <see cref="AttributeExtractor.ExtractFields"/>, then the program is
        ///    evaluated against the pre-extracted values.
        /// 4. No JsonDocument DOM is allocated — fields are extracted directly from the raw bytes.
        ///
        /// The <paramref name="filterBitmap"/> is populated with one bit per result:
        /// bit i = 1 means result i passed the filter. Caller can test with:
        ///   (filterBitmap[i >> 3] &amp; (1 &lt;&lt; (i &amp; 7))) != 0
        ///
        /// No in-place compaction — the caller skips non-matching results using the bitmap.
        /// </summary>
        private static int ApplyPostFilter(
            ReadOnlySpan<byte> filter,
            int numResults,
            ReadOnlySpan<byte> attributesSpan,
            Span<byte> filterBitmap)
        {
            if (numResults == 0)
            {
                return 0;
            }

            // Compile the filter expression (UTF-8 bytes) into a flat postfix program.
            // This is done once and reused for all candidate evaluations.
            var program = ExprCompiler.TryCompile(filter, out _);
            if (program == null)
            {
                return 0; // If the filter doesn't compile, treat it as filtering out all results
            }

            // Clear the bitmap
            filterBitmap.Clear();

            // Collect unique selectors — byte-ranges into the filter expression.
            var selectorRanges = program.GetSelectorRanges();

            // Pre-allocate extraction buffer — reused across all candidates.
            var extractedFields = new ExprToken[selectorRanges.Length];

            var filteredCount = 0;

            // Allocate the evaluation stack once and reuse it across all candidate evaluations
            var stack = ExprRunner.CreateStack();

            var remaining = attributesSpan;

            for (var i = 0; i < numResults; i++)
            {
                // Read attribute length-prefix + data
                var attrLen = BinaryPrimitives.ReadInt32LittleEndian(remaining);
                var attrData = remaining.Slice(sizeof(int), attrLen);

                // Reset runtime tuple pool for this candidate
                program.ResetRuntimePool();

                // Single-pass extraction: scan JSON once, extract all needed fields.
                AttributeExtractor.ExtractFields(attrData, program.FilterBytes, selectorRanges, extractedFields, program);

                // Execute the compiled program against pre-extracted fields.
                if (ExprRunner.Run(program, attrData, selectorRanges, extractedFields, stack))
                {
                    filterBitmap[i >> 3] |= (byte)(1 << (i & 7));
                    filteredCount++;
                }

                remaining = remaining[(sizeof(int) + attrLen)..];
            }

            return filteredCount;
        }
    }
}
