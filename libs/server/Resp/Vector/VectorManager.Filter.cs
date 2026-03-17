// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

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
        /// When a post-filter is active, request this many times more candidates from DiskANN
        /// to compensate for candidates that will be discarded by the filter.
        /// A single search with this multiplier is tried first; if not enough results pass,
        /// a second search with the retry multiplier is attempted.
        /// </summary>
        private const int FilterOverFetchMultiplier = 5;

        /// <summary>
        /// When the initial over-fetch doesn't yield enough results, retry with this larger multiplier.
        /// </summary>
        private const int FilterOverFetchRetryMultiplier = 10;

        /// <summary>
        /// Maximum number of pages to fetch during paged search to prevent unbounded iteration.
        /// </summary>
        private const int MaxPagedSearchPages = 20;

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
                scratchBufferBuilder.RewindScratchBuffer(ref bufferSlice);
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

        /// <summary>
        /// Perform a filtered similarity search using DiskANN's paged search API.
        /// Fetches pages of results on-demand until enough pass the filter or max pages reached.
        /// Uses the same bitmap-based filtering as the main ApplyPostFilter — no in-place compaction.
        /// </summary>
        private VectorManagerResult PagedFilterSearch(
            ulong context,
            nint indexPtr,
            VectorQuantType quantType,
            int requestedCount,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            bool isVector,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            ReadOnlySpan<byte> element,
            int initialPassingCount = 0)
        {
            var pagedSearchL = searchExplorationFactor;
            var pageSize = pagedSearchL;

            // Allocate page buffers for each page of results from DiskANN
            var pageIdsSize = pageSize * MinimumSpacePerId;
            var pageDistsSize = pageSize * sizeof(float);
            var pageIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageIdsSize), pageIdsSize);
            var pageDists = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageDistsSize), pageDistsSize);

            // Reusable attribute buffer for pages
            var pageAttrsSize = pageSize * 64;
            var pageAttrs = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageAttrsSize), pageAttrsSize);

            // Bitmap for filtering — one bit per page result
            var pageBitmapSize = (pageSize + 7) >> 3;
            var pageBitmap = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(pageBitmapSize), pageBitmapSize);

            // Accumulation buffers for passing results
            var accumIdsSize = requestedCount * MinimumSpacePerId;
            var accumDistsSize = requestedCount * sizeof(float);
            var accumAttrsSize = requestedCount * 64;

            // Ensure output buffers are large enough for the final results
            if (accumIdsSize > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumIdsSize), accumIdsSize);
            }

            if (accumDistsSize > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumDistsSize), accumDistsSize);
            }

            if (includeAttributes && accumAttrsSize > outputAttributes.Length)
            {
                if (!outputAttributes.IsSpanByte)
                {
                    outputAttributes.Memory.Dispose();
                }

                outputAttributes = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(accumAttrsSize), accumAttrsSize);
            }

            nint searchState = 0;
            try
            {
                searchState = isVector
                    ? Service.StartPagedSearchVector(context, indexPtr, valueType, values, pagedSearchL)
                    : Service.StartPagedSearchElement(context, indexPtr, element, pagedSearchL);

                if (searchState == 0)
                {
                    logger?.LogWarning("Failed to start paged search");
                    outputIdFormat = VectorIdFormat.Invalid;
                    return VectorManagerResult.BadParams;
                }

                var totalPassing = initialPassingCount;
                var accumIdPos = 0;
                var accumAttrPos = 0;

                // If we already have results from the static over-fetch, compute where we are in the output buffers
                if (initialPassingCount > 0)
                {
                    var idsSpan = outputIds.AsReadOnlySpan();
                    for (var i = 0; i < initialPassingCount; i++)
                    {
                        var idLen = BinaryPrimitives.ReadInt32LittleEndian(idsSpan[accumIdPos..]);
                        accumIdPos += sizeof(int) + idLen;
                    }

                    if (includeAttributes)
                    {
                        var attrsSpan = outputAttributes.AsReadOnlySpan();
                        for (var i = 0; i < initialPassingCount; i++)
                        {
                            var attrLen = BinaryPrimitives.ReadInt32LittleEndian(attrsSpan[accumAttrPos..]);
                            accumAttrPos += sizeof(int) + attrLen;
                        }
                    }
                }

                for (var page = 0; page < MaxPagedSearchPages; page++)
                {
                    // Reset page buffer lengths for this page
                    pageIds.Length = pageIdsSize;
                    pageDists.Length = pageDistsSize;

                    var pageFound = Service.NextPagedSearchResults(
                        context, indexPtr, searchState, pageSize, pageIds, pageDists);

                    if (pageFound <= 0)
                    {
                        break;
                    }

                    // Reset reusable attribute buffer for this page
                    if (pageAttrs.Memory != null)
                    {
                        pageAttrs.Length = pageAttrs.Memory.Memory.Length;
                    }
                    else
                    {
                        pageAttrs.Length = pageAttrsSize;
                    }

                    FetchVectorElementAttributes(context, pageFound, pageIds, ref pageAttrs);

                    // Ensure bitmap is large enough
                    var requiredBitmapBytes = (pageFound + 7) >> 3;
                    if (requiredBitmapBytes > pageBitmap.Length)
                    {
                        if (!pageBitmap.IsSpanByte)
                        {
                            pageBitmap.Memory.Dispose();
                        }

                        pageBitmap = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(requiredBitmapBytes), requiredBitmapBytes);
                    }

                    // Apply bitmap-based filter — no memory compaction
                    var pagePassCount = ApplyPostFilter(
                        filter, pageFound,
                        pageAttrs.AsReadOnlySpan(), pageBitmap.AsSpan(),
                        ActiveThreadSession.scratchBufferBuilder);

                    if (pagePassCount > 0)
                    {
                        var toTake = Math.Min(pagePassCount, requestedCount - totalPassing);

                        // Walk page results using the bitmap, copy only passing entries
                        AppendPassingResults(
                            pageFound, toTake,
                            pageBitmap.AsReadOnlySpan(),
                            pageIds, pageDists, pageAttrs,
                            ref outputIds, ref outputDistances, ref outputAttributes,
                            includeAttributes,
                            ref accumIdPos, totalPassing, ref accumAttrPos);

                        totalPassing += toTake;
                    }

                    if (totalPassing >= requestedCount)
                    {
                        break;
                    }
                }

                outputIds.Length = accumIdPos;
                outputDistances.Length = totalPassing * sizeof(float);
                if (includeAttributes)
                {
                    outputAttributes.Length = accumAttrPos;
                }

                outputIdFormat = VectorIdFormat.I32LengthPrefixed;

                if (quantType == VectorQuantType.XPreQ8)
                {
                    outputIdFormat = VectorIdFormat.I32LengthPrefixed;
                }

                return VectorManagerResult.OK;
            }
            finally
            {
                if (searchState != 0)
                {
                    Service.DropPagedSearchState(searchState);
                }

                pageIds.Memory?.Dispose();
                pageDists.Memory?.Dispose();
                pageAttrs.Memory?.Dispose();
                pageBitmap.Memory?.Dispose();
            }
        }

        /// <summary>
        /// Walk page results using the filter bitmap and append passing entries
        /// into the accumulation output buffers.
        /// </summary>
        private static void AppendPassingResults(
            int pageFound,
            int toTake,
            ReadOnlySpan<byte> filterBitmap,
            SpanByteAndMemory pageIds,
            SpanByteAndMemory pageDists,
            SpanByteAndMemory pageAttrs,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes,
            bool includeAttributes,
            ref int accumIdPos,
            int accumDistPos,
            ref int accumAttrPos)
        {
            var srcIdSpan = pageIds.AsReadOnlySpan();
            var srcDistSpan = MemoryMarshal.Cast<byte, float>(pageDists.AsReadOnlySpan());
            var srcAttrSpan = pageAttrs.AsReadOnlySpan();

            var srcIdPos = 0;
            var srcAttrPos = 0;
            var taken = 0;

            for (var i = 0; i < pageFound && taken < toTake; i++)
            {
                // Read source ID length
                var idLen = BinaryPrimitives.ReadInt32LittleEndian(srcIdSpan[srcIdPos..]);
                var idTotalLen = sizeof(int) + idLen;

                // Read source attribute length
                var attrLen = BinaryPrimitives.ReadInt32LittleEndian(srcAttrSpan[srcAttrPos..]);
                var attrTotalLen = sizeof(int) + attrLen;

                // Check bitmap — skip if this result didn't pass the filter
                if ((filterBitmap[i >> 3] & (1 << (i & 7))) == 0)
                {
                    srcIdPos += idTotalLen;
                    srcAttrPos += attrTotalLen;
                    continue;
                }

                // Ensure output IDs buffer has space
                if (accumIdPos + idTotalLen > outputIds.Length)
                {
                    var newSize = Math.Max(outputIds.Length * 2, accumIdPos + idTotalLen);
                    var newBuf = MemoryPool<byte>.Shared.Rent(newSize);
                    outputIds.AsReadOnlySpan()[..accumIdPos].CopyTo(newBuf.Memory.Span);
                    outputIds.Memory?.Dispose();
                    outputIds = new SpanByteAndMemory(newBuf, newSize);
                }

                srcIdSpan.Slice(srcIdPos, idTotalLen).CopyTo(outputIds.AsSpan()[accumIdPos..]);
                accumIdPos += idTotalLen;

                // Copy distance
                var destDistSpan = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());
                destDistSpan[accumDistPos + taken] = srcDistSpan[i];

                // Copy attributes if needed
                if (includeAttributes)
                {
                    if (accumAttrPos + attrTotalLen > outputAttributes.Length)
                    {
                        var newSize = Math.Max(outputAttributes.Length * 2, accumAttrPos + attrTotalLen);
                        var newBuf = MemoryPool<byte>.Shared.Rent(newSize);
                        outputAttributes.AsReadOnlySpan()[..accumAttrPos].CopyTo(newBuf.Memory.Span);
                        outputAttributes.Memory?.Dispose();
                        outputAttributes = new SpanByteAndMemory(newBuf, newSize);
                    }

                    srcAttrSpan.Slice(srcAttrPos, attrTotalLen).CopyTo(outputAttributes.AsSpan()[accumAttrPos..]);
                    accumAttrPos += attrTotalLen;
                }

                srcIdPos += idTotalLen;
                srcAttrPos += attrTotalLen;
                taken++;
            }
        }
    }
}