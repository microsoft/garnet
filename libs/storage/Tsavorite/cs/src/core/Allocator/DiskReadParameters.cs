// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    internal unsafe partial class DiskStreamReader<TStoreFunctions> where TStoreFunctions : IStoreFunctions
    {
        /// <summary>
        /// Information for the IO read operation.
        /// </summary>
        internal readonly struct DiskReadParameters
        {
            /// <summary>The <see cref="SectorAlignedBufferPool"/> for <see cref="SectorAlignedMemory"/> buffer allocations</summary>
            internal readonly SectorAlignedBufferPool bufferPool;

            /// <summary>For <see cref="SpanByteAllocator{TStoreFunctions}"/>, the fixed page size for buffer allocation</summary>
            internal readonly int fixedPageSize;

            /// <summary>The threshold at which a key becomes overflow</summary>
            internal readonly int maxInlineKeyLength;

            /// <summary>The threshold at which a value becomes overflow</summary>
            internal readonly int maxInlineValueLength;

            /// <summary>The sector size of the device; will be either 512b or 4kb</summary>
            internal readonly uint sectorSize;

            /// <summary>The page buffer size; current a global constant but if it becomes customizable it will have to be read.</summary>
            internal readonly int pageBufferSize;

            /// <summary>The amount of data space on the disk page between the header and footer</summary>
            internal int UsablePageSize => pageBufferSize - DiskPageHeader.Size - DiskPageFooter.Size;

            /// <summary>The last usable position on the page.</summary>
            internal int PageEndPosition => pageBufferSize - DiskPageFooter.Size;

            /// <summary>The record address (without AddressType), i.e. the sector-unaligned (but record-aligned) start of the record on the device</summary>
            internal readonly long recordAddress;

            /// <summary>The <see cref="IStoreFunctions"/> implementation to use</summary>
            internal readonly TStoreFunctions storeFunctions;

            /// <summary>Constructor used for SpanByteAllocator, which has a fixed page size and no overflow</summary>
            internal DiskReadParameters(SectorAlignedBufferPool bufferPool, int fixedPageSize, uint sectorSize, int pageBufferSize, long recordAddress, TStoreFunctions storeFunctions)
            {
                this.bufferPool = bufferPool ?? throw new ArgumentNullException(nameof(bufferPool));
                this.fixedPageSize = fixedPageSize;
                maxInlineKeyLength = LogSettings.kMaxInlineKeySize;     // Note: We throw an exception if it exceeds this but include it here for consistency
                maxInlineValueLength = -1;
                this.sectorSize = sectorSize;
                this.pageBufferSize = pageBufferSize;
                this.recordAddress = recordAddress;
                this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
            }

            /// <summary>Constructor used for ObjectAllocator, which has no fixed pages size and may have overflow and objects</summary>
            internal DiskReadParameters(SectorAlignedBufferPool bufferPool, int maxInlineKeyLength, int maxInlineValueLength, uint sectorSize, int pageBufferSize, long recordAddress, TStoreFunctions storeFunctions)
            {
                this.bufferPool = bufferPool ?? throw new ArgumentNullException(nameof(bufferPool));
                fixedPageSize = -1;
                this.maxInlineKeyLength = maxInlineKeyLength > IStreamBuffer.DiskReadForceOverflowSize ? IStreamBuffer.DiskReadForceOverflowSize : maxInlineKeyLength;
                this.maxInlineValueLength = maxInlineValueLength > IStreamBuffer.DiskReadForceOverflowSize ? IStreamBuffer.DiskReadForceOverflowSize : maxInlineValueLength;
                this.sectorSize = sectorSize;
                this.pageBufferSize = pageBufferSize;
                this.recordAddress = recordAddress;
                this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
            }

            internal readonly (long alignedFieldOffset, int padding) GetAlignedReadStart(long unalignedFieldOffset)
            {
                var alignedFieldOffset = RoundDown(recordAddress + unalignedFieldOffset, (int)sectorSize);
                return (alignedFieldOffset, (int)(recordAddress + unalignedFieldOffset - alignedFieldOffset));
            }

            internal readonly (int alignedBytesToRead, int padding) GetAlignedBytesToRead(int unalignedBytesToRead)
            {
                var alignedBytesToRead = RoundUp(unalignedBytesToRead, (int)sectorSize);
                return (alignedBytesToRead, alignedBytesToRead - unalignedBytesToRead);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal int CalculateOffsetDistanceFromEndOfPage(long offset) => (int)(recordAddress + offset) & (pageBufferSize - 1);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool CalculatePageBreaks(long length, int distanceFromEndOfPage, out PageBreakInfo pageBreakInfo) 
                => DiskStreamWriter.CalculatePageBreaks(length, distanceFromEndOfPage, UsablePageSize, out pageBreakInfo);
        }
    }
}