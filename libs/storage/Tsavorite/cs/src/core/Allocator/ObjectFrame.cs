// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    class ObjectFrame
    {
        private SectorAlignedBufferPool bufferPool;

        /// <summary>The number of pages in the frame</summary>
        public readonly int frameSize;
        
        // <summary>The size of the <see cref="recordBuffer"/></summary>
        public readonly int pageSize;

        /// <summary>The frame of individual records for each page, either as direct pointers to inline records in <see cref="pageBuffers"/> or some combination of
        /// that and out-of-line heap allocations (Overflow or Object).</summary>
        /// <remarks>This is a list instead of an Array because we don't know how many </remarks>
        private readonly DiskLogRecord[][] frame;

        /// <summary>
        /// The initial buffer into which page's IO was done. It starts on a record alignment and may contain zero or more complete records followed 
        /// by one partial record (either a single record that won't fit in the buffer, or a partial record following. Buffer size is <see cref="pageSize"/>.
        /// </summary>
        private readonly SectorAlignedMemory[] pageBuffers;

        /// <summary>
        /// Initial capacity for the DiskLogRecord list created during page load
        /// </summary>
        internal const int InitialRecordCount = 128;

        public ObjectFrame(SectorAlignedBufferPool bufferPool, int frameSize, int pageSize)
        {
            this.bufferPool = bufferPool;
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            frame = new DiskLogRecord[frameSize][];
            pageBuffers = new SectorAlignedMemory[frameSize];
        }

        public void Initialize(int pageIndex)
        {
            if (pageBuffers[pageIndex] is null)
            {
                // The frame page (DiskLogRecord[]) is allocated after we actually do the load, because we don't know how many records we will get
                pageBuffers[pageIndex] = bufferPool.Get(pageSize);
                return;
            }
            Clear(pageIndex);
        }

        public void Clear(int pageIndex)
        {
            if (frame[pageIndex] is not null)
            {
                for (var recNum = 0; recNum < frame[pageIndex].Length; recNum++)
                    frame[pageIndex][recNum].Dispose();
                Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
                frame[pageIndex] = null;
            }
            pageBuffers[pageIndex]?.Return();
            pageBuffers[pageIndex] = null;
        }

        public ref DiskLogRecord GetRecord(long pageIndex, long recordIndex) => ref frame[pageIndex][recordIndex];

        public void Dispose()
        {
            for (var pageIndex = 0; pageIndex < frameSize; pageIndex++)
                Clear(pageIndex);
        }
    }
}
