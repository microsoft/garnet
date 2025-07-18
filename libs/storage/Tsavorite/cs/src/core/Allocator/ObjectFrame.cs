// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    class ObjectFrame
    {
        private readonly DiskLogRecord[][] frame;
        public readonly int frameSize, pageSize;

        const int InitialRecordCount = 128;

        public ObjectFrame(int frameSize, int pageSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            frame = new DiskLogRecord[frameSize][];
        }

        public void Allocate(int index)
        {
            frame[index] = new DiskLogRecord[InitialRecordCount];
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public ref RecordInfo GetInfo(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].Info;
        }

        public ref DiskLogRecord[] GetPage(long frameNumber)
        {
            return ref frame[frameNumber];
        }

        public void Dispose()
        {
            Array.Clear(frame, 0, frame.Length);
        }
    }
}
