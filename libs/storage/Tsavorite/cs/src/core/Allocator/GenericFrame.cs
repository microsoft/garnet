// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed class GenericFrame<TKey, TValue> : IDisposable
    {
        private readonly AllocatorRecord<TKey, TValue>[][] frame;
        public readonly int frameSize, pageSize;
        private static int RecordSize => Unsafe.SizeOf<AllocatorRecord<TKey, TValue>>();

        public GenericFrame(int frameSize, int pageSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            frame = new AllocatorRecord<TKey, TValue>[frameSize][];
        }

        public void Allocate(int index)
        {
            frame[index] = new AllocatorRecord<TKey, TValue>[(pageSize + RecordSize - 1) / RecordSize];
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public ref TKey GetKey(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].key;
        }

        public ref TValue GetValue(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].value;
        }

        public ref RecordInfo GetInfo(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].info;
        }

        public ref AllocatorRecord<TKey, TValue>[] GetPage(long frameNumber)
        {
            return ref frame[frameNumber];
        }

        public void Dispose()
        {
            Array.Clear(frame, 0, frame.Length);
        }
    }
}