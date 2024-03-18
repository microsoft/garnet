// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Lightweight iterator for memory page (copied to buffer). GetNext() can be used outside epoch protection and locking,
    /// but ctor must be called within epoch protection.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    class MemoryPageScanIterator<Key, Value> : ITsavoriteScanIterator<Key, Value>
    {
        readonly Record<Key, Value>[] page;
        readonly long pageStartAddress;
        readonly int recordSize;
        readonly int start, end;
        int offset;

        public MemoryPageScanIterator(Record<Key, Value>[] page, int start, int end, long pageStartAddress, int recordSize)
        {
            this.page = new Record<Key, Value>[page.Length];
            Array.Copy(page, start, this.page, start, end - start);
            offset = start - 1;
            this.start = start;
            this.end = end;
            this.pageStartAddress = pageStartAddress;
            this.recordSize = recordSize;
        }

        public long CurrentAddress => pageStartAddress + offset * recordSize;

        public long NextAddress => pageStartAddress + (offset + 1) * recordSize;

        public long BeginAddress => pageStartAddress + start * recordSize;

        public long EndAddress => pageStartAddress + end * recordSize;

        public void Dispose()
        {
        }

        public ref Key GetKey() => ref page[offset].key;
        public ref Value GetValue() => ref page[offset].value;

        public bool GetNext(out RecordInfo recordInfo)
        {
            while (true)
            {
                offset++;
                if (offset >= end)
                {
                    recordInfo = default;
                    return false;
                }
                if (!page[offset].info.Invalid)
                    break;
            }

            recordInfo = page[offset].info;
            return true;
        }

        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            var r = GetNext(out recordInfo);
            if (r)
            {
                key = page[offset].key;
                value = page[offset].value;
            }
            else
            {
                key = default;
                value = default;
            }
            return r;
        }

        /// <inheritdoc/>
        public override string ToString() => $"BA {BeginAddress}, EA {EndAddress}, CA {CurrentAddress}, NA {NextAddress}, start {start}, end {end}, recSize {recordSize}, pageSA {pageStartAddress}";
    }
}