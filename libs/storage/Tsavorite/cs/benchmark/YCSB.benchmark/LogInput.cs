// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    struct LogInput : IStoreInput
    {
        public SpanByte header;
        public SpanByte state;
        public long arg;

        public LogInput(long arg)
        {
            this.arg = arg;
        }

        public int SerializedLength => header.TotalSize + state.TotalSize + sizeof(long);

        public unsafe int CopyTo(byte* dest, int length)
        {
            var curr = dest;
            header.CopyTo(curr);
            curr += header.TotalSize;

            *(long*)curr = arg;
            curr += sizeof(long);

            state.CopyTo(curr);
            return (int)(curr - dest);
        }

        public unsafe int DeserializeFrom(byte* src) => throw new System.NotImplementedException();
    }
}