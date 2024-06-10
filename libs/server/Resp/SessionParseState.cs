// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    unsafe struct SessionParseState
    {
        const int SpanByteLength = 12;
        const int MinParams = 5;
        const int MaxParams = 1 << 13;
        int count;
        SpanByte[] buffer;
        SpanByte* bufferPtr;

        public void Initialize()
        {
            count = 0;
            buffer = GC.AllocateArray<SpanByte>(MinParams, true);
            bufferPtr = (SpanByte*)Unsafe.AsPointer(ref buffer[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(int count)
        {
            this.count = count;

            if (count <= MinParams || count <= buffer.Length)
                return;

            buffer = GC.AllocateArray<SpanByte>(count, true);
            bufferPtr = (SpanByte*)Unsafe.AsPointer(ref buffer[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte Get(int i)
            => ref buffer[i];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int offset, int length, byte* data)
        {
            Debug.Assert(offset < count);
            buffer[offset] = new SpanByte(length, (nint)data);
        }
    }
}