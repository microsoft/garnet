// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.common.Parsing;

namespace Garnet.server
{
    unsafe struct SessionParseState
    {
        const int MinParams = 5;
        int count;
        ArgSlice[] buffer;
        ArgSlice* bufferPtr;

        public void Initialize()
        {
            count = 0;
            buffer = GC.AllocateArray<ArgSlice>(MinParams, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(int count)
        {
            this.count = count;

            if (count <= MinParams || count <= buffer.Length)
                return;

            buffer = GC.AllocateArray<ArgSlice>(count, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int i, ref byte* ptr, byte* end)
        {
            ref var slice = ref Unsafe.AsRef<ArgSlice>(bufferPtr + i);

            // Parse RESP string header
            if (!RespReadUtils.ReadUnsignedLengthHeader(out slice.length, ref ptr, end))
            {
                return false;
            }

            slice.ptr = ptr;

            // Parse content: ensure that input contains key + '\r\n'
            ptr += slice.length + 2;
            if (ptr > end)
            {
                return false;
            }

            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ArgSlice GetByRef(int i)
            => ref Unsafe.AsRef<ArgSlice>(bufferPtr + i);
    }
}