// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using System.Runtime.CompilerServices;

namespace TemplateStuff
{
    public unsafe class SpanVsPointer
    {
        const int Count = 1024;

        byte[] bytes;
        byte* bytesPtr;

        [GlobalSetup]
        public void GlobalSetup()
        {
            bytes = GC.AllocateArray<byte>(Count, true);
            bytesPtr = (byte*)Unsafe.AsPointer(ref bytes[0]);
            for (var ii = 0; ii < Count; ++ii)
                bytes[ii] = (byte)ii;
        }

        [BenchmarkCategory("Swap"), Benchmark(Baseline = true)]
        public void Pointer()
        {
            byte* pointer = bytesPtr;
            byte* end = pointer + Count - 1;
            while (pointer < end)
            {
                var tmp = *pointer;
                *pointer = *++pointer;
                *pointer = tmp;
            }
        }

        [BenchmarkCategory("Swap"), Benchmark]
        public void Span()
        {
            var span = new Span<byte>(bytesPtr, Count);
            int i = 0;
            while (i < Count - 1)
            {
                var tmp = span[i];
                span[i] = span[++i];
                span[i] = tmp;
            }
        }
            
        [BenchmarkCategory("Swap"), Benchmark]
        public void SpanToPointer()
        {
            var span = new Span<byte>(bytesPtr, Count);
            fixed (byte* ptr = span)
            {
                byte* pointer = ptr;
                byte* end = pointer + Count - 1;
                while (pointer < end)
                {
                    var tmp = *pointer;
                    *pointer = *++pointer;
                    *pointer = tmp;
                }
            }
        }
    }
}
