// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.server;
using System.Runtime.InteropServices;

namespace BDN.benchmark.Bitmap
{
    public unsafe partial class UnaryOperations
    {
        private const int Alignment = 64;

        [ParamsSource(nameof(GetBitmapSize))]
        public int BitmapSize { get; set; }

        public IEnumerable<int> GetBitmapSize()
        {
            yield return 256;
            yield return 1 << 21;
        }

        private const int Keys = 1;
        private byte** srcPtrs;
        private byte** srcEndPtrs;

        private byte* dstPtr;

        [GlobalSetup]
        public void GlobalSetup_Binary()
        {
            srcPtrs = (byte**)NativeMemory.AllocZeroed(Keys, (nuint)sizeof(byte*));
            srcEndPtrs = (byte**)NativeMemory.AllocZeroed(Keys, (nuint)sizeof(byte*));

            srcPtrs[0] = (byte*)NativeMemory.AlignedAlloc((uint)BitmapSize, Alignment);
            srcEndPtrs[0] = srcPtrs[0] + (uint)BitmapSize;

            new Random(0).NextBytes(new Span<byte>(srcPtrs[0], BitmapSize));

            dstPtr = (byte*)NativeMemory.AlignedAlloc((nuint)BitmapSize, Alignment);
        }

        [Benchmark]
        public void BitOperation_NOT()
        {
            BitmapManager.InvokeBitOperationUnsafe(BitmapOperation.NOT, Keys, srcPtrs, srcEndPtrs, dstPtr, BitmapSize, BitmapSize);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            NativeMemory.AlignedFree(srcPtrs[0]);

            NativeMemory.Free(srcPtrs);
            NativeMemory.Free(srcEndPtrs);
            NativeMemory.AlignedFree(dstPtr);
        }
    }
}