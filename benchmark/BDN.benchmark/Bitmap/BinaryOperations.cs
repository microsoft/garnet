// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Garnet.server;

namespace BDN.benchmark.Bitmap
{
    public unsafe class BinaryOperations
    {
        private const int Alignment = 64;

        [ParamsSource(nameof(GetBitmapSizes))]
        public Sizes BitmapSizes { get; set; }

        [Params(BitmapOperation.XOR)]
        public BitmapOperation Op { get; set; }

        public IEnumerable<Sizes> GetBitmapSizes()
        {
            yield return new([1 << 21, 1 << 21]);
            yield return new([1 << 21, (1 << 21) + 1]);

            yield return new([1 << 21, 1 << 21, 1 << 21]);
            yield return new([1 << 21, 1 << 21, (1 << 21) + 1]);

            yield return new([256, 6 * 512 + 7, 512, 1024]);
        }

        private int minBitmapSize;
        private byte** srcPtrs;
        private byte** srcEndPtrs;

        private int dstLength;
        private byte* dstPtr;

        [GlobalSetup]
        public void GlobalSetup_Binary()
        {
            minBitmapSize = BitmapSizes.Values.Min();
            srcPtrs = (byte**)NativeMemory.AllocZeroed((nuint)BitmapSizes.Values.Length, (nuint)sizeof(byte*));
            srcEndPtrs = (byte**)NativeMemory.AllocZeroed((nuint)BitmapSizes.Values.Length, (nuint)sizeof(byte*));

            for (var i = 0; i < BitmapSizes.Values.Length; i++)
            {
                srcPtrs[i] = (byte*)NativeMemory.AlignedAlloc((nuint)BitmapSizes.Values[i], Alignment);
                srcEndPtrs[i] = srcPtrs[i] + BitmapSizes.Values[i];

                new Random(i).NextBytes(new Span<byte>(srcPtrs[i], BitmapSizes.Values[i]));
            }

            dstLength = BitmapSizes.Values.Max();
            dstPtr = (byte*)NativeMemory.AlignedAlloc((nuint)dstLength, Alignment);
        }

        [Benchmark]
        public void BinaryOperation()
        {
            BitmapManager.InvokeBitOperationUnsafe(Op, BitmapSizes.Values.Length, srcPtrs, srcEndPtrs, dstPtr, dstLength, minBitmapSize);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            for (var i = 0; i < BitmapSizes.Values.Length; i++)
            {
                NativeMemory.AlignedFree(srcPtrs[i]);
            }

            NativeMemory.Free(srcPtrs);
            NativeMemory.Free(srcEndPtrs);
            NativeMemory.AlignedFree(dstPtr);
        }

        public record struct Sizes(int[] Values)
        {
            public override string ToString() => string.Join(", ", Values);
        }
    }
}