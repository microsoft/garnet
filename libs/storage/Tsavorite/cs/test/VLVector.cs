// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    // Extension class for SpanByte to wrap a non-byte Span
    internal static unsafe class VLVector
    {
        // Wrap a SpanByte around a Span of (usually) non-byte type, e.g.:
        //      Span<int> valueSpan = stackalloc int[numElem];
        //      for (var ii = 0; ii < numElem; ++ii) valueSpan[ii] = someInt;
        //      var valueSpanByte = valueSpan.AsSpanByte();
        public static SpanByte AsSpanByte<T>(this Span<T> span) where T : unmanaged
            => new SpanByte(span.Length * sizeof(T), (IntPtr)Unsafe.AsPointer(ref span[0]));

        public static SpanByte AsSpanByte<T>(this ReadOnlySpan<T> span) where T : unmanaged
        {
            fixed (T* ptr = span)
            {
                return new SpanByte(span.Length * sizeof(T), (IntPtr)ptr);
            }
        }

        public static Span<T> AsSpan<T>(this ref SpanByte sb) where T : unmanaged
            => new Span<T>(sb.MetadataSize + sb.ToPointer(), (sb.Length - sb.MetadataSize) / sizeof(T));

        internal static T[] ToArray<T>(this ref SpanByte spanByte) where T : unmanaged
            => AsSpan<T>(ref spanByte).ToArray();
    }

    public class VLVectorFunctions : SpanByteFunctions<int[], Empty>
    {
        public override void RMWCompletionCallback(ref SpanByte key, ref SpanByte input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            for (int i = 0; i < output.Length; i++)
                Assert.AreEqual(output.Length, output[i]);
        }

        // Read functions
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref int[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToArray<int>();
            return true;
        }

        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref int[] dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst = value.ToArray<int>();
            return true;
        }

        // Upsert functions
        public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref int[] output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref int[] output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            if (src.Length != dst.Length)
                return false;
            return base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo);
        }
    }
}