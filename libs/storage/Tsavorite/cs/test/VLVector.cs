// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    // Extension class for SpanByte to wrap a non-byte Span
    internal static unsafe class VLVector
    {
        // Wrap a SpanByte around a Span of non-byte type, e.g.:
        //      Span<int> valueSpan = stackalloc int[numElem];
        //      for (var ii = 0; ii < numElem; ++ii) valueSpan[ii] = someInt;
        //      var valueSpanByte = valueSpan.AsSpanByte();
        public static PinnedSpanByte FromPinnedSpan<T>(Span<T> span) where T : unmanaged
            => PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<T, byte>(span));

        internal static T[] ToArray<T>(this Span<byte> byteSpan) where T : unmanaged
            => MemoryMarshal.Cast<byte, T>(byteSpan).ToArray();

        internal static T[] ToArray<T>(this ReadOnlySpan<byte> byteSpan) where T : unmanaged
            => MemoryMarshal.Cast<byte, T>(byteSpan).ToArray();
    }

    public class VLVectorFunctions : SessionFunctionsBase<PinnedSpanByte, int[], Empty>
    {
        public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            for (int i = 0; i < output.Length; i++)
                ClassicAssert.AreEqual(output.Length, output[i]);
        }

        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref int[] output, ref ReadInfo readInfo)
        {
            output = srcLogRecord.ValueSpan.ToArray<int>();
            return true;
        }

        // Upsert functions are unchanged from SessionFunctionsBase
    }
}