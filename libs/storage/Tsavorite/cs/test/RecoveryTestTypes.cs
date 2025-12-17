// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore
{
    public struct AdId
    {
        public const int Size = sizeof(long);

        public long adId;

        public override string ToString() => adId.ToString();

        public struct Comparer : IKeyComparer
        {
            public long GetHashCode64(ReadOnlySpan<byte> key) => Utility.GetHashCode(key.AsRef<AdId>().adId);

            public bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.AsRef<AdId>().adId == k2.AsRef<AdId>().adId;
        }
    }

    public struct AdInput
    {
        public AdId adId;
        public NumClicks numClicks;

        public override string ToString() => $"id = {adId.adId}, clicks = {numClicks.numClicks}";
    }

    public struct NumClicks
    {
        public const int Size = sizeof(long);
        public long numClicks;

        public override string ToString() => numClicks.ToString();
    }

    public struct Output
    {
        public NumClicks value;

        public override string ToString() => value.ToString();
    }

    public class Functions : SessionFunctionsBase<AdInput, Output, Empty>
    {
        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref AdInput input, ref Output output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueSpan.AsRef<NumClicks>();
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref input.numClicks), in sizeInfo);

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Add(ref logRecord.ValueSpan.AsRef<NumClicks>().numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            dstLogRecord.ValueSpan.AsRef<NumClicks>().numClicks += srcLogRecord.ValueSpan.AsRef<NumClicks>().numClicks + input.numClicks.numClicks;
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref AdInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = NumClicks.Size, ValueIsObject = false };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref AdInput input)
            => new() { KeySize = key.Length, ValueSize = NumClicks.Size, ValueIsObject = false };
    }
}