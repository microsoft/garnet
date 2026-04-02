// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore
{
    public struct AdId : IKey
    {
        public const int Size = sizeof(long);

        public long adId;

        // Not always pinned, so don't act like it is.
        public readonly bool IsPinned => false;

        [UnscopedRef]
        public readonly ReadOnlySpan<byte> KeyBytes => MemoryMarshal.AsBytes<long>(new(in adId));

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];

        public override string ToString() => adId.ToString();

        public struct Comparer : IKeyComparer
        {
            public long GetHashCode64<TKey>(TKey key)
                where TKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                => Utility.GetHashCode(key.KeyBytes.AsRef<AdId>().adId);

            public bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
                where TFirstKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                where TSecondKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                => k1.KeyBytes.AsRef<AdId>().adId == k2.KeyBytes.AsRef<AdId>().adId;
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

        public override bool InPlaceUpdater(ref LogRecord logRecord, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
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
        public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref AdInput input)
            => new() { KeySize = key.KeyBytes.Length, ValueSize = NumClicks.Size, ValueIsObject = false };
    }
}