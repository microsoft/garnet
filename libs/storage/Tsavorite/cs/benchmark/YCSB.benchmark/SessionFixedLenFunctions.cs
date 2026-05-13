// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable IDE0130 // Namespace does not match folder structure

namespace Tsavorite.benchmark
{
    public struct SessionFixedLenFunctions : ISessionFunctions<Input, Output, Empty>
    {
        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            output.value = srcLogRecord.ValueSpan.AsRef<FixedLengthValue>();
            return true;
        }

        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);

        public readonly bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, in TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true; // not used

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceWriter(ref LogRecord logRecord, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceWriter(ref LogRecord logRecord, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(logRecord, srcValue, ref input) };
            return logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref Input input, in TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(dstLogRecord, inputLogRecord, ref input) };
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref Input input, ref Output output, ref RMWInfo rmwInfoo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            dstLogRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => true;

        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo) { }

        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord
             => GetFieldInfo();

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref Input input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref Input input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly unsafe RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref Input input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => new() { KeySize = sizeof(FixedLengthKey), ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

        /// <summary>Length of value object, when populated by Upsert using given log record and input</summary>
        public readonly unsafe RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord inputLogRecord, ref Input input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetUpsertFieldInfo(TSourceLogRecord)");

        static unsafe RecordFieldInfo GetFieldInfo() => new() { KeySize = sizeof(FixedLengthKey), ValueSize = sizeof(FixedLengthValue) };

        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo) { }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo) { }

        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, in TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        { }

        public readonly void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref Input input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public readonly void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref Input input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public readonly void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref Input input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public readonly void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        public readonly void ConvertOutputToHeap(ref Input input, ref Output output) { }

        public void BeforeConsistentReadCallback(long hash) { }

        public void AfterConsistentReadKeyCallback() { }

        public void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters) { }

        public bool AfterConsistentReadKeyBatchCallback(int keyCount) => true;
    }

    static class StaticUtilities
    {
        public static unsafe ref T AsRef<T>(this Span<byte> spanByte) where T : unmanaged
        {
            Debug.Assert(spanByte.Length == Unsafe.SizeOf<T>());
            return ref Unsafe.As<byte, T>(ref spanByte[0]);
        }

        public static ref readonly T AsRef<T>(this ReadOnlySpan<byte> spanByte) where T : unmanaged
        {
            Debug.Assert(spanByte.Length == Unsafe.SizeOf<T>());
            return ref MemoryMarshal.Cast<byte, T>(spanByte)[0];
        }
    }
}