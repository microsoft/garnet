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
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Input input, in TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
            => dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfoo)
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

        public readonly bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo) { }

        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord
             => GetFieldInfo();

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref Input input) => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref Input input) => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref Input input)
            => new() { KeySize = sizeof(FixedLengthKey), ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

        /// <summary>Length of value object, when populated by Upsert using given log record and input</summary>
        public readonly unsafe RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetUpsertFieldInfo(TSourceLogRecord)");

        static unsafe RecordFieldInfo GetFieldInfo() => new() { KeySize = sizeof(FixedLengthKey), ValueSize = sizeof(FixedLengthValue) };

        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo) { }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo) { }

        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Input input, in TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        { }

        public readonly void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref Input input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor) where TEpochAccessor : IEpochAccessor { }
        public readonly void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref Input input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor) where TEpochAccessor : IEpochAccessor { }
        public readonly void PostRMWOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref Input input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor) where TEpochAccessor : IEpochAccessor { }
        public readonly void PostDeleteOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor) where TEpochAccessor : IEpochAccessor { }

        public readonly void ConvertOutputToHeap(ref Input input, ref Output output) { }
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