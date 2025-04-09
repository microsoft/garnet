// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public struct SessionFixedLenFunctions : ISessionFunctions<Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            output.value = srcLogRecord.ValueSpan.AsRef<FixedLengthValue>();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref LogRecord logRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
        {
            output.value = logRecord.ValueSpan.AsRef<FixedLengthValue>();
            return true;
        }

        public bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            logRecord.TrySetValueObject(srcValue, ref sizeInfo);
            return true;
        }

        public bool SingleWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
            => true; // not used

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            logRecord.TrySetValueObject(srcValue, ref sizeInfo);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            return dstLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfoo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            dstLogRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public void PostInitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo) { }

        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord
             => GetFieldInfo();

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref Input input) => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref Input input) => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref Input input) 
            => new() { KeyDataSize = sizeof(FixedLengthKey), ValueDataSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

        /// <summary>Length of value object, when populated by Upsert using given log record and input</summary>
        public unsafe RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TSourceLogRecord inputLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetUpsertFieldInfo(TSourceLogRecord)");

        static unsafe RecordFieldInfo GetFieldInfo() => new () { KeyDataSize = sizeof(FixedLengthKey), ValueDataSize = sizeof(FixedLengthValue) };

        public void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        public void PostSingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ReadOnlySpan<byte> srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void PostSingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, IHeapObject srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void PostSingleWriter<TSourceLogRecord>(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref TSourceLogRecord inputLogRecord, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
            { }

        public void ConvertOutputToHeap(ref Input input, ref Output output) { }
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