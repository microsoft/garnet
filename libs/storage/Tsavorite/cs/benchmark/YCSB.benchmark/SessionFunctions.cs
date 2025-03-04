// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public struct SessionFunctions : ISessionFunctions<SpanByte, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void ReadCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            output.value = srcLogRecord.ValueSpan.AsRef<FixedLengthValue>();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref Input input, ref Output output, ref ReadInfo readInfo)
        {
            output.value = logRecord.ValueSpan.AsRef<FixedLengthValue>();
            return true;
        }

        public bool SingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo) => true;

        public bool ConcurrentDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, SpanByte srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>() = srcValue.AsRef<FixedLengthValue>();
            return true;
        }

        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => true; // not used

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, SpanByte srcValue, ref Output output, ref UpsertInfo upsertInfo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>() = srcValue.AsRef<FixedLengthValue>();
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfoo)
        {
            logRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            dstLogRecord.ValueSpan.AsRef<FixedLengthValue>().value = input.value;
            return true;
        }

        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => true;

        public bool NeedInitialUpdate(SpanByte key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public void PostInitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, ref Output output, ref RMWInfo rmwInfo) { }

        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => true;

        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref Input input)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
             => GetFieldInfo();

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref Input input) => GetFieldInfo();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref Input input) => GetFieldInfo();

        static unsafe RecordFieldInfo GetFieldInfo() => new () { KeyDataSize = sizeof(FixedLengthKey), ValueDataSize = sizeof(FixedLengthValue) };

        public void PostSingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo) { }

        public void PostSingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref Input input, SpanByte srcValue, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void ConvertOutputToHeap(ref Input input, ref Output output) { }
    }

    static class StaticUtilities
    {
        public static unsafe ref T AsRef<T>(this SpanByte spanByte) where T : unmanaged
        {
            Debug.Assert(spanByte.Length == Unsafe.SizeOf<T>());
            return ref *(T*)spanByte.ToPointer();
        }
    }
}