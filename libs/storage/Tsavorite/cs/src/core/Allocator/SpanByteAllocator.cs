// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Allocator for ReadOnlySpan{byte} Keys and Span{byte} Values.
    /// </summary>
    public struct SpanByteAllocator<TStoreFunctions> : IAllocator<TStoreFunctions>
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly SpanByteAllocatorImpl<TStoreFunctions> _this;

        public SpanByteAllocator(AllocatorSettings settings, TStoreFunctions storeFunctions)
        {
            // Called by TsavoriteKV via allocatorCreator; must pass a wrapperCreator to AllocatorBase
            _this = new(settings, storeFunctions, @this => new SpanByteAllocator<TStoreFunctions>(@this));
        }

        internal SpanByteAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (SpanByteAllocatorImpl<TStoreFunctions>)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<TStoreFunctions>
            => (AllocatorBase<TStoreFunctions, TAllocator>)(object)_this;

        /// <inheritdoc/>
        public readonly bool HasObjectLog => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord(ReadOnlySpan<byte> key, long logicalAddress, in RecordSizeInfo sizeInfo, ref LogRecord newLogRecord)
            => newLogRecord.InitializeRecord(key, in sizeInfo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(in TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
             => _this.GetRMWCopyRecordSize(in srcLogRecord, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => _this.GetRMWInitialRecordSize(key, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => _this.GetUpsertRecordSize(key, value, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => _this.GetUpsertRecordSize(key, value, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => _this.GetUpsertRecordSize(key, in inputLogRecord, ref input, varlenInput);

        /// <summary>Get record size required for a new tombstone record</summary>
        public readonly RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key) => _this.GetDeleteRecordSize(key);

        /// <inheritdoc/>
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => _this.PopulateRecordSizeInfo(ref sizeInfo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void MarkPage(long logicalAddress, long version) => _this.MarkPage(logicalAddress, version);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void MarkPageAtomic(long logicalAddress, long version) => _this.MarkPageAtomic(logicalAddress, version);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void ClearPage(long page, int offset = 0) => _this.ClearPage(page, offset);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void FreePage(long pageIndex) => _this.FreePage(pageIndex);

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress) => _this.CreateLogRecord(logicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => _this.CreateLogRecord(logicalAddress, physicalAddress);

        /// <inheritdoc/>
        public readonly LogRecord CreateRemappedLogRecordOverPinnedTransientMemory(long logicalAddress, long physicalAddress) => _this.CreateLogRecord(logicalAddress, physicalAddress);

        /// <inheritdoc/>
        public readonly ObjectIdMap TransientObjectIdMap => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason) => _this.DisposeRecord(ref logRecord, disposeReason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => _this.DisposeRecord(ref logRecord, disposeReason);
    }
}