// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // This is unused; just allows things to build. TsavoriteLog does not do key comparisons or value operations; it is just a memory allocator
    using TsavoriteLogStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Struct wrapper (for inlining) around the TsavoriteLogAllocator used by TsavoriteLog.
    /// </summary>
    public struct TsavoriteLogAllocator : IAllocator<TsavoriteLogStoreFunctions>
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly TsavoriteLogAllocatorImpl _this;

        public TsavoriteLogAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (TsavoriteLogAllocatorImpl)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<TsavoriteLogStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<TsavoriteLogStoreFunctions>
            => (AllocatorBase<TsavoriteLogStoreFunctions, TAllocator>)(object)_this;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetStartLogicalAddressOfPage(long page) => _this.GetStartLogicalAddressOfPage(page);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetFirstValidLogicalAddressOnPage(long page) => _this.GetFirstValidLogicalAddressOnPage(page);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetPhysicalAddress(long logicalAddress) => _this.GetPhysicalAddress(logicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeValue(long physicalAddress, in RecordSizeInfo _) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(in TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
              => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsAllocated(int pageIndex) => _this.IsAllocated(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex)
            => TsavoriteLogAllocatorImpl.PopulatePage(src, required_bytes, destinationPageIndex);

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
        public readonly void SerializeKey(ReadOnlySpan<byte> key, long physicalAddress, ref LogRecord logRecord) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");
    }
}