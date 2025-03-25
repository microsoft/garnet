// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // This is unused; just allows things to build. TsavoriteAof does not do key comparisons or value operations; it is just a memory allocator
    using AofStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Struct wrapper (for inlining) around the AofAllocator used by TsavoriteAof.
    /// </summary>
    public struct AofAllocator : IAllocator<AofStoreFunctions>
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly AofAllocatorImpl _this;

        public AofAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (AofAllocatorImpl)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<AofStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<AofStoreFunctions>
            => (AllocatorBase<AofStoreFunctions, TAllocator>)(object)_this;

        /// <inheritdoc/>
        public readonly bool IsFixedLength => true;

        /// <inheritdoc/>
        public readonly bool HasObjectLog => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetStartLogicalAddress(long page) => _this.GetStartLogicalAddress(page);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetFirstValidLogicalAddress(long page) => _this.GetFirstValidLogicalAddress(page);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetPhysicalAddress(long logicalAddress) => _this.GetPhysicalAddress(logicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeValue(long physicalAddress, ref RecordSizeInfo _) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
              => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void DeserializeValueObject(ref DiskLogRecord diskLogRecord, ref AsyncIOContext ctx) { }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsAllocated(int pageIndex) => _this.IsAllocated(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex)
            => AofAllocatorImpl.PopulatePage(src, required_bytes, destinationPageIndex);

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
        public readonly ref ReadOnlySpan<byte> GetContextRecordKey(ref AsyncIOContext ctx) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        public readonly ref ReadOnlySpan<byte> GetContextRecordValue(ref AsyncIOContext ctx) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long[] GetSegmentOffsets() => throw new NotImplementedException("Not implemented for AofAllocator");    // TODO remove all the SegmentOffset stuff

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SerializeKey(ReadOnlySpan<byte> key, long physicalAddress, ref LogRecord logRecord) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        public readonly int GetInitialRecordIOSize() => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for AofAllocator");
    }
}