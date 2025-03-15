// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // This is unused; just allows things to build. TsavoriteAof does not do key comparisons or value operations; it is just a memory allocator
    using AofStoreFunctions = StoreFunctions<AofValue, SpanByteComparer, DefaultRecordDisposer<AofValue>>;

    /// <summary>
    /// Struct wrapper (for inlining) around the AofAllocator used by TsavoriteAof.
    /// </summary>
    public struct AofAllocator : IAllocator<AofValue, AofStoreFunctions>
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly AofAllocatorImpl _this;

        public AofAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (AofAllocatorImpl)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<AofValue, AofStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<AofValue, AofStoreFunctions>
            => (AllocatorBase<AofValue, AofStoreFunctions, TAllocator>)(object)_this;

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
            where TSourceLogRecord : ISourceLogRecord<AofValue>
            where TVariableLengthInput : IVariableLengthInput<AofValue, TInput>
              => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(SpanByte key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<AofValue, TInput>
            => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(SpanByte key, AofValue value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<AofValue, TInput>
            => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetDeleteRecordSize(SpanByte key) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void DeserializeValueObject(ref DiskLogRecord<AofValue> diskLogRecord, ref AsyncIOContext<AofValue> ctx) { }

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
        public readonly ref SpanByte GetContextRecordKey(ref AsyncIOContext<AofValue> ctx) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        public readonly ref AofValue GetContextRecordValue(ref AsyncIOContext<AofValue> ctx) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<SpanByte> GetKeyContainer(SpanByte key) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<AofValue> GetValueContainer(AofValue value) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long[] GetSegmentOffsets() => throw new NotImplementedException("Not implemented for AofAllocator");    // TODO remove all the SegmentOffset stuff

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SerializeKey(SpanByte key, long physicalAddress, ref LogRecord<AofValue> logRecord) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<AofValue> CreateLogRecord(long logicalAddress) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<AofValue> CreateLogRecord(long logicalAddress, long physicalAddress) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        public readonly int GetInitialRecordIOSize() => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref LogRecord<AofValue> logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for AofAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(ref DiskLogRecord<AofValue> logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for AofAllocator");
    }
}