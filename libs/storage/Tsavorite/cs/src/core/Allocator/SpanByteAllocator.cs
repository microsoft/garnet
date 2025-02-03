// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // Allocator for SpanByte Keys and Values.
    public struct SpanByteAllocator<TStoreFunctions> : IAllocator<SpanByte, TStoreFunctions>
        where TStoreFunctions : IStoreFunctions<SpanByte>
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
        public readonly AllocatorBase<SpanByte, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<SpanByte, TStoreFunctions>
            => (AllocatorBase<SpanByte, TStoreFunctions, TAllocator>)(object)_this;

        /// <inheritdoc/>
        public readonly bool IsFixedLength => false;

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
        public readonly ref RecordInfo GetInfoRef(long physicalAddress) => ref LogRecord<SpanByte>.GetInfoRef(physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe ref RecordInfo GetInfoRefFromBytePointer(byte* ptr) => ref *(RecordInfo*)ptr;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly SpanByte GetKey(long physicalAddress) => SpanByteAllocatorImpl<TStoreFunctions>.GetKey(physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeValue(long physicalAddress, int valueTotalSize) => _this.InitializeValue(physicalAddress, valueTotalSize);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly (int actualSize, int allocatedSize) GetInlineRecordSizes(long physicalAddress) => new LogRecord<SpanByte>(physicalAddress).GetInlineRecordSizes();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
             => _this.GetRMWCopyRecordSize(ref srcLogRecord, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(SpanByte key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
            => _this.GetRMWInitialRecordSize(key, ref input, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(SpanByte key, SpanByte value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
            => _this.GetUpsertRecordSize(key, value, ref input, varlenInput);

        /// <summary>Get record size required for a new tombstone record</summary>
        public readonly RecordSizeInfo GetDeleteRecordSize(SpanByte key) => _this.GetDeleteRecordSize(key);

        /// <inheritdoc/>
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => _this.PopulateRecordSizeInfo(ref sizeInfo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void DeserializeValue(ref DiskLogRecord<SpanByte> diskLogRecord, ref AsyncIOContext<SpanByte> ctx) { }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsAllocated(int pageIndex) => _this.IsAllocated(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex) => _this.PopulatePage(src, required_bytes, destinationPageIndex);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<SpanByte> GetKeyContainer(SpanByte key) => _this.GetKeyContainer(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<SpanByte> GetValueContainer(SpanByte value) => _this.GetValueContainer(ref value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long[] GetSegmentOffsets()
            => SpanByteAllocatorImpl<TStoreFunctions>.GetSegmentOffsets();

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SerializeKey(SpanByte key, long logicalAddress, ref LogRecord<SpanByte> logRecord) => _this.SerializeKey(key, logicalAddress, ref logRecord);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<SpanByte> CreateLogRecord(long logicalAddress) => _this.CreateLogRecord(logicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<SpanByte> CreateLogRecord(long logicalAddress, long physicalAddress) => _this.CreateLogRecord(logicalAddress, physicalAddress);

        /// <inheritdoc/>
        public readonly int GetInitialRecordIOSize() => RecordInfo.GetLength()
            + (1 << LogSettings.kMaxInlineKeySizeBits) * 2  // double to include value as well
            + sizeof(long) * 2;                             // ETag and Expiration
    }
}