// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // Allocator for SpanByte Keys and Values.
    public struct SpanByteAllocator<TStoreFunctions> : IAllocator<SpanByte, SpanByte, TStoreFunctions>
        where TStoreFunctions : IStoreFunctions<SpanByte, SpanByte>
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly SpanByteAllocatorImpl<TStoreFunctions> _this;

        public SpanByteAllocator(AllocatorSettings settings, TStoreFunctions storeFunctions)
        {
            // Called by TsavoriteKV via allocatorCreator; must pass a wrapperCreator to AllocatorBase
            _this = new(settings, storeFunctions, @this => new SpanByteAllocator<TStoreFunctions>(@this));
        }

        public SpanByteAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (SpanByteAllocatorImpl<TStoreFunctions>)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<SpanByte, SpanByte, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<SpanByte, SpanByte, TStoreFunctions>
            => (AllocatorBase<SpanByte, SpanByte, TStoreFunctions, TAllocator>)(object)_this;

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
        public readonly ref RecordInfo GetInfoRef(long physicalAddress) => ref LogRecord.GetInfoRef(physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe ref RecordInfo GetInfoRefFromBytePointer(byte* ptr) => ref *(RecordInfo*)ptr;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly SpanByte GetKey(long physicalAddress) => SpanByteAllocatorImpl<TStoreFunctions>.GetKey(physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeValue(long physicalAddress, long endPhysicalAddress) => _this.InitializeValue(physicalAddress, endPhysicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress) => new LogRecord(physicalAddress).GetFullRecordSizes();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TSourceLogRecord : IReadOnlyLogRecord
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
             => _this.GetRMWCopyRecordSize(ref srcLogRecord, ref input, ref recordInfo, varlenInput);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TInput, TSessionFunctionsWrapper>(ref SpanByte key, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<SpanByte, TInput>
            => _this.GetRMWInitialRecordSize(key, ref input, sessionFunctions);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TInput, TSessionFunctionsWrapper>(ref SpanByte key, ref SpanByte value, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<SpanByte, TInput>
            => _this.GetUpsertRecordSize(key, value, ref input, sessionFunctions);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref SpanByte key, ref SpanByte value) => _this.GetRecordSize(ref key, ref value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe bool RetrievedFullRecord(byte* record, ref AsyncIOContext<SpanByte, SpanByte> ctx)
            => SpanByteAllocatorImpl<TStoreFunctions>.RetrievedFullRecord(record, ref ctx);

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
        public readonly IHeapContainer<SpanByte> GetKeyContainer(ref SpanByte key) => _this.GetKeyContainer(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<SpanByte> GetValueContainer(ref SpanByte value) => _this.GetValueContainer(ref value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long[] GetSegmentOffsets()
            => SpanByteAllocatorImpl<TStoreFunctions>.GetSegmentOffsets();

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SerializeKey(ref SpanByte key, long physicalAddress)
            => SpanByteAllocatorImpl<TStoreFunctions>.SerializeKey(ref key, physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LogRecord CreateLogRecord(long logicalAddress, long physicalAddress)   // TODO: replace in favor of SpanByteAllocator- or ObjectAllocator-specific call
            => _this.CreateLogRecord(logicalAddress, physicalAddress);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OverflowAllocator GetOverflowAllocator(long logicalAddress) => _this.GetOverflowAllocator(logicalAddress);
    }
}