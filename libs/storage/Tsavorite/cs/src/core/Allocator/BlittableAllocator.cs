// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Struct wrapper (for inlining) around the fixed-length Blittable allocator.
    /// </summary>
    public struct BlittableAllocator<Key, Value, TStoreFunctions> : IAllocator<Key, Value, TStoreFunctions>
        where TStoreFunctions : IStoreFunctions<Key, Value>
    {
        /// <summary>The wrapped class containing all data and most actual functionality. This must be the ONLY field in this structure so its size is sizeof(IntPtr).</summary>
        private readonly BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>> _this;

        public BlittableAllocator(AllocatorSettings settings, TStoreFunctions storeFunctions)
        {
            // Called by TsavoriteKV via allocatorCreator; must pass a wrapperCreator to AllocatorBase
            _this = new(settings, storeFunctions, @this => new BlittableAllocator<Key, Value, TStoreFunctions>(@this));
        }

        public BlittableAllocator(object @this)
        {
            // Called by AllocatorBase via primary ctor wrapperCreator
            _this = (BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>)@this;
        }

        /// <inheritdoc/>
        public readonly AllocatorBase<Key, Value, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<Key, Value, TStoreFunctions>
            => (AllocatorBase<Key, Value, TStoreFunctions, TAllocator>)(object)_this;

        /// <inheritdoc/>
        public readonly bool IsFixedLength => true;

        /// <inheritdoc/>
        public readonly bool HasObjectLog => false;

        /// <inheritdoc/>
        public readonly long GetStartLogicalAddress(long page) => _this.GetStartLogicalAddress(page);

        /// <inheritdoc/>
        public readonly long GetFirstValidLogicalAddress(long page) => _this.GetFirstValidLogicalAddress(page);

        /// <inheritdoc/>
        public readonly long GetPhysicalAddress(long logicalAddress) => _this.GetPhysicalAddress(logicalAddress);

        /// <inheritdoc/>
        public readonly ref RecordInfo GetInfo(long physicalAddress) 
            => ref BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetInfo(physicalAddress);

        /// <inheritdoc/>
        public readonly unsafe ref RecordInfo GetInfoFromBytePointer(byte* ptr) 
            => ref BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetInfoFromBytePointer(ptr);

        /// <inheritdoc/>
        public readonly ref Key GetKey(long physicalAddress) 
            => ref BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetKey(physicalAddress);

        /// <inheritdoc/>
        public readonly ref Value GetValue(long physicalAddress) 
            => ref BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetValue(physicalAddress);

        /// <inheritdoc/>
        public readonly ref Value GetAndInitializeValue(long physicalAddress, long endPhysicalAddress) => ref GetValue(physicalAddress);

        /// <inheritdoc/>
        public readonly (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetRecordSize(physicalAddress);

        /// <inheritdoc/>
        public readonly (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<Input, TVariableLengthInput>(ref Key key, ref Input input, ref Value value, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<Value, Input>
             => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetRMWCopyDestinationRecordSize(ref key, ref input, ref value, ref recordInfo, varlenInput);

        /// <inheritdoc/>
        public readonly int GetRequiredRecordSize(long physicalAddress, int availableBytes) => GetAverageRecordSize();

        /// <inheritdoc/>
        public readonly int GetAverageRecordSize() 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetAverageRecordSize();

        /// <inheritdoc/>
        public readonly int GetFixedRecordSize() 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetFixedRecordSize();

        /// <inheritdoc/>
        public readonly (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<Input, TSessionFunctionsWrapper>(ref Key key, ref Input input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<Value, Input>
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetRMWInitialRecordSize(ref key, ref input, sessionFunctions);

        /// <inheritdoc/>
        public readonly (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref Key key, ref Value value) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetRecordSize(ref key, ref value);

        /// <inheritdoc/>
        public readonly int GetValueLength(ref Value value) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetValueLength(ref value);

        /// <inheritdoc/>
        public readonly unsafe bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.RetrievedFullRecord(record, ref ctx);

        /// <inheritdoc/>
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        public readonly bool IsAllocated(int pageIndex) => _this.IsAllocated(pageIndex);

        /// <inheritdoc/>
        public readonly unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.PopulatePage(src, required_bytes, destinationPageIndex);

        /// <inheritdoc/>
        public readonly void MarkPage(long logicalAddress, long version) => _this.MarkPage(logicalAddress, version);

        /// <inheritdoc/>
        public readonly void MarkPageAtomic(long logicalAddress, long version) => _this.MarkPageAtomic(logicalAddress, version);

        /// <inheritdoc/>
        public readonly void ClearPage(long page, int offset = 0) => _this.ClearPage(page, offset);

        /// <inheritdoc/>
        public readonly void FreePage(long pageIndex) => _this.FreePage(pageIndex);

        /// <inheritdoc/>
        public readonly ref Key GetContextRecordKey(ref AsyncIOContext<Key, Value> ctx) => ref ctx.key;

        /// <inheritdoc/>
        public readonly ref Value GetContextRecordValue(ref AsyncIOContext<Key, Value> ctx) => ref ctx.value;

        /// <inheritdoc/>
        public readonly IHeapContainer<Key> GetKeyContainer(ref Key key) => new StandardHeapContainer<Key>(ref key);

        /// <inheritdoc/>
        public readonly IHeapContainer<Value> GetValueContainer(ref Value value) => new StandardHeapContainer<Value>(ref value);

        /// <inheritdoc/>
        public readonly long[] GetSegmentOffsets() 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.GetSegmentOffsets();

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        public readonly void SerializeKey(ref Key key, long physicalAddress) 
            => BlittableAllocatorImpl<Key, Value, TStoreFunctions, BlittableAllocator<Key, Value, TStoreFunctions>>.SerializeKey(ref key, physicalAddress);
    }
}