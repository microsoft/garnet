// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper for inlining. This contains the performance-critical methods that must be inlined;
    /// abstract/virtual methods may be called via <see cref="AllocatorBase{Key, Value, TStoreFunctions, TAllocatorCallbacks}"/>.
    /// </summary>
    public interface IAllocator<TKey, TValue, TStoreFunctions> : IAllocatorCallbacks<TKey, TValue, TStoreFunctions>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
    {
        /// <summary>The base class instance of the allocator implementation</summary>
        AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>;

        /// <summary>Whether this allocator uses fixed-length records</summary>
        bool IsFixedLength { get; }

        /// <summary>Whether this allocator uses a separate object log</summary>
        bool HasObjectLog { get; }

        /// <summary>Cast address range to <typeparamref name="TValue"/>. For <see cref="SpanByte"/> this will also initialize the value to span the address range.</summary>
        ref TValue GetAndInitializeValue(long physicalAddress, long endPhysicalAddress);

        /// <summary>Get copy destination size for RMW, taking Input into account</summary>
        (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<TInput, TVariableLengthInput>(ref TKey key, ref TInput input, ref TValue value, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get initial record size for RMW, given the <paramref name="key"/> and <paramref name="input"/></summary>
        (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<TInput, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/> and <paramref name="value"/></summary>
        (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref TKey key, ref TValue value);

        /// <summary>Get the size of the given <paramref name="value"/></summary>
        int GetValueLength(ref TValue value);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty</summary>
        void MarkPage(long logicalAddress, long version);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty atomically</summary>
        void MarkPageAtomic(long logicalAddress, long version);

        /// <summary>Get segment offsets</summary>
        long[] GetSegmentOffsets();

        /// <summary>Serialize key to log</summary>
        void SerializeKey(ref TKey key, long physicalAddress);
    }
}