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

        /// <summary>Cast address range to <typeparamref name="TValue"/>. For <see cref="SpanByte"/> this will also initialize the value to span the address range.</summary>
        void InitializeValue(long physicalAddress, long endPhysicalAddress);

        /// <summary>Get copy destination size for RMW, taking Input into account</summary>
        RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : IReadOnlyLogRecord
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get initial record size for RMW, given the <paramref name="key"/> and <paramref name="input"/></summary>
        RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(TKey key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/>, <paramref name="value"/>, and <paramref name="input"/></summary>
        RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(TKey key, ref TValue value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/> and <paramref name="value"/></summary>
        (int actualSize, int allocatedSize, int keySize) GetRecordSize(TKey key, ref TValue value);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty</summary>
        void MarkPage(long logicalAddress, long version);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty atomically</summary>
        void MarkPageAtomic(long logicalAddress, long version);

        /// <summary>Get segment offsets</summary>
        long[] GetSegmentOffsets(); // TODO remove

        /// <summary>Serialize key to log</summary>
        void SerializeKey(ref TKey key, long physicalAddress);  // TODO remove

        /// <summary>Return the <see cref="LogRecord"/> for the allocator page at <paramref name="physicalAddress"/></summary>
        LogRecord CreateLogRecord(long logicalAddress, long physicalAddress);   // TODO remove in favor of SpanByteAllocator- or ObjectAllocator-specific call

        /// <summary>Return the <see cref="OverflowAllocator"/> for the in-memory page containing <paramref name="logicalAddress"/></summary>
        OverflowAllocator GetOverflowAllocator(long logicalAddress);
    }
}