// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper for inlining. This contains the performance-critical methods that must be inlined;
    /// abstract/virtual methods may be called via <see cref="AllocatorBase{TValue, TStoreFunctions, TAllocatorCallbacks}"/>.
    /// </summary>
    public interface IAllocator<TValue, TStoreFunctions> : IAllocatorCallbacks<TValue, TStoreFunctions>
        where TStoreFunctions : IStoreFunctions<TValue>
    {
        /// <summary>The base class instance of the allocator implementation</summary>
        AllocatorBase<TValue, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<TValue, TStoreFunctions>;

        /// <summary>Cast address range to <typeparamref name="TValue"/>. For <see cref="SpanByte"/> this will also initialize the value to span the address range.</summary>
        void InitializeValue(long physicalAddress, long endPhysicalAddress);

        /// <summary>Get copy destination size for RMW, taking Input into account</summary>
        RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get initial record size for RMW, given the <paramref name="key"/> and <paramref name="input"/></summary>
        RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(SpanByte key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/>, <paramref name="value"/>, and <paramref name="input"/></summary>
        RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(SpanByte key, TValue value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TValue, TInput>;

        /// <summary>Get record size required for a new tombstone record</summary>
        RecordSizeInfo GetDeleteRecordSize(SpanByte key);

        /// <summary>Get record size required to allocate a new record. Includes allocator-specific information such as key and value overflow.</summary>
        /// <remarks>Requires <see cref="RecordSizeInfo.FieldInfo"/> to be populated already.</remarks>
        void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty</summary>
        void MarkPage(long logicalAddress, long version);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty atomically</summary>
        void MarkPageAtomic(long logicalAddress, long version);

        /// <summary>Get segment offsets</summary>
        long[] GetSegmentOffsets(); // TODO remove

        /// <summary>Serialize key to log</summary>
        void SerializeKey(SpanByte key, long logicalAddress, ref LogRecord<TValue> logRecord);

        /// <summary>Return the <see cref="LogRecord"/> for the allocator page at <paramref name="logicalAddress"/></summary>
        LogRecord<TValue> CreateLogRecord(long logicalAddress);

        /// <summary>Return the <see cref="LogRecord"/> for the allocator page at <paramref name="physicalAddress"/></summary>
        LogRecord<TValue> CreateLogRecord(long logicalAddress, long physicalAddress);

        /// <summary>Return the initial IO size when reading a record for this allocator</summary>
        int GetInitialRecordIOSize();
    }
}