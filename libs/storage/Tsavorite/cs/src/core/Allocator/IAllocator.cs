// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper for inlining. This contains the performance-critical methods that must be inlined;
    /// abstract/virtual methods may be called via <see cref="AllocatorBase{TStoreFunctions, TAllocatorCallbacks}"/>.
    /// </summary>
    public interface IAllocator<TStoreFunctions> : IAllocatorCallbacks<TStoreFunctions>
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>The base class instance of the allocator implementation</summary>
        AllocatorBase<TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<TStoreFunctions>;

        /// <summary>Initialize the value to span the address range.</summary>
        /// <param name="physicalAddress">The start of the record (address of its <see cref="RecordInfo"/>).</param>
        /// <param name="sizeInfo">The record size info, which tells us the value size and whether that is overflow.</param>
        void InitializeValue(long physicalAddress, in RecordSizeInfo sizeInfo);

        /// <summary>Get copy destination size for RMW, taking Input into account</summary>
        RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(in TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>;

        /// <summary>Get initial record size for RMW, given the <paramref name="key"/> and <paramref name="input"/></summary>
        RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/>, <paramref name="value"/>, and <paramref name="input"/></summary>
        RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/>, <paramref name="value"/>, and <paramref name="input"/></summary>
        RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>;

        /// <summary>Get record size required for the given <paramref name="key"/>, <paramref name="inputLogRecord"/>, and <paramref name="input"/></summary>
        RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>;

        /// <summary>Get record size required for a new tombstone record</summary>
        RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key);

        /// <summary>Get record size required to allocate a new record. Includes allocator-specific information such as key and value overflow.</summary>
        /// <remarks>Requires <see cref="RecordSizeInfo.FieldInfo"/> to be populated already.</remarks>
        void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty</summary>
        void MarkPage(long logicalAddress, long version);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty atomically</summary>
        void MarkPageAtomic(long logicalAddress, long version);

        /// <summary>Serialize key to log</summary>
        void SerializeKey(ReadOnlySpan<byte> key, long logicalAddress, ref LogRecord logRecord);

        /// <summary>Return the <see cref="LogRecord"/> for the allocator page at <paramref name="logicalAddress"/></summary>
        LogRecord CreateLogRecord(long logicalAddress);

        /// <summary>Return the <see cref="LogRecord"/> for the allocator page at <paramref name="physicalAddress"/></summary>
        LogRecord CreateLogRecord(long logicalAddress, long physicalAddress);

        /// <summary>Dispose an in-memory log record</summary>
        void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason);

        /// <summary>Dispose an on-disk log record</summary>
        void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason);
    }
}