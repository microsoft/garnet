// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // This is unused; just allows things to build. TsavoriteLog does not do key comparisons or value operations; it is just a memory allocator
#pragma warning disable IDE0065 // Misplaced using directive
    using TsavoriteLogStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

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
        public readonly bool HasObjectLog => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord<TKey>(TKey key, long logicalAddress, in RecordSizeInfo _, ref LogRecord newLogRecord)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(in TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
              => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetRMWInitialRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetUpsertRecordSize<TKey, TSourceLogRecord, TInput, TVariableLengthInput>(TKey key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordSizeInfo GetDeleteRecordSize<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void AllocatePage(int pageIndex) => _this.AllocatePage(pageIndex);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void FreePage(long pageIndex) => _this.FreePage(pageIndex);

        /// <inheritdoc/>
        public readonly int OverflowPageCount => _this.OverflowPageCount;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        public readonly LogRecord CreateRemappedLogRecordOverPinnedTransientMemory(long logicalAddress, long physicalAddress) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        public readonly ObjectIdMap TransientObjectIdMap => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDispose(ref LogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => throw new NotImplementedException("Not implemented for TsavoriteLogAllocator");

        /// <inheritdoc/>
        public void EvictRecordsInRange(long startAddress, long endAddress, EvictionSource source) { }
    }
}