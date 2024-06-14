// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper for inlining. This contains the performance-critical methods that must be inlined;
    /// abstract/virtual methods may be called via <see cref="AllocatorBase{Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions, TAllocatorCallbacks}"/>.
    /// </summary>
    public interface IAllocator<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions> : IAllocatorCallbacks<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions>
        where TKeyComparer : IKeyComparer<Key>
        where TKeySerializer : IObjectSerializer<Key>
        where TValueSerializer : IObjectSerializer<Value>
        where TRecordDisposer : IRecordDisposer<Key, Value>
        where TStoreFunctions : IStoreFunctions<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
    {
        /// <summary>The <see cref="IStoreFunctions{Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer}"/> implementation instance for this allocator instance</summary>
        TStoreFunctions StoreFunctions { get; }

        /// <summary>The base class instance of the allocator implementation</summary>
        AllocatorBase<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions, TAllocator> GetBase<TAllocator>()
            where TAllocator : IAllocator<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions>;

        /// <summary>Initialize</summary>
        void Initialize();

        /// <summary>Cast address range to <typeparamref name="Value"/>. For <see cref="SpanByte"/> this will also initialize the value to span the address range.</summary>
        ref Value GetAndInitializeValue(long physicalAddress, long endPhysicalAddress);

        /// <summary>Get copy destination size for RMW, taking Input into account</summary>
        (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<Input, TVariableLengthInput>(ref Key key, ref Input input, ref Value value, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<Value, Input>;

        /// <summary>Get initial record size for RMW, given the <paramref name="key"/> and <paramref name="input"/></summary>
        (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<Input, TSessionFunctionsWrapper>(ref Key key, ref Input input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<Value, Input>;

        /// <summary>Get record size required for the given <paramref name="key"/> and <paramref name="value"/></summary>
        (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref Key key, ref Value value);

        /// <summary>Get the size of the given <paramref name="value"/></summary>
        int GetValueLength(ref Value value);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty</summary>
        void MarkPage(long logicalAddress, long version);

        /// <summary>Mark the page that contains <paramref name="logicalAddress"/> as dirty atomically</summary>
        void MarkPageAtomic(long logicalAddress, long version);

        /// <summary>Get segment offsets</summary>
        long[] GetSegmentOffsets();

        /// <summary>Serialize key to log</summary>
        void SerializeKey(ref Key key, long physicalAddress);
    }
}