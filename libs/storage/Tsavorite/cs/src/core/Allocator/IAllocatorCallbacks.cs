// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper callbacks for inlining performance-path callbacks from 
    /// <see cref="AllocatorBase{Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions, TAllocatorCallbacks}"/>
    /// to the fully derived allocator, including both record accessors and Scan calls.
    /// </summary>
    public interface IAllocatorCallbacks<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer, TStoreFunctions>
        where TKeyComparer : IKeyComparer<Key>
        where TKeySerializer : IObjectSerializer<Key>
        where TValueSerializer : IObjectSerializer<Value>
        where TRecordDisposer : IRecordDisposer<Key, Value>
        where TStoreFunctions : IStoreFunctions<Key, Value, TKeyComparer, TKeySerializer, TValueSerializer, TRecordDisposer>
    {
        /// <summary>Get start logical address on <paramref name="page"/></summary>
        long GetStartLogicalAddress(long page);

        /// <summary>Get first valid logical address on <paramref name="page"/></summary>
        long GetFirstValidLogicalAddress(long page);

        /// <summary>Get physical address from <paramref name="logicalAddress"/></summary>
        long GetPhysicalAddress(long logicalAddress);

        /// <summary>Get <see cref="RecordInfo"/> from <paramref name="physicalAddress"/></summary>
        ref RecordInfo GetInfo(long physicalAddress);

        /// <summary>Get <see cref="RecordInfo"/> from pinned memory</summary>
        unsafe ref RecordInfo GetInfoFromBytePointer(byte* ptr);

        /// <summary>Get <typeparamref name="Key"/> from <paramref name="physicalAddress"/></summary>
        ref Key GetKey(long physicalAddress);

        /// <summary>Get <typeparamref name="Value"/> from <paramref name="physicalAddress"/></summary>
        ref Value GetValue(long physicalAddress);

        /// <summary>Get the actual (used) and allocated record sizes at <paramref name="physicalAddress"/></summary>
        (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress);

        /// <summary>Get number of bytes required to read the full record that starts at <paramref name="physicalAddress"/> for <paramref name="availableBytes"/>.</summary>
        int GetRequiredRecordSize(long physicalAddress, int availableBytes);

        /// <summary>Get average record size</summary>
        int GetAverageRecordSize();

        /// <summary>Allocate the page in the circular buffer slot at <paramref name="pageIndex"/></summary>
        void AllocatePage(int pageIndex);

        /// <summary>Whether the page at <paramref name="pageIndex"/> is allocated</summary>
        bool IsAllocated(int pageIndex);

        /// <summary>
        /// Populate the page at <paramref name="destinationPageIndex"/> from the <paramref name="src"/> pointer, which has <paramref name="required_bytes"/> bytes.
        /// </summary>
        unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex);

        /// <summary>Free the page at <paramref name="pageIndex"/>, starting at <paramref name="offset"/></summary>
        void ClearPage(long pageIndex, int offset = 0);

        /// <summary>Free the page at <paramref name="pageIndex"/></summary>
        void FreePage(long pageIndex);

        /// <summary>Number of extra overflow pages allocated</summary>
        int OverflowPageCount { get; }

        int GetFixedRecordSize();

        /// <summary>Retrieve key from IO context record</summary>
        ref Key GetContextRecordKey(ref AsyncIOContext<Key, Value> ctx);

        /// <summary>Retrieve value from IO context record</summary>
        ref Value GetContextRecordValue(ref AsyncIOContext<Key, Value> ctx);

        /// <summary>Determine whether we IO has returned the full record</summary>
        unsafe bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx);

        /// <summary>Get heap container for pending key</summary>
        IHeapContainer<Key> GetKeyContainer(ref Key key);

        /// <summary>Get heap container for pending value</summary>
        IHeapContainer<Value> GetValueContainer(ref Value value);
    }
}