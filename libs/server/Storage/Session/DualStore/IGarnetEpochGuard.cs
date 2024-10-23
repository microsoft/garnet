// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    // These are the "typedefs" that C# won't let us define, so have to be copied to referencing files. 
    #region Aliases
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;
    #endregion Aliases

    interface IGarnetEpochGuard : IEpochGuard<DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;

    /// <summary>
    /// Perform epoch safety operations when entering/exiting an API 
    /// </summary>
    public struct GarnetSafeEpochGuard : IEpochGuard<DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>> kernelSession) => kernelSession.BeginUnsafe();

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>> kernelSession) => kernelSession.EndUnsafe();
    }

    /// <summary>
    /// Do not perform epoch safety operations when entering/exiting an API (assumes a higher code region has done so)
    /// </summary>
    public struct GarnetUnsafeEpochGuard : IEpochGuard<DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>> kernelSession) { }

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* TStoreFunctions1 */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            /* TAllocator1 */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                             byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
            /* TStoreFunctions2 */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            /* TAllocator2 */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>> kernelSession) { }
    }
}