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

    using GarnetDualKernelSession = DualKernelSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
                /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
                /* MainStoreAllocator */ SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>,
                byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions,
                /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
                /* ObjectStoreAllocator */ GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>
    #endregion Aliases

    public interface IGarnetEpochGuard : IEpochGuard<GarnetDualKernelSession>;

    /// <summary>
    /// Perform epoch safety operations when entering/exiting an API 
    /// </summary>
    public struct GarnetSafeEpochGuard : IGarnetEpochGuard
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref GarnetDualKernelSession kernelSession) 
            => kernelSession.BeginUnsafe();

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref GarnetDualKernelSession kernelSession) 
            => kernelSession.EndUnsafe();
    }

    /// <summary>
    /// Do not perform epoch safety operations when entering/exiting an API (assumes a higher code region has done so)
    /// </summary>
    public struct GarnetUnsafeEpochGuard : IGarnetEpochGuard
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref GarnetDualKernelSession kernelSession)
        { }

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref GarnetDualKernelSession kernelSession)
        { }
    }
}