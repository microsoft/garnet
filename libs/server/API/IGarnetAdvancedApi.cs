// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Advanced API calls, not to be used by normal clients
    /// </summary>
    public interface IGarnetAdvancedApi
    {
        /// <summary>
        /// GET with support for pending multiple ongoing operations, scatter gather IO for outputs
        /// </summary>
        GarnetStatus GET_WithPending(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, out bool pending);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="outputArr"></param>
        /// <param name="wait"></param>
        bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="completedOutputs"></param>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool GET_CompletePending(out CompletedOutputIterator<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long> completedOutputs, bool wait = false);

        /// <summary>
        /// RMW operation on main store
        /// </summary>
        GarnetStatus RMW_MainStore(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output);

        /// <summary>
        /// Read operation on main store
        /// </summary>
        GarnetStatus Read_MainStore(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output);

        /// <summary>
        /// RMW operation on object store
        /// </summary>
        GarnetStatus RMW_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output);

        /// <summary>
        /// Read operation on object store
        /// </summary>
        GarnetStatus Read_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output);

        /// <summary>
        /// Read batch of keys on main store.
        /// </summary>
        void ReadWithPrefetch<TBatch>(ref TBatch batch, long context = default)
            where TBatch : IReadArgBatch<SpanByte, RawStringInput, SpanByteAndMemory>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            ;
    }
}