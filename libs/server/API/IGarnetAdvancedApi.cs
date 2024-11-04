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
        GarnetStatus GET_WithPending<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="outputArr"></param>
        /// <param name="wait"></param>
        bool GET_CompletePending<TKeyLocker, TEpochGuard>((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="completedOutputs"></param>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool GET_CompletePending<TKeyLocker, TEpochGuard>(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long> completedOutputs, bool wait = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// RMW operation on main store
        /// </summary>
        GarnetStatus RMW_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// Read operation on main store
        /// </summary>
        GarnetStatus Read_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// RMW operation on object store
        /// </summary>
        GarnetStatus RMW_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;

        /// <summary>
        /// Read operation on object store
        /// </summary>
        GarnetStatus Read_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard;
    }
}