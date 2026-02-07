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
        GarnetStatus GET_WithPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output, long ctx, out bool pending);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="outputArr"></param>
        /// <param name="wait"></param>
        bool GET_CompletePending((GarnetStatus, StringOutput)[] outputArr, bool wait = false);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="completedOutputs"></param>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool GET_CompletePending(out CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs, bool wait = false);

        /// <summary>
        /// RMW operation on main store
        /// </summary>
        GarnetStatus RMW_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Read operation on main store
        /// </summary>
        GarnetStatus Read_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// RMW operation on object store
        /// </summary>
        GarnetStatus RMW_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Read operation on object store
        /// </summary>
        GarnetStatus Read_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// RMW operation on unified store
        /// </summary>
        GarnetStatus RMW_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// Read operation on unified store
        /// </summary>
        GarnetStatus Read_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// Read batch of keys on main store.
        /// </summary>
        void ReadWithPrefetch<TBatch>(ref TBatch batch, long context = default)
            where TBatch : IReadArgBatch<StringInput, StringOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            ;
    }
}