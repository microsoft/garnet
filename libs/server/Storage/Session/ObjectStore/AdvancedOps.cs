// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectContext.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);


            if (status.IsWrongType)
            {
                return GarnetStatus.WRONGTYPE;
            }

            if (status.Found)
            {
                if (output.HasWrongType)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectContext.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (status.Found)
            {
                if (output.HasWrongType)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Reads the object store, reporting rather than waiting when the record is not in memory.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing the read.</param>
        /// <param name="output">Output for a read served from memory.</param>
        /// <param name="pending">True if the read went to the device, leaving the status meaningless.</param>
        /// <param name="objectContext">Context to read through.</param>
        /// <returns>The status, meaningless when <paramref name="pending"/> is set.</returns>
        public GarnetStatus Read_ObjectStore_WithPending<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, out bool pending, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (objectContext.Session is null)
                ThrowObjectStoreUninitializedException();

            var status = objectContext.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
            {
                incr_session_pending();
                pending = true;
                return GarnetStatus.NOTFOUND;
            }

            pending = false;
            return InterpretObjectStoreStatus(status, ref output);
        }

        /// <summary>
        /// Maps the status of an object-store operation, whether it was served from memory or from the
        /// device, onto the status its caller replies from. Matches the mapping the waiting path applies in
        /// <c>CompletePendingAndGetGarnetStatus</c>.
        /// </summary>
        internal static GarnetStatus InterpretObjectStoreStatus(Status status, ref ObjectOutput output)
        {
            if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;

            if (status.NotFound && !status.Record.Created)
                return GarnetStatus.NOTFOUND;

            if (status.Found && output.HasWrongType)
                return GarnetStatus.WRONGTYPE;

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Drains this session's pending object-store operations, waiting on the device if asked to.
        /// </summary>
        /// <param name="completedOutputs">Outputs of the operations that completed.</param>
        /// <param name="wait">Whether to wait for every outstanding operation.</param>
        /// <param name="objectContext">Context to drain.</param>
        /// <returns>True if every outstanding operation completed.</returns>
        public bool Object_CompletePending<TObjectContext>(out CompletedOutputIterator<ObjectInput, ObjectOutput, long> completedOutputs, bool wait, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.CompletePendingWithOutputs(out completedOutputs, wait);

        /// <summary>
        /// Drains this session's pending object-store operations without holding the calling thread on the
        /// device.
        /// </summary>
        /// <param name="objectContext">Context to drain.</param>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        public ValueTask<CompletedOutputIterator<ObjectInput, ObjectOutput, long>> Object_CompletePendingAsync<TObjectContext>(ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.CompletePendingWithOutputsAsync();
    }
}