// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession
    {
        /// <summary>
        /// Handles the complete pending for Object Store session
        /// </summary>
        /// <param name="status"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        static void CompletePendingForObjectStoreSession<TContext>(ref Status status, ref GarnetObjectStoreOutput output, ref TContext objectContext)
            where TContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            objectContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            Debug.Assert(!completedOutputs.Next());
            completedOutputs.Dispose();
        }
    }
}