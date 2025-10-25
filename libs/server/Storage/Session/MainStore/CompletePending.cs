// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession
    {
        /// <summary>
        /// Handles the complete pending status for Session Store
        /// </summary>
        static void CompletePendingForSession<TContext>(ref Status status, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => CompletePendingForSession(ref status, ref output, ref context, out _);

        /// <summary>
        /// Handles the complete pending status for Session Store
        /// </summary>
        static void CompletePendingForSession<TContext>(ref Status status, ref SpanByteAndMemory output, ref TContext context, out RecordMetadata recordMetadata)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            recordMetadata = completedOutputs.Current.RecordMetadata;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
        }
    }
}