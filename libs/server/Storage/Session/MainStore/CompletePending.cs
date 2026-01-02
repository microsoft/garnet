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
        internal static void CompletePendingForSession<TStringContext>(ref Status status, ref SpanByteAndMemory output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => CompletePendingForSession(ref status, ref output, ref context, out _);

        /// <summary>
        /// Handles the complete pending status for Session Store
        /// </summary>
        static void CompletePendingForSession<TStringContext>(ref Status status, ref SpanByteAndMemory output, ref TStringContext stringContext, out RecordMetadata recordMetadata)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            stringContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            recordMetadata = completedOutputs.Current.RecordMetadata;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
        }

        /// <summary>
        /// Handles the complete pending status for Session Store, without outputs.
        /// </summary>
        static void CompletePendingForSession<TContext>(ref TContext context)
            where TContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        => context.CompletePending(wait: true);
    }
}