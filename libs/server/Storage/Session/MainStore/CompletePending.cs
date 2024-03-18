// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession
    {
        /// <summary>
        /// Handles the complete pending status for Session Store
        /// </summary>
        /// <param name="status"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        static void CompletePendingForSession<TContext>(ref Status status, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
        }
    }
}