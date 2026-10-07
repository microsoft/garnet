// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;

namespace Garnet.client
{
    /// <summary>
    /// Reusable payload async flush result for the request lane of a duplex operation ring.
    /// </summary>
    sealed class DuplexOperationAsyncFlushResult<TRequestContext>
        where TRequestContext : struct, IRequestContext
    {
        public CountWrapper count;
        public TRequestContext request;
        public int remainingChunks;
        public DuplexAdmissionController controller;
        public bool perOperationAllocation;

        internal void Initialize(
            CountWrapper count,
            TRequestContext request,
            int remainingChunks,
            DuplexAdmissionController admission,
            bool perOperationAllocation)
        {
            this.count = count;
            this.request = request;
            this.remainingChunks = remainingChunks;
            this.controller = admission;
            this.perOperationAllocation = perOperationAllocation;
        }

        /// <summary>
        /// Finalizes one dispatched chunk. The final chunk disposes the request and advances the admission
        /// controller's flushed watermark.
        /// </summary>
        public static void CompleteChunk(object context)
        {
            switch (context)
            {
                case DuplexOperationAsyncFlushResult<TRequestContext> result:
                    if (Interlocked.Decrement(ref result.remainingChunks) == 0)
                    {
                        var request = result.request;
                        var admission = result.controller;
                        var count = result.count;
                        var perOperationAllocation = result.perOperationAllocation;
                        result.request = default;
                        result.controller = null;
                        result.count = null;
                        result.perOperationAllocation = false;
                        if (perOperationAllocation)
                            admission.CompletePerOperationFlushResult();
                        request.Dispose();
                        admission.CompleteFlush(count);
                    }
                    break;

                default:
                    Debug.Fail($"Unexpected network flush context type {context?.GetType().FullName ?? "null"}.");
                    break;
            }
        }
    }
}