// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;

namespace Garnet.client
{
    /// <summary>
    /// Reusable flush result for one transport payload of the request lane: either a single out-of-line request
    /// or a coalesced run of inline requests (which own no buffer and carry a default request).
    /// </summary>
    sealed class DuplexOperationAsyncFlushResult<TRequestContext>
        where TRequestContext : struct, IRequestContext
    {
        public TRequestContext request;
        public int remainingChunks;
        public DuplexAdmissionController controller;
        public bool perOperationAllocation;

        internal void Initialize(
            TRequestContext request,
            int remainingChunks,
            DuplexAdmissionController admission,
            bool perOperationAllocation)
        {
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
                        var perOperationAllocation = result.perOperationAllocation;
                        result.request = default;
                        result.controller = null;
                        result.perOperationAllocation = false;
                        if (perOperationAllocation)
                            admission.CompletePerOperationFlushResult();
                        request.Dispose();
                        admission.CompleteFlushPart();
                    }
                    break;

                default:
                    Debug.Fail($"Unexpected network flush context type {context?.GetType().FullName ?? "null"}.");
                    break;
            }
        }
    }
}