// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;

namespace Garnet.client
{
    /// <summary>
    /// Out-of-line payload async flush result for the request lane of a duplex operation ring.
    /// </summary>
    sealed class DuplexOperationAsyncFlushResult<TRequest> where TRequest : struct, IRequestContext
    {
        public CountWrapper count;
        public TRequest request;
        public int remainingChunks;
        public DuplexOperationAdmission admission;

        /// <summary>
        /// Finalizes one dispatched chunk. The final chunk disposes the request and advances the admission
        /// controller's flushed watermark.
        /// </summary>
        public static void CompleteChunk(object context)
        {
            switch (context)
            {
                case DuplexOperationAsyncFlushResult<TRequest> result:
                    if (Interlocked.Decrement(ref result.remainingChunks) == 0)
                    {
                        result.request.Dispose();
                        result.admission.CompleteFlush(result.count);
                    }
                    break;

                default:
                    Debug.Fail($"Unexpected network flush context type {context?.GetType().FullName ?? "null"}.");
                    break;
            }
        }
    }
}
