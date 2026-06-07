// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Device.benchmark
{
    /// <summary>
    /// Common surface the run harness drives: a thread entrypoint plus per-worker counters
    /// (single-writer from the processor thread; published to the main thread via doneEvent).
    /// </summary>
    abstract class BenchWorkerBase
    {
        internal long localCompletedOk;
        internal long localErrors;
        internal readonly long[] localErrorCounts = new long[256];

        public abstract void Run();
    }
}