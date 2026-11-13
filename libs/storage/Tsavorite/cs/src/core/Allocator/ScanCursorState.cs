// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal sealed class ScanCursorState
    {
        internal IScanIteratorFunctions functions;
        internal long acceptedCount;     // Number of records pushed to and accepted by the caller
        internal bool endBatch;          // End the batch (but return a valid cursor for the next batch, as if "count" records had been returned)
        internal long retryAddress;     // Earliest original address requested for retry in this batch
        internal bool stop;              // Stop the operation (as if all records in the db had been returned)

        internal void Initialize(IScanIteratorFunctions scanIteratorFunctions)
        {
            functions = scanIteratorFunctions;
            acceptedCount = 0;
            endBatch = false;
            retryAddress = long.MaxValue;
            stop = false;
        }
    }
}