// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal sealed class ScanCursorState<TKey, TValue>
    {
        internal IScanIteratorFunctions<TKey, TValue> functions;
        internal long acceptedCount;    // Number of records pushed to and accepted by the caller
        internal bool endBatch;         // End the batch (but return a valid cursor for the next batch, as of "count" records had been returned)
        internal bool stop;             // Stop the operation (as if all records in the db had been returned)

        internal void Initialize(IScanIteratorFunctions<TKey, TValue> scanIteratorFunctions)
        {
            functions = scanIteratorFunctions;
            acceptedCount = 0;
            endBatch = false;
            stop = false;
        }
    }
}