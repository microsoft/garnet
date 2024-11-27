// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        IScanIteratorFunctions<TKey, TValue> streamingSnapshotScanIteratorFunctions;
        long scannedUntilAddressCursor;
        long numberOfRecords;

        class StreamingSnapshotSessionFunctions : SessionFunctionsBase<TKey, TValue, Empty, Empty, Empty>
        {

        }

        class ScanPhase1Functions : IScanIteratorFunctions<TKey, TValue>
        {
            readonly IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions;
            public long numberOfRecords;

            public ScanPhase1Functions(IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions)
            {
                this.userScanIteratorFunctions = userScanIteratorFunctions;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => userScanIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress)
                => userScanIteratorFunctions.OnStart(beginAddress, endAddress);

            /// <inheritdoc />
            public void OnStop(bool completed, long numberOfRecords)
            {
                this.numberOfRecords = numberOfRecords;
            }
        }

        internal void StreamingSnapshotScanPhase1()
        {
            try
            {
                Debug.Assert(systemState.Phase == Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT);

                // Iterate all the read-only records in the store
                scannedUntilAddressCursor = Log.SafeReadOnlyAddress;
                var scanFunctions = new ScanPhase1Functions(streamingSnapshotScanIteratorFunctions);
                using var s = NewSession<Empty, Empty, Empty, StreamingSnapshotSessionFunctions>(new());
                long cursor = 0;
                _ = s.ScanCursor(ref cursor, long.MaxValue, scanFunctions, scannedUntilAddressCursor);
                this.numberOfRecords = scanFunctions.numberOfRecords;
            }
            finally
            {
                Debug.Assert(systemState.Phase == Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT);
                GlobalStateMachineStep(systemState);
            }
        }

        class ScanPhase2Functions : IScanIteratorFunctions<TKey, TValue>
        {
            readonly IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions;
            readonly long phase1NumberOfRecords;

            public ScanPhase2Functions(IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions, long acceptedRecordCount)
            {
                this.userScanIteratorFunctions = userScanIteratorFunctions;
                this.phase1NumberOfRecords = acceptedRecordCount;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => userScanIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress) => true;

            /// <inheritdoc />
            public void OnStop(bool completed, long numberOfRecords)
                => userScanIteratorFunctions.OnStop(completed, phase1NumberOfRecords + numberOfRecords);
        }

        internal void StreamingSnapshotScanPhase2(long untilAddress)
        {
            try
            {
                Debug.Assert(systemState.Phase == Phase.WAIT_FLUSH);

                // Iterate all the (v) records in the store
                var scanFunctions = new ScanPhase2Functions(streamingSnapshotScanIteratorFunctions, this.numberOfRecords);
                using var s = NewSession<Empty, Empty, Empty, StreamingSnapshotSessionFunctions>(new());

                // TODO: This requires ScanCursor to provide a consistent snapshot considering only records up to untilAddress
                // There is a bug in the current implementation of ScanCursor, where it does not provide such a consistent snapshot
                _ = s.ScanCursor(ref scannedUntilAddressCursor, long.MaxValue, scanFunctions, endAddress: untilAddress, maxAddress: untilAddress);

                // Reset the cursor to 0
                scannedUntilAddressCursor = 0;
                numberOfRecords = 0;

                // Reset the callback functions
                streamingSnapshotScanIteratorFunctions = null;

                // Release the semaphore to allow the checkpoint waiting task to proceed
                _hybridLogCheckpoint.flushedSemaphore.Release();
            }
            finally
            {
                Debug.Assert(systemState.Phase == Phase.WAIT_FLUSH);
                GlobalStateMachineStep(systemState);
            }
        }
    }
}