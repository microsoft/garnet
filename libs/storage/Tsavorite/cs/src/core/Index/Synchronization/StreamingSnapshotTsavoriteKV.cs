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

        struct ScanPhase1Functions : IScanIteratorFunctions<TKey, TValue>
        {
            readonly IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions;

            public ScanPhase1Functions(IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions)
            {
                this.userScanIteratorFunctions = userScanIteratorFunctions;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.EndBatch | CursorRecordResult.RetryLastRecord;
                return false;
            }

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => userScanIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress)
                => userScanIteratorFunctions.OnStart(beginAddress, long.MaxValue);

            /// <inheritdoc />
            public void OnStop(bool completed, long numberOfRecords) { }
        }

        public void StreamingSnapshotScanPhase1()
        {
            try
            {
                Debug.Assert(systemState.Phase == Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT);

                // Iterate all the read-only records in the store
                scannedUntilAddressCursor = 0;
                var scanFunctions = new ScanPhase1Functions(streamingSnapshotScanIteratorFunctions);
                using var s = NewSession<Empty, Empty, Empty, SessionFunctionsBase<TKey, TValue, Empty, Empty, Empty>>(default);
                _ = s.ScanCursor(ref scannedUntilAddressCursor, long.MaxValue, scanFunctions);
            }
            finally
            {
                Debug.Assert(systemState.Phase == Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT);
                GlobalStateMachineStep(systemState);
            }
        }

        struct ScanPhase2Functions : IScanIteratorFunctions<TKey, TValue>
        {
            readonly IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions;

            public ScanPhase2Functions(IScanIteratorFunctions<TKey, TValue> userScanIteratorFunctions)
            {
                this.userScanIteratorFunctions = userScanIteratorFunctions;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => userScanIteratorFunctions.SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                // NOTE: SingleReader is invoked here, because records in (v) are read-only during the WAIT_FLUSH phase
                return userScanIteratorFunctions.SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
            }

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => userScanIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress) => true;

            /// <inheritdoc />
            public void OnStop(bool completed, long numberOfRecords)
                => userScanIteratorFunctions.OnStop(completed, numberOfRecords);
        }

        public void StreamingSnapshotScanPhase2(long untilAddress)
        {
            try
            {
                Debug.Assert(systemState.Phase == Phase.WAIT_FLUSH);

                // Iterate all the (v) records in the store
                var scanFunctions = new ScanPhase2Functions(streamingSnapshotScanIteratorFunctions);
                using var s = NewSession<Empty, Empty, Empty, SessionFunctionsBase<TKey, TValue, Empty, Empty, Empty>>(default);

                // TODO: This requires ScanCursor to provide a consistent snapshot considering only records up to untilAddress
                // There is a bug in the current implementation of ScanCursor, where it does not provide such a consistent snapshot
                _ = s.ScanCursor(ref scannedUntilAddressCursor, long.MaxValue, scanFunctions, untilAddress);

                // Reset the cursor to 0
                scannedUntilAddressCursor = 0;

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