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
        IStreamingSnapshotIteratorFunctions<TKey, TValue> streamingSnapshotIteratorFunctions;
        long scannedUntilAddressCursor;
        long numberOfRecords;

        class StreamingSnapshotSessionFunctions : SessionFunctionsBase<TKey, TValue, Empty, Empty, Empty>
        {

        }

        class ScanPhase1Functions : IScanIteratorFunctions<TKey, TValue>
        {
            readonly IStreamingSnapshotIteratorFunctions<TKey, TValue> streamingSnapshotIteratorFunctions;
            readonly Guid checkpointToken;
            readonly long currentVersion;
            readonly long targetVersion;
            public long numberOfRecords;

            public ScanPhase1Functions(IStreamingSnapshotIteratorFunctions<TKey, TValue> streamingSnapshotIteratorFunctions, Guid checkpointToken, long currentVersion, long targetVersion)
            {
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                this.checkpointToken = checkpointToken;
                this.currentVersion = currentVersion;
                this.targetVersion = targetVersion;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept;
                return streamingSnapshotIteratorFunctions.Reader(ref key, ref value, recordMetadata, numberOfRecords);
            }

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => streamingSnapshotIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress)
                => streamingSnapshotIteratorFunctions.OnStart(checkpointToken, currentVersion, targetVersion);

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
                var scanFunctions = new ScanPhase1Functions(streamingSnapshotIteratorFunctions, _hybridLogCheckpointToken, _hybridLogCheckpoint.info.version, _hybridLogCheckpoint.info.nextVersion);
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
            readonly IStreamingSnapshotIteratorFunctions<TKey, TValue> streamingSnapshotIteratorFunctions;
            readonly long phase1NumberOfRecords;

            public ScanPhase2Functions(IStreamingSnapshotIteratorFunctions<TKey, TValue> streamingSnapshotIteratorFunctions, long acceptedRecordCount)
            {
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                this.phase1NumberOfRecords = acceptedRecordCount;
            }

            /// <inheritdoc />
            public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept;
                return streamingSnapshotIteratorFunctions.Reader(ref key, ref value, recordMetadata, numberOfRecords);
            }

            /// <inheritdoc />
            public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => streamingSnapshotIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress) => true;

            /// <inheritdoc />
            public void OnStop(bool completed, long numberOfRecords)
                => streamingSnapshotIteratorFunctions.OnStop(completed, phase1NumberOfRecords + numberOfRecords);
        }

        internal void StreamingSnapshotScanPhase2(long untilAddress)
        {
            try
            {
                Debug.Assert(systemState.Phase == Phase.WAIT_FLUSH);

                // Iterate all the (v) records in the store
                var scanFunctions = new ScanPhase2Functions(streamingSnapshotIteratorFunctions, this.numberOfRecords);
                using var s = NewSession<Empty, Empty, Empty, StreamingSnapshotSessionFunctions>(new());

                // TODO: This requires ScanCursor to provide a consistent snapshot considering only records up to untilAddress
                // There is a bug in the current implementation of ScanCursor, where it does not provide such a consistent snapshot
                _ = s.ScanCursor(ref scannedUntilAddressCursor, long.MaxValue, scanFunctions, endAddress: untilAddress, maxAddress: untilAddress);

                // Reset the cursor to 0
                scannedUntilAddressCursor = 0;
                numberOfRecords = 0;

                // Reset the callback functions
                streamingSnapshotIteratorFunctions = null;

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