// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions;
        long scannedUntilAddressCursor;
        long numberOfRecords;

        class StreamingSnapshotSessionFunctions : SessionFunctionsBase<Empty, Empty, Empty>
        {

        }

        class ScanPhase1Functions : IScanIteratorFunctions
        {
            readonly IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions;
            readonly Guid checkpointToken;
            readonly long currentVersion;
            readonly long nextVersion;
            public long numberOfRecords;

            public ScanPhase1Functions(IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions, Guid checkpointToken, long currentVersion, long nextVersion)
            {
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                this.checkpointToken = checkpointToken;
                this.currentVersion = currentVersion;
                this.nextVersion = nextVersion;
            }

            /// <inheritdoc />
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                return streamingSnapshotIteratorFunctions.Reader(in srcLogRecord, recordMetadata, numberOfRecords);
            }

            /// <inheritdoc />
            public void OnException(Exception exception, long numberOfRecords)
                => streamingSnapshotIteratorFunctions.OnException(exception, numberOfRecords);

            /// <inheritdoc />
            public bool OnStart(long beginAddress, long endAddress)
                => streamingSnapshotIteratorFunctions.OnStart(checkpointToken, currentVersion, nextVersion);

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
                // Iterate all the read-only records in the store
                scannedUntilAddressCursor = Log.SafeReadOnlyAddress;
                var scanFunctions = new ScanPhase1Functions(streamingSnapshotIteratorFunctions, _hybridLogCheckpointToken, _hybridLogCheckpoint.info.version, _hybridLogCheckpoint.info.nextVersion);
                using var s = NewSession<Empty, Empty, Empty, StreamingSnapshotSessionFunctions>(new());
                long cursor = 0;
                _ = s.ScanCursor(ref cursor, long.MaxValue, scanFunctions, scannedUntilAddressCursor);
                this.numberOfRecords = scanFunctions.numberOfRecords;
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Exception in StreamingSnapshotScanPhase1");
                throw;
            }
        }

        class ScanPhase2Functions : IScanIteratorFunctions
        {
            readonly IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions;
            readonly long phase1NumberOfRecords;

            public ScanPhase2Functions(IStreamingSnapshotIteratorFunctions streamingSnapshotIteratorFunctions, long acceptedRecordCount)
            {
                this.streamingSnapshotIteratorFunctions = streamingSnapshotIteratorFunctions;
                this.phase1NumberOfRecords = acceptedRecordCount;
            }

            /// <inheritdoc />
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                return streamingSnapshotIteratorFunctions.Reader(in srcLogRecord, recordMetadata, numberOfRecords);
            }

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
                // Iterate all the (v) records in the store
                var scanFunctions = new ScanPhase2Functions(streamingSnapshotIteratorFunctions, this.numberOfRecords);
                using var s = NewSession<Empty, Empty, Empty, StreamingSnapshotSessionFunctions>(new());

                _ = s.ScanCursor(ref scannedUntilAddressCursor, long.MaxValue, scanFunctions, endAddress: untilAddress, maxAddress: untilAddress);

                // Reset the cursor to 0
                scannedUntilAddressCursor = 0;
                numberOfRecords = 0;

                // Reset the callback functions
                streamingSnapshotIteratorFunctions = null;
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Exception in StreamingSnapshotScanPhase2");
                throw;
            }
        }
    }
}