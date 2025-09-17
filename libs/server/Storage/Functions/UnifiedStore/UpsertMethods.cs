// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, GarnetUnifiedStoreOutput, long>
    {
        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedStoreInput input, in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output,
            ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> newValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject newValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) =>
            throw new NotImplementedException();

        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();
    }
}
