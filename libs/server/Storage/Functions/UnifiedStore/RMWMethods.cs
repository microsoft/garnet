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
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output,
            ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        public void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();

        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord,
            in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref RMWInfo rmwInfo) =>
            throw new NotImplementedException();
    }
}
