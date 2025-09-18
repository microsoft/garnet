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
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();

        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref UnifiedStoreInput input) => throw new NotImplementedException();

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref UnifiedStoreInput input) => throw new NotImplementedException();

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref UnifiedStoreInput input) => throw new NotImplementedException();

        public RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord,
            ref UnifiedStoreInput input) where TSourceLogRecord : ISourceLogRecord =>
            throw new NotImplementedException();
    }
}
