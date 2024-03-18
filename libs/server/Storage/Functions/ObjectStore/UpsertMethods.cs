// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }

        /// <inheritdoc />
        public void PostSingleWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (reason != WriteReason.CopyToTail)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);

            if (reason == WriteReason.CopyToReadCache)
                functionsState.objectStoreSizeTracker?.AddReadCacheTrackedSize(MemoryUtils.CalculateKeyValueSize(key, src));
            else
                functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, src));
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            dst = src;
            if (!upsertInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);
            functionsState.objectStoreSizeTracker?.AddTrackedSize(dst.Size - src.Size);
            return true;
        }
    }
}