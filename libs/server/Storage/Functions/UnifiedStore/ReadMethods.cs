// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
#pragma warning disable IDE0005 // Using directive is unnecessary.
    using static LogRecordUtils;

    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, GarnetUnifiedStoreOutput, long>
    {
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref ReadInfo readInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (CheckExpiry(in srcLogRecord))
            {
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            var cmd = input.header.cmd;
            return cmd switch
            {
                RespCommand.EXISTS => true,
                RespCommand.MIGRATE => HandleMigrate(in srcLogRecord, ref output),
                RespCommand.GETETAG => HandleGetEtag(in srcLogRecord, ref output),
                RespCommand.MEMORY_USAGE => HandleMemoryUsage(in srcLogRecord, ref output),
                RespCommand.TYPE => HandleType(in srcLogRecord, ref output),
                RespCommand.TTL or
                RespCommand.PTTL => HandleTtl(in srcLogRecord, ref output, cmd == RespCommand.PTTL),
                RespCommand.EXPIRETIME or
                RespCommand.PEXPIRETIME => HandleExpireTime(in srcLogRecord, ref output, cmd == RespCommand.PEXPIRETIME),
                _ => throw new NotImplementedException(),
            };
        }

        private bool HandleGetEtag<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref GarnetUnifiedStoreOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var etag = srcLogRecord.ETag;
            writer.WriteInt64(etag);

            return true;
        }

        private bool HandleMemoryUsage<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref GarnetUnifiedStoreOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            var inlineRecordSize = srcLogRecord.GetInlineRecordSizes().allocatedSize;
            long heapMemoryUsage = 0;
            if (srcLogRecord.Info.KeyIsOverflow)
                heapMemoryUsage += srcLogRecord.Key.Length + MemoryUtils.ByteArrayOverhead;

            if (srcLogRecord.Info.ValueIsOverflow)
                heapMemoryUsage += srcLogRecord.ValueSpan.Length + MemoryUtils.ByteArrayOverhead;
            else if (srcLogRecord.Info.ValueIsObject)
            {
                heapMemoryUsage = RecordInfo.Size + (2 * IntPtr.Size) + // Log record length
                              Utility.RoundUp(srcLogRecord.Key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + // Key allocation in heap with overhead
                              srcLogRecord.ValueObject.HeapMemorySize; // Value allocation in heap
            }

            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteInt64(heapMemoryUsage + inlineRecordSize);

            return true;
        }

        private bool HandleType<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref GarnetUnifiedStoreOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            if (srcLogRecord.Info.ValueIsObject)
            {
                switch (srcLogRecord.ValueObject)
                {
                    case SortedSetObject:
                        writer.WriteSimpleString(CmdStrings.zset);
                        break;
                    case ListObject:
                        writer.WriteSimpleString(CmdStrings.list);
                        break;
                    case SetObject:
                        writer.WriteSimpleString(CmdStrings.set);
                        break;
                    case HashObject:
                        writer.WriteSimpleString(CmdStrings.hash);
                        break;
                }
            }
            else
            {
                writer.WriteSimpleString(CmdStrings.stringt);
            }

            return true;
        }

        private bool HandleTtl<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref GarnetUnifiedStoreOutput output, bool milliseconds) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var expiration = srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1;
            var ttlValue = milliseconds
                ? ConvertUtils.MillisecondsFromDiffUtcNowTicks(expiration)
                : ConvertUtils.SecondsFromDiffUtcNowTicks(expiration);

            writer.WriteInt64(ttlValue);
            return true;
        }

        private bool HandleExpireTime<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref GarnetUnifiedStoreOutput output, bool milliseconds) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var expiration = srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1;
            var expireTime = milliseconds
                ? ConvertUtils.UnixTimeInMillisecondsFromTicks(expiration)
                : ConvertUtils.UnixTimeInSecondsFromTicks(expiration);

            writer.WriteInt64(expireTime);
            return true;
        }

        private bool HandleMigrate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref GarnetUnifiedStoreOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            DiskLogRecord.Serialize(in srcLogRecord,
                valueObjectSerializer: srcLogRecord.Info.ValueIsObject ? functionsState.garnetObjectSerializer : null,
                memoryPool: functionsState.memoryPool, output: ref output.SpanByteAndMemory);
            return true;
        }
    }
}