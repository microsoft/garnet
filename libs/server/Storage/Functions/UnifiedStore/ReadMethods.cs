// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogRecordUtils;
    using static Utility;

    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedInput input,
            ref UnifiedOutput output, ref ReadInfo readInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (CheckExpiry(in srcLogRecord))
            {
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            output.ETag = srcLogRecord.ETag;

            if (!input.metaCommandInfo.CheckConditionalExecution(srcLogRecord.ETag, out _, readOnlyContext: true))
                return functionsState.HandleSkippedExecution(in input.header, ref output.SpanByteAndMemory);

            var cmd = input.header.cmd;
            var result =  cmd switch
            {
                RespCommand.EXISTS => true,
                RespCommand.MIGRATE => HandleMigrate(in srcLogRecord, (int)input.arg1, ref output),
                RespCommand.GETETAG => HandleGetEtag(in srcLogRecord, ref output),
                RespCommand.MEMORY_USAGE => HandleMemoryUsage(in srcLogRecord, ref output),
                RespCommand.TYPE => HandleType(in srcLogRecord, ref output),
                RespCommand.TTL or
                RespCommand.PTTL => HandleTtl(in srcLogRecord, ref output, cmd == RespCommand.PTTL),
                RespCommand.EXPIRETIME or
                RespCommand.PEXPIRETIME => HandleExpireTime(in srcLogRecord, ref output, cmd == RespCommand.PEXPIRETIME),
                RespCommand.RENAME or RespCommand.RENAMENX => HandleRename(in srcLogRecord, ref output),
                _ => throw new NotImplementedException(),
            };

            return result;
        }

        private bool HandleGetEtag<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref UnifiedOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var etag = srcLogRecord.ETag;
            writer.WriteInt64(etag);

            return true;
        }

        private bool HandleMemoryUsage<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref UnifiedOutput output) where TSourceLogRecord : ISourceLogRecord
        {
            var inlineRecordSize = srcLogRecord.AllocatedSize;
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
            ref UnifiedOutput output) where TSourceLogRecord : ISourceLogRecord
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
            ref UnifiedOutput output, bool milliseconds) where TSourceLogRecord : ISourceLogRecord
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
            ref UnifiedOutput output, bool milliseconds) where TSourceLogRecord : ISourceLogRecord
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            var expiration = srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1;
            var expireTime = milliseconds
                ? ConvertUtils.UnixTimeInMillisecondsFromTicks(expiration)
                : ConvertUtils.UnixTimeInSecondsFromTicks(expiration);

            writer.WriteInt64(expireTime);
            return true;
        }

        private bool HandleMigrate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, int maxHeapAllocationSize, ref UnifiedOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            DiskLogRecord.Serialize(in srcLogRecord, maxHeapAllocationSize,
                valueObjectSerializer: srcLogRecord.Info.ValueIsObject ? functionsState.garnetObjectSerializer : null,
                memoryPool: functionsState.memoryPool, output: ref output.SpanByteAndMemory);
            return true;
        }

        private bool HandleRename<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedOutput output)
            where TSourceLogRecord : ISourceLogRecord
        {
            // First, copy the inline portion of the record to the output. Any object references are retained in this step; we do *not* serialize,
            // but rather hand off the object references (remapped to the transient allocator if needed), because RENAME is an in-memory operation.

            // network In case of significant shrinkage, calculate this AllocatedSize separately rather than logRecord.GetInlineRecordSizes().allocatedSize.
            var inlineRecordSize = RoundUp(srcLogRecord.ActualSize, 8); // TODO: Constants.kRecordAlignment
            DiskLogRecord.DirectCopyInlinePortionOfRecord(in srcLogRecord, inlineRecordSize, estimatedTotalSize: inlineRecordSize, maxHeapAllocationSize: inlineRecordSize,
                functionsState.memoryPool, ref output.SpanByteAndMemory);
            if (srcLogRecord.Info.RecordHasObjects)
            {
                fixed (byte* recordPtr = output.SpanByteAndMemory.Span)
                {
                    var logRecord = new LogRecord(recordPtr, srcLogRecord.ObjectIdMap);
                    logRecord.RemapOverPinnedTransientMemory(srcLogRecord.ObjectIdMap, functionsState.transientObjectIdMap);
                }
            }
            return true;
        }
    }
}