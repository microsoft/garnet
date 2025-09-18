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
                RespCommand.MEMORY_USAGE => HandleMemoryUsage(in srcLogRecord, ref input, ref output, ref readInfo),
                RespCommand.TYPE => HandleType(in srcLogRecord, ref input, ref output, ref readInfo),
                _ => throw new NotImplementedException(),
            };
        }

        private bool HandleMemoryUsage<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref ReadInfo readInfo) where TSourceLogRecord : ISourceLogRecord
        {
            long memoryUsage;
            if (srcLogRecord.Info.ValueIsObject)
            {
                memoryUsage = RecordInfo.GetLength() + (2 * IntPtr.Size) + // Log record length
                              Utility.RoundUp(srcLogRecord.Key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + // Key allocation in heap with overhead
                              srcLogRecord.ValueObject.MemorySize; // Value allocation in heap
            }
            else
            {
                memoryUsage = RecordInfo.GetLength() +
                              Utility.RoundUp(srcLogRecord.Key.TotalSize(), RecordInfo.GetLength()) +
                              Utility.RoundUp(srcLogRecord.ValueSpan.TotalSize(), RecordInfo.GetLength());
            }

            output.Header.result1 = memoryUsage;
            return true;
        }

        private bool HandleType<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref UnifiedStoreInput input,
            ref GarnetUnifiedStoreOutput output, ref ReadInfo readInfo) where TSourceLogRecord : ISourceLogRecord
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
    }
}
