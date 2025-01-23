// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            if (CheckExpiry(ref srcLogRecord))
                return false;

            var cmd = input.header.cmd;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations
            if ((ushort)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.Length;
                (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.Memory, 0);
                var ret = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(srcLogRecord.Key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref memoryAndLength, ref readInfo);
                Debug.Assert(valueLength <= value.Length);
                (output.Memory, output.Length) = memoryAndLength;
                return ret;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(ref input, value, ref output, readInfo.IsFromPending);

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref LogRecord<SpanByte> srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            if (CheckExpiry(ref srcLogRecord))
            {
                // TODO: we can proactively expire if we wish, but if we do, we need to write WAL entry
                // readInfo.Action = ReadAction.Expire;
                return false;
            }

            var cmd = input.header.cmd;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations
            if ((ushort)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.Length;
                (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.Memory, 0);
                var ret = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(srcLogRecord.Key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref memoryAndLength, ref readInfo);
                Debug.Assert(valueLength <= value.Length);
                (output.Memory, output.Length) = memoryAndLength;
                return ret;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(ref input, value, ref output, readInfo.IsFromPending);

            return true;
        }
    }
}