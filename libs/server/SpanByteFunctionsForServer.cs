// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctionsForServer<Context> : SpanByteFunctions<Context>
    {
        /// <summary>
        /// Memory pool
        /// </summary>
        protected readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctionsForServer(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref SpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            => CopyTo(srcLogRecord.ValueSpan, ref output, memoryPool);

        /// <inheritdoc />
        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref SpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            => CopyTo(logRecord.ValueSpan, ref output, memoryPool);

        /// <summary>
        /// Copy to given SpanByteAndMemory (header length and payload copied to actual span/memory)
        /// </summary>
        /// <param name="src"></param>
        /// <param name="output"></param>
        /// <param name="memoryPool"></param>
        private static unsafe bool CopyTo(SpanByte src, ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
        {
            if (output.IsSpanByte)
            {
                if (output.Length >= src.TotalSize)
                {
                    output.Length = src.TotalSize;
                    var span = output.SpanByte.AsSpan();
                    fixed (byte* ptr = span)
                        *(int*)ptr = src.Length;
                    src.AsReadOnlySpan().CopyTo(span.Slice(sizeof(int)));
                    return true;
                }
                output.ConvertToHeap();
            }

            output.Length = src.TotalSize;
            output.Memory = memoryPool.Rent(src.TotalSize);
            output.Length = src.TotalSize;
            fixed (byte* ptr = output.Memory.Memory.Span)
                *(int*)ptr = src.Length;
            src.AsReadOnlySpan().CopyTo(output.Memory.Memory.Span.Slice(sizeof(int)));
            return true;
        }
    }
}