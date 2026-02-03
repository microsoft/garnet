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
        protected new readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctionsForServer(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            => CopyWithHeaderTo(ref value, ref dst, memoryPool);

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => CopyWithHeaderTo(ref value, ref dst, memoryPool);

        /// <summary>
        /// Copy to given SpanByteAndMemory (header length and payload copied to actual span/memory)
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <param name="memoryPool"></param>
        private static unsafe bool CopyWithHeaderTo(ref SpanByte src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= src.TotalSize)
                {
                    dst.Length = src.TotalSize;
                    var span = dst.SpanByte.AsSpan();
                    fixed (byte* ptr = span)
                        *(int*)ptr = src.Length;
                    src.AsReadOnlySpan().CopyTo(span.Slice(sizeof(int)));
                    return true;
                }
                dst.ConvertToHeap();
            }

            dst.Length = src.TotalSize;
            dst.Memory = memoryPool.Rent(src.TotalSize);
            dst.Length = src.TotalSize;
            fixed (byte* ptr = dst.Memory.Memory.Span)
                *(int*)ptr = src.Length;
            src.AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span.Slice(sizeof(int)));
            return true;
        }
    }
}