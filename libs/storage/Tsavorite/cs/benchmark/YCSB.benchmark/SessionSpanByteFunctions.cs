// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionSpanByteFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            // Copy only the first cache line for more interpretable results
            value.CopySliceTo(32, ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            // Copy only the first cache line for more interpretable results
            value.CopySliceTo(32, ref dst, memoryPool);
            return true;
        }
    }
}