// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Output type used by Garnet vector store.
    /// 
    /// Basically <see cref="StringOutput"/>, but Vector Set operations know sizes in advance more often.
    /// </summary>
    public struct VectorOutput
    {
        /// <summary>
        /// Span byte and memory
        /// </summary>
        public SpanByteAndMemory SpanByteAndMemory;

        public VectorOutput() => SpanByteAndMemory = new(null);

        public VectorOutput(SpanByteAndMemory span) => SpanByteAndMemory = span;

        public VectorOutput(Span<byte> span) => SpanByteAndMemory = new(PinnedSpanByte.FromPinnedSpan(span));

        public unsafe VectorOutput(byte* ptr, int len) => SpanByteAndMemory = new(PinnedSpanByte.FromPinnedPointer(ptr, len));
    }
}