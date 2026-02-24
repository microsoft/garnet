// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Output type used by Garnet unified store.
    /// Any field / property added to this struct must be set in the back-end (IFunctions) and used in the front-end (GarnetApi caller).
    /// That is in order to justify transferring data in this struct through the Tsavorite storage layer.
    /// </summary>
    public struct UnifiedOutput
    {
        /// <summary>
        /// Span byte and memory
        /// </summary>
        public SpanByteAndMemory SpanByteAndMemory;

        public UnifiedOutput() => SpanByteAndMemory = new(null);

        public UnifiedOutput(SpanByteAndMemory sbam) => SpanByteAndMemory = sbam;

        public static unsafe UnifiedOutput FromPinnedPointer(byte* pointer, int length)
            => new(new SpanByteAndMemory() { SpanByte = PinnedSpanByte.FromPinnedPointer(pointer, length) });

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we complete all pending operations before releasing the pinned source bytes.
        }

        public void Dispose()
        {
            SpanByteAndMemory.Dispose();
        }
    }
}