// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public void DisposeSingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
        }

        /// <inheritdoc />
        public void DisposeCopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeInitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeSingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeDeserializedFromDisk(ref SpanByte key, ref SpanByte value)
        {
        }

        /// <inheritdoc />
        public void DisposeForRevivification(ref SpanByte key, ref SpanByte value, int newKeySize)
        {
        }
    }
}