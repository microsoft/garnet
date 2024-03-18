// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public void DisposeSingleWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
        }

        /// <inheritdoc />
        public void DisposeCopyUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeInitialUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeSingleDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo)
        {
        }

        /// <inheritdoc />
        public void DisposeDeserializedFromDisk(ref byte[] key, ref IGarnetObject value)
        {
        }

        /// <inheritdoc />
        public void DisposeForRevivification(ref byte[] key, ref IGarnetObject value, int newKeySize)
        {
        }
    }
}