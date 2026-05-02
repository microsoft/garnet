// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Represents the result of a chunk read from a checkpoint data source.
    /// For device-backed sources, <see cref="Buffer"/> is set and <see cref="Data"/> is empty.
    /// For memory-backed sources, <see cref="Data"/> is set and <see cref="Buffer"/> is null.
    /// </summary>
    internal readonly struct DataSourceReadResult
    {
        /// <summary>
        /// The buffer containing the read data (device-backed sources).
        /// May be null for memory-backed sources.
        /// </summary>
        public readonly SectorAlignedMemory Buffer;

        /// <summary>
        /// The in-memory data (memory-backed sources).
        /// Null for device-backed sources.
        /// </summary>
        public readonly byte[] Data;

        /// <summary>
        /// The number of bytes read.
        /// </summary>
        public readonly int BytesRead;

        /// <summary>
        /// The start address of this chunk in the source.
        /// </summary>
        public readonly long ChunkStartAddress;

        /// <summary>
        /// Creates a device-backed chunk read result.
        /// </summary>
        public DataSourceReadResult(SectorAlignedMemory buffer, int bytesRead, long chunkStartAddress)
        {
            Buffer = buffer;
            Data = default;
            BytesRead = bytesRead;
            ChunkStartAddress = chunkStartAddress;
        }

        /// <summary>
        /// Creates a memory-backed chunk read result.
        /// </summary>
        public DataSourceReadResult(byte[] data, long chunkStartAddress)
        {
            Buffer = null;
            Data = data;
            BytesRead = data.Length;
            ChunkStartAddress = chunkStartAddress;
        }
    }
}