// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    ///  Represents a entry that can be serialized directly onto TsavoriteAof when enqueuing
    /// </summary>
    public interface IAofEnqueueEntry
    {
        /// <summary></summary>
        /// <returns> the size in bytes after serialization onto TsavoriteAof</returns>
        public int SerializedLength { get; }

        /// <summary>
        /// Serialize the entry onto TsavoriteAof.
        /// </summary>
        /// <param name="dest">Memory buffer of TsavoriteAof to serialize onto. Guaranteed to have at least SerializedLength() many bytes</param>
        public void SerializeTo(Span<byte> dest);
    }
}