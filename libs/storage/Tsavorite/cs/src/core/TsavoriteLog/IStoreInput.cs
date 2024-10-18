// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Represents a store input that can be serialized into / deserialized from TsavoriteLog
    /// </summary>
    public interface IStoreInput
    {
        /// <summary>
        /// Size of serialized IStoreInput object
        /// </summary>
        public int SerializedLength { get; }

        /// <summary>
        /// Serialize the IStoreInput object into memory buffer
        /// </summary>
        /// <param name="dest">Memory buffer to serialize into. Guaranteed to have at least SerializedLength many bytes</param>
        /// <param name="length">Length of buffer to serialize into.</param>
        /// <returns>Number of serialized bytes</returns>
        public unsafe int CopyTo(byte* dest, int length);

        /// <summary>
        /// Deserializes the IStoreInput object from memory buffer.
        /// </summary>
        /// <param name="src">Memory buffer to deserialize from. Guaranteed to have at least SerializedLength many bytes</param>
        /// <returns>Number of deserialized bytes</returns>
        public unsafe int DeserializeFrom(byte* src);
    }
}