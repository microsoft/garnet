// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Byte array serializer
    /// </summary>
    public sealed class ByteArrayBinaryObjectSerializer : BinaryObjectSerializer<byte[]>
    {
        /// <inheritdoc />
        public override void Deserialize(out byte[] obj) => obj = reader.ReadBytes(reader.ReadInt32());
        /// <inheritdoc />
        public override void Serialize(ref byte[] obj)
        {
            writer.Write(obj.Length);
            writer.Write(obj);
        }
    }
}