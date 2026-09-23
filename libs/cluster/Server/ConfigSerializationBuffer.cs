// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;

namespace Garnet.cluster
{
    /// <summary>
    /// Reusable buffer for serializing cluster configuration.
    /// </summary>
    internal readonly struct ConfigSerializationBuffer : IDisposable
    {
        readonly MemoryStream stream;
        readonly BinaryWriter writer;

        /// <summary>
        /// Creates a reusable serialization buffer with no initial backing-array allocation.
        /// </summary>
        public ConfigSerializationBuffer()
            : this(0)
        {
        }

        /// <summary>
        /// Creates a reusable serialization buffer.
        /// </summary>
        /// <param name="initialCapacity">Initial stream capacity.</param>
        public ConfigSerializationBuffer(int initialCapacity)
        {
            stream = new MemoryStream(initialCapacity);
            writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: false);
        }

        /// <summary>
        /// Gets the serialized bytes without allocating or copying.
        /// The memory remains valid until this buffer is reset, expanded, or disposed.
        /// </summary>
        public ReadOnlyMemory<byte> WrittenMemory
        {
            get
            {
                _ = stream.TryGetBuffer(out var buffer);
                return buffer.Array.AsMemory(buffer.Offset, checked((int)stream.Length));
            }
        }

        /// <summary>
        /// Returns a newly allocated array containing the serialized bytes.
        /// </summary>
        public byte[] ToByteArray()
            => stream.ToArray();

        /// <summary>
        /// Resets the buffer for serialization while retaining its backing array.
        /// </summary>
        internal BinaryWriter Reset()
        {
            stream.Position = 0;
            stream.SetLength(0);
            return writer;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            writer.Dispose();
            stream.Dispose();
        }
    }
}