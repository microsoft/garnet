// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Text;

namespace Tsavorite.core
{
    /// <summary>
    /// Object serializer interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IObjectSerializer<T>
    {
        /// <summary>
        /// Begin serialization to given stream
        /// </summary>
        /// <param name="stream"></param>
        void BeginSerialize(Stream stream);

        /// <summary>
        /// Serialize object
        /// </summary>
        /// <param name="obj"></param>
        void Serialize(T obj);

        /// <summary>
        /// End serialization to given stream
        /// </summary>
        void EndSerialize();

        /// <summary>
        /// Begin deserialization from given stream
        /// </summary>
        /// <param name="stream"></param>
        void BeginDeserialize(Stream stream);

        /// <summary>
        /// Deserialize object
        /// </summary>
        /// <param name="obj"></param>
        void Deserialize(out T obj);

        /// <summary>
        /// End deserialization from given stream
        /// </summary>
        void EndDeserialize();
    }

    /// <summary>
    /// Serializer base class for binary reader and writer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BinaryObjectSerializer<T> : IObjectSerializer<T>
    {
        /// <summary>Shared no-BOM UTF8 encoding for the reader/writer; <see cref="Encoding"/> instances are thread-safe, and
        /// <see cref="BinaryReader"/> / <see cref="BinaryWriter"/> never emit or consume a preamble.</summary>
        static readonly UTF8Encoding Utf8 = new();

        protected BinaryReader reader;
        protected BinaryWriter writer;

        /// <summary>The stream <see cref="writer"/> was created over, so a repeated <see cref="BeginSerialize"/> on the same
        /// (reused) stream can reuse the writer rather than allocating a new one per serialization.</summary>
        Stream writerStream;

        /// <summary>Begin deserialization</summary>
        public void BeginDeserialize(Stream stream) => reader = new BinaryReader(stream, Utf8, true);

        /// <summary>Deserialize</summary>
        public abstract void Deserialize(out T obj);

        /// <summary>End deserialize</summary>
        public void EndDeserialize() => reader.Dispose();

        /// <summary>Begin serialize</summary>
        /// <remarks>The writer is cached and reused while <paramref name="stream"/> is unchanged (the streaming chunk writer
        /// reuses one stream across records), so a hot serialization path allocates no writer.</remarks>
        public void BeginSerialize(Stream stream)
        {
            if (!ReferenceEquals(writerStream, stream))
            {
                writer = new BinaryWriter(stream, Utf8, leaveOpen: true);
                writerStream = stream;
            }
        }

        /// <summary>Serialize</summary>
        public abstract void Serialize(T obj);

        /// <summary>End serialize</summary>
        /// <remarks>Flushes rather than disposes: the writer is left open (it never owns the stream) so it can be reused by the
        /// next <see cref="BeginSerialize"/> on the same stream.</remarks>
        public void EndSerialize() => writer.Flush();
    }
}