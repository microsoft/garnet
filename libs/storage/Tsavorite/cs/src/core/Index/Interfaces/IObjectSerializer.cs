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
        protected BinaryReader reader;
        protected BinaryWriter writer;

        /// <summary>Begin deserialization</summary>
        public void BeginDeserialize(Stream stream) => reader = new BinaryReader(stream, new UTF8Encoding(), true);

        /// <summary>Deserialize</summary>
        public abstract void Deserialize(out T obj);

        /// <summary>End deserialize</summary>
        public void EndDeserialize() => reader.Dispose();

        /// <summary>Begin serialize</summary>
        public void BeginSerialize(Stream stream) => writer = new BinaryWriter(stream, new UTF8Encoding(), true);

        /// <summary>Serialize</summary>
        public abstract void Serialize(T obj);

        /// <summary>End serialize</summary>
        public void EndSerialize() => writer.Dispose();
    }
}