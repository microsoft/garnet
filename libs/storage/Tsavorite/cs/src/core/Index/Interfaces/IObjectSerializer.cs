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
        /// Indicates whether the struct is to be used. Primarily for serializers implemented as structs (e.g. for inlining).
        /// </summary>
        bool IsNull { get; }

        /// <summary>
        /// Begin serialization to given stream
        /// </summary>
        /// <param name="stream"></param>
        void BeginSerialize(Stream stream);

        /// <summary>
        /// Serialize object
        /// </summary>
        /// <param name="obj"></param>
        void Serialize(ref T obj);

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

        /// <summary>If this class is non-null it is always to be used</summary>
        public bool IsNull => false;

        /// <summary>Begin deserialization</summary>
        public void BeginDeserialize(Stream stream) => reader = new BinaryReader(stream, new UTF8Encoding(), true);

        /// <summary>Deserialize</summary>
        public abstract void Deserialize(out T obj);

        /// <summary>End deserialize</summary>
        public void EndDeserialize() => reader.Dispose();

        /// <summary>Begin serialize</summary>
        public void BeginSerialize(Stream stream) => writer = new BinaryWriter(stream, new UTF8Encoding(), true);

        /// <summary>Serialize</summary>
        public abstract void Serialize(ref T obj);

        /// <summary>End serialize</summary>
        public void EndSerialize() => writer.Dispose();
    }

    /// <summary>
    /// For use with <see cref="IStoreFunctions{TKey, TValue}"/> implementations for Key or Value types that do not have serialization 
    /// or have a custom implementation that is handled entirely by the allocator (e.g. <see cref="SpanByte"/>).
    /// </summary>
    /// <remarks>Calling methods on this type an error, so it throws rather than no-ops.</remarks>>
    public struct NoSerializer<T> : IObjectSerializer<T>
    {
        /// <summary>Default instance</summary>
        public static NoSerializer<T> Instance = new();

        /// <inheritdoc/>
        public readonly bool IsNull => true;

        /// <inheritdoc/>
        public void BeginDeserialize(Stream stream) => throw new System.NotImplementedException();

        /// <inheritdoc/>
        public void BeginSerialize(Stream stream) => throw new System.NotImplementedException();

        /// <inheritdoc/>
        public void Deserialize(out T obj) => throw new System.NotImplementedException();

        /// <inheritdoc/>
        public void EndDeserialize() => throw new System.NotImplementedException();

        /// <inheritdoc/>
        public void EndSerialize() => throw new System.NotImplementedException();

        /// <inheritdoc/>
        public void Serialize(ref T obj) => throw new System.NotImplementedException();
    }
}