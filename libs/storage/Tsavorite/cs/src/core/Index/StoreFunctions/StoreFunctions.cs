// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Store functions for an instance of TsavoriteKV.
    /// </summary>
    /// <remarks>
    /// The implementation takes instances of the supported interfaces (e.g. <see cref="IObjectSerializer{T}"/>) to allow custom
    /// implementation of any/all. We also provide standard implementations for standard types. The design exposes the instances
    /// because there is no need to wrap calls to them with additional functionality. This can be changed to redirect if such wrapper
    /// functionality is needed.
    /// </remarks>
    public struct StoreFunctions<TRecordDisposer>(Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordDisposer recordDisposer) : IStoreFunctions
        where TRecordDisposer : IRecordDisposer
    {
        #region Fields
        /// <summary>Serialize a Value to persistent storage</summary>
        readonly Func<IObjectSerializer<IHeapObject>> valueSerializerCreator = valueSerializerCreator;

        /// <summary>Dispose a record</summary>
        readonly TRecordDisposer recordDisposer = recordDisposer;

        /// <summary>Optional checkpoint completion callback, set separately from ctor.</summary>
        Action checkpointCompletionCallback = () => { };
        #endregion Fields

        #region Value Serializer
        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> CreateValueObjectSerializer() => valueSerializerCreator is null ? default : valueSerializerCreator();

        /// <inheritdoc/>
        public readonly bool HasValueSerializer => valueSerializerCreator is not null;

        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> BeginSerializeValue(Stream stream)
        {
            var valueSerializer = CreateValueObjectSerializer();
            valueSerializer.BeginSerialize(stream);
            return valueSerializer;
        }

        /// <inheritdoc/>
        public readonly IObjectSerializer<IHeapObject> BeginDeserializeValue(Stream stream)
        {
            var valueSerializer = CreateValueObjectSerializer();
            valueSerializer.BeginDeserialize(stream);
            return valueSerializer;
        }
        #endregion Value Serializer

        #region Record Disposer
        /// <inheritdoc/>
        public readonly bool DisposeOnPageEviction => recordDisposer.DisposeOnPageEviction;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void DisposeValueObject(IHeapObject valueObject, DisposeReason reason) => recordDisposer.DisposeValueObject(valueObject, reason);
        #endregion Record Disposer

        #region Checkpoint Completion
        /// <inheritdoc/>
        public void SetCheckpointCompletedCallback(Action callback) => checkpointCompletionCallback = callback;

        /// <inheritdoc/>
        public readonly void OnCheckpointCompleted() => checkpointCompletionCallback();
        #endregion Checkpoint Completion
    }

    /// <summary>
    /// A minimally-parameterized version of StoreFunctions that provides type-reduced Create() methods.
    /// </summary>
    public struct StoreFunctions
    {
        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TRecordDisposer> Create<TRecordDisposer>
                (Func<IObjectSerializer<IHeapObject>> valueSerializerCreator, TRecordDisposer recordDisposer)
            where TRecordDisposer : IRecordDisposer
            => new(valueSerializerCreator, recordDisposer);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<DefaultRecordDisposer> Create(Func<IObjectSerializer<IHeapObject>> valueSerializerCreator)
            => new(valueSerializerCreator, DefaultRecordDisposer.Instance);

        /// <summary>
        /// Construct a StoreFunctions instance with all types specified and contained instances passed, e.g. for custom objects.
        /// </summary>
        public static StoreFunctions<TRecordDisposer> Create<TRecordDisposer>(TRecordDisposer recordDisposer)
            where TRecordDisposer : IRecordDisposer
            => new(valueSerializerCreator: null, recordDisposer);

        /// <summary>
        /// Default store functions.
        /// </summary>
        public static StoreFunctions<DefaultRecordDisposer> Create()
            => new(valueSerializerCreator: null, DefaultRecordDisposer.Instance);
    }
}