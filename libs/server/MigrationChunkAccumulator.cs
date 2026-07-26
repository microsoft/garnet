// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;
using Tsavorite.core.Allocator.ObjectSerialization;

namespace Garnet.server
{
    /// <summary>
    /// Holds the out-of-line pieces of a record being migrated, captured in-epoch by <c>HandleMigrate</c> so the record can be
    /// sent to the migration target out of epoch.
    /// </summary>
    /// <remarks>
    /// Migration must serialize while holding the store epoch (a migrating key is not locked, so its value may be concurrently
    /// updated), but it cannot stream to the network there: migration sends <b>asynchronously</b> and the store epoch must never
    /// be held across an <c>await</c>. (Replication, by contrast, sends synchronously via <c>BlockingWait</c> and never awaits, so
    /// it can stream a record to the network in-epoch.) So <c>HandleMigrate</c> captures the record's pieces here in-epoch and the
    /// caller assembles and sends them out of epoch. The inline portion is copied separately into
    /// <see cref="UnifiedOutput.SpanByteAndMemory"/>; this holds:
    /// <list type="bullet">
    ///   <item>the overflow key as a <b>shallow reference</b> — store keys are immutable, so the backing array is stable;</item>
    ///   <item>the overflow value as a <b>deep copy</b> — the store value may be mutated once the epoch is released;</item>
    ///   <item>an object value serialized into a <b>list of chunks</b> (which together may exceed 2 GB, the max length of a single
    ///     <c>byte[]</c>), filled via <see cref="IChunkedObjectSerializerConsumer"/>.</item>
    /// </list>
    /// </remarks>
    public sealed class MigrationChunkAccumulator : IChunkedObjectSerializerConsumer
    {
        /// <summary>Serializer ring-buffer size used to stream an object value into <see cref="objectValueChunks"/>; each drained
        /// run becomes one owned chunk.</summary>
        const int ObjectSerializeBufferSize = 4 * 1024 * 1024;

        // Overflow key: shallow reference to the store's immutable key array (no copy).
        OverflowByteArray keyOverflow;
        bool hasKey;

        // Overflow value: deep copy of the store value bytes (the store value may change after the epoch is released).
        byte[] valueOverflow;

        // Object value serialized as a list of owned chunks (>2 GB capable); filled via Consume.
        readonly List<byte[]> objectValueChunks = [];
        bool hasObjectValue;

        /// <summary>Length of the record's inline portion (in <see cref="UnifiedOutput.SpanByteAndMemory"/>); set by the writer.</summary>
        public int InlineLength { get; set; }

        /// <summary>Reset for reuse before capturing the next record.</summary>
        public void Reset()
        {
            keyOverflow = default;
            hasKey = false;
            valueOverflow = null;
            objectValueChunks.Clear();
            hasObjectValue = false;
            InlineLength = 0;
        }

        /// <summary>True when the record is fully inline (no overflow key, no overflow/object value): the whole record is in
        /// <see cref="UnifiedOutput.SpanByteAndMemory"/> and there is nothing to send from here.</summary>
        public bool IsEmpty => !hasKey && valueOverflow is null && !hasObjectValue;

        /// <summary>Capture the overflow key as a shallow reference (store keys are immutable, so the backing array is stable).</summary>
        public void SetKeyOverflow(OverflowByteArray key)
        {
            keyOverflow = key;
            hasKey = true;
        }

        /// <summary>Capture a deep copy of the overflow value bytes (the store value may be mutated after the epoch is released).</summary>
        public void SetValueOverflowDeepCopy(OverflowByteArray value)
            => valueOverflow = value.AsReadOnlySpan(0).ToArray();

        /// <summary>Serialize an object value into <see cref="objectValueChunks"/> via the chunked serializer, which drains here as
        /// its ring fills, so the whole serialized form is never materialized at once (and may exceed 2 GB).</summary>
        public void SerializeObjectValue(IHeapObject valueObject, IObjectSerializer<IHeapObject> serializer)
        {
            hasObjectValue = true;
            var chunker = new ChunkedObjectSerializer<byte>(this, ObjectSerializeBufferSize);
            chunker.BeginSerialize(context: 0);
            using var stream = chunker.GetStream();
            serializer.BeginSerialize(stream);
            serializer.Serialize(valueObject);
            serializer.EndSerialize();
            chunker.EndSerialize();
        }

        /// <summary>True if the record has an overflow key.</summary>
        public bool HasKey => hasKey;
        /// <summary>The overflow key bytes as memory (valid only when <see cref="HasKey"/>).</summary>
        public ReadOnlyMemory<byte> KeyMemory => hasKey ? keyOverflow.AsMemory() : ReadOnlyMemory<byte>.Empty;
        /// <summary>Length of the overflow key (0 if none).</summary>
        public long KeyLength => hasKey ? keyOverflow.AsMemory().Length : 0;

        /// <summary>True if the record has an overflow (non-object) value.</summary>
        public bool HasValueOverflow => valueOverflow is not null;
        /// <summary>The deep-copied overflow value bytes (valid only when <see cref="HasValueOverflow"/>).</summary>
        public ReadOnlyMemory<byte> ValueOverflowMemory => valueOverflow;

        /// <summary>True if the record has an object value.</summary>
        public bool HasObjectValue => hasObjectValue;
        /// <summary>The serialized object value as a list of chunks (valid only when <see cref="HasObjectValue"/>).</summary>
        public List<byte[]> ObjectValueChunks => objectValueChunks;

        /// <summary>Total length of the overflow value or serialized object value (0 if the value is inline).</summary>
        public long ValueLength
        {
            get
            {
                if (valueOverflow is not null)
                    return valueOverflow.Length;
                long len = 0;
                foreach (var chunk in objectValueChunks)
                    len += chunk.Length;
                return len;
            }
        }

        /// <inheritdoc/>
        public int Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TContext context)
        {
            if (!first.IsEmpty)
                objectValueChunks.Add(first.ToArray());
            if (!second.IsEmpty)
                objectValueChunks.Add(second.ToArray());
            return first.Length + second.Length;
        }

        /// <inheritdoc/>
        public int Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TKey key, ref TInput input, TContext context)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput
            => throw new NotSupportedException("Migration serializes only the object value; the key/inline portion are captured separately.");
    }
}