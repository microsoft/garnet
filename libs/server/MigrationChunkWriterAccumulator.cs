// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
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
    public sealed class MigrationChunkWriterAccumulator : IChunkedObjectSerializerConsumer
    {
        /// <summary>Serializer ring-buffer size used to stream an object value into <see cref="objectValueChunks"/>; each drained
        /// run becomes one owned chunk.</summary>
        const int ObjectSerializeBufferSize = 4 * 1024 * 1024;

        /// <summary>Reused across records to serialize object values: this accumulator persists across a migration's keys (it lives
        /// on the reused <see cref="UnifiedOutput.Accumulator"/>), so the <see cref="ObjectSerializeBufferSize"/> ring is allocated
        /// once per migration rather than once per object. Created lazily on the first object value.</summary>
        ChunkedObjectSerializer<byte> objectChunker;

        // Overflow key: shallow reference to the store's immutable key array (no copy).
        OverflowByteArray keyOverflow;
        bool hasKey;

        // Overflow value: deep copy of the store value bytes (the store value may change after the epoch is released).
        byte[] valueOverflow;

        // Object value serialized into pooled chunk buffers (>2 GB capable); filled via Consume.
        readonly PooledChunkList objectValueChunks = new();
        bool hasObjectValue;

        /// <summary>Length of the record's inline portion (in <see cref="UnifiedOutput.SpanByteAndMemory"/>); set by the writer.</summary>
        public int InlineLength { get; set; }

        /// <summary>Reset for reuse before capturing the next record.</summary>
        public void Reset()
        {
            keyOverflow = default;
            hasKey = false;
            valueOverflow = null;
            objectValueChunks.Reset();
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
        /// its ring fills, so the whole serialized form is never materialized at once (and may exceed 2 GB). The serializer is
        /// reused across records so its ring is allocated once (see <see cref="objectChunker"/>).</summary>
        public void SerializeObjectValue(IHeapObject valueObject, IObjectSerializer<IHeapObject> serializer)
        {
            hasObjectValue = true;
            objectChunker ??= new ChunkedObjectSerializer<byte>(this, ObjectSerializeBufferSize);
            objectChunker.BeginSerialize(context: 0);
            using var stream = objectChunker.GetStream();
            serializer.BeginSerialize(stream);
            serializer.Serialize(valueObject);
            serializer.EndSerialize();
            objectChunker.EndSerialize();
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
        /// <summary>The serialized object value as pooled chunks (valid only when <see cref="HasObjectValue"/>, and until the
        /// next <see cref="Reset"/>). Enumerate via <see cref="ChunkCount"/> and <see cref="GetChunk"/>.</summary>
        public int ChunkCount => objectValueChunks.Count;

        /// <summary>The serialized object value chunk at <paramref name="index"/>, bounded to its valid bytes.</summary>
        public ReadOnlyMemory<byte> GetChunk(int index) => objectValueChunks.GetChunk(index);

        /// <summary>Total length of the overflow value or serialized object value (0 if the value is inline).</summary>
        public long ValueLength => valueOverflow is not null ? valueOverflow.Length : objectValueChunks.TotalLength;

        /// <inheritdoc/>
        public int Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TContext context)
        {
            // This consumer always consumes the whole buffer (returns first.Length + second.Length), so the serializer's ring
            // never wraps: each drain starts clean at offset 0 with all data contiguous in 'first'. Thus 'second' is always empty
            // here. The second append below is a release-mode safety net only.
            Debug.Assert(second.IsEmpty, "MigrationChunkWriterAccumulator consumes the whole buffer each drain, so the wrapped 'second' span must be empty.");
            if (!first.IsEmpty)
                objectValueChunks.Append(first);
            if (!second.IsEmpty)
                objectValueChunks.Append(second);
            return first.Length + second.Length;
        }

        /// <inheritdoc/>
        public int Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TKey key, ref TInput input, TContext context)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput
            => throw new NotSupportedException("Migration serializes only the object value; the key/inline portion are captured separately.");
    }
}