// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Key type for Vector Set element data - anything that's hidden in a namespace.
    /// 
    /// Has same constraints as <see cref="FixedSpanByteKey"/> - must be pinned and "fixed" for duration of operation.
    /// 
    /// Always has a namespace.
    /// </summary>
    public readonly ref struct VectorElementKey : IKey
    {
        /// <inheritdoc/>
        public readonly bool IsPinned
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => true;
        }

        /// <inheritdoc/>
        public readonly bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => false;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            get => true;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }

        /// <summary>
        /// Construct a new <see cref="VectorElementKey"/>.
        /// 
        /// Note that <paramref name="namespaceBytes"/> cannot be 0.
        /// </summary>
        public VectorElementKey(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> key)
        {
            RecordNamespace.AssertValid(namespaceBytes);
            Debug.Assert(namespaceBytes.Length == 1 || (namespaceBytes.Length % 4) == 0, "Namespace must be either 1-byte or a multiple of 4-bytes long");
            Debug.Assert(namespaceBytes.Length != 4 || BinaryPrimitives.ReadUInt32LittleEndian(namespaceBytes) > RecordNamespace.MaximumSingleByteNamespaceValue, $"A context of {RecordNamespace.MaximumSingleByteNamespaceValue} or less must be stored in a single byte");

            KeyBytes = key;
            NamespaceBytes = namespaceBytes;
        }

        /// <inheritdoc/>
        public override readonly string ToString() => $"ns: {SpanByte.ToShortString(NamespaceBytes)}, {SpanByte.ToShortString(KeyBytes)}";
    }
}