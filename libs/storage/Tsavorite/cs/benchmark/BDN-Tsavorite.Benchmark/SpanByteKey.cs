// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace BenchmarkDotNetTests
{
    public readonly ref struct SpanByteKey : IKey
    {
        /// <summary>
        /// In benchmarks, we don't wait for pending operations to complete so the span isn't fixed.
        /// </summary>
        public readonly bool IsPinned => false;

        /// <inheritdoc/>
        public readonly bool IsEmpty => false;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes { get; }

        public SpanByteKey(ReadOnlySpan<byte> keyBytes)
        {
            KeyBytes = keyBytes;
        }

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];
    }
}