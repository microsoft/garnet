// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;

namespace Tsavorite.core
{
    /// <summary>
    /// Defines a key type for Tsavorite operations, providing hashing and equality comparison.
    /// </summary>
    public interface IKey
    {
        /// <summary>
        /// True if the <see cref="KeyBytes"/> and other memory exposed by this <see cref="IKey"/> can be safely assumed to not move.
        /// 
        /// This includes for the duration of any pending operations, through their explicit completion.
        /// This means things like variables or <see cref="Span{T}"/> need to stay unchanged and in place if wrapped, provided this returns true.
        /// </summary>
        bool IsPinned { get; }

        /// <summary>
        /// True if the <see cref="IKey"/> is truly empty - not zero bytes, but uninitialized and conceptually bereft of data.
        /// 
        /// This should be false for almost all implementors.
        /// </summary>
        bool IsEmpty => false;

        /// <summary>
        /// The raw bytes of this key.
        /// </summary>
        [UnscopedRef]
        ReadOnlySpan<byte> KeyBytes { get; }

        /// <summary>
        /// True if this <see cref="IKey"/> has a namespace associated with it.
        /// 
        /// Namespaces are not visible parts of a key, but are used in hashing and equality.
        /// </summary>
        bool HasNamespace { get; }

        /// <summary>
        /// If <see cref="HasNamespace"/> returns true, called to get the contents of the namespace.
        /// 
        /// The special value [0] is reserved and should never be returned.
        /// </summary>
        [UnscopedRef]
        ReadOnlySpan<byte> NamespaceBytes { get; }
    }
}