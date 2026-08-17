// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// The encoding rules for the namespace carried by an <see cref="IKey"/>. A namespace of a single byte valued
    /// 1..<see cref="MaximumSingleByteNamespaceValue"/> is held in the record's namespace byte and costs no record space; any other
    /// namespace is written into the record between the <see cref="RecordDataHeader"/> and the Key, and the namespace byte holds its
    /// length instead. Callers that size a record must report that length via <see cref="RecordFieldInfo.ExtendedNamespaceSize"/>.
    /// </summary>
    public static class RecordNamespace
    {
        /// <summary>Largest namespace value that can be held in the record's namespace byte; larger values require extended namespace space.</summary>
        public const byte MaximumSingleByteNamespaceValue = (1 << RecordDataHeader.ExtendedNamespaceIndicatorBit) - 1;

        /// <summary>Largest extended namespace, in bytes; its size is encoded in the same 7 bits that would otherwise hold a single-byte value.</summary>
        public const byte MaximumExtendedNamespaceSize = (1 << RecordDataHeader.ExtendedNamespaceIndicatorBit) - 1;

        /// <summary>Asserts <paramref name="namespaceBytes"/> satisfies the <see cref="IKey.NamespaceBytes"/> contract.</summary>
        [Conditional("DEBUG")]
        public static void AssertValid(ReadOnlySpan<byte> namespaceBytes)
        {
            Debug.Assert(!namespaceBytes.IsEmpty, "Namespace cannot be empty");
            Debug.Assert(namespaceBytes.Length != 1 || namespaceBytes[0] != 0, "The single-byte namespace value 0 is reserved");
            Debug.Assert(namespaceBytes.Length <= MaximumExtendedNamespaceSize, $"Namespace size {namespaceBytes.Length} exceeds the maximum of {MaximumExtendedNamespaceSize}");
        }

        /// <summary>Asserts the caller sized the record for the namespace of <paramref name="key"/>; a mismatch frames the record incorrectly.</summary>
        [Conditional("DEBUG")]
        public static void AssertKeyCorrectlySized<TKey>(TKey key, in RecordSizeInfo sizeInfo) where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var expectedExtendedSize = GetExtendedNamespaceSize(in key);
            Debug.Assert(sizeInfo.FieldInfo.ExtendedNamespaceSize == expectedExtendedSize, $"Extended namespace size {sizeInfo.FieldInfo.ExtendedNamespaceSize} does not match the key's required size {expectedExtendedSize}");
        }

        /// <summary>The record space the namespace of <paramref name="key"/> requires ahead of the Key data; 0 if it needs none.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetExtendedNamespaceSize<TKey>(in TKey key) where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => key.HasNamespace ? GetExtendedNamespaceSize(key.NamespaceBytes) : 0;

        /// <summary>The record space <paramref name="namespaceBytes"/> requires ahead of the Key data; 0 if it needs none.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetExtendedNamespaceSize(ReadOnlySpan<byte> namespaceBytes)
        {
            AssertValid(namespaceBytes);
            return namespaceBytes.Length == 1 && namespaceBytes[0] <= MaximumSingleByteNamespaceValue ? 0 : namespaceBytes.Length;
        }
    }
}