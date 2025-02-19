// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Garnet.server
{
    /// <summary>
    /// Specialized key type for storing script hashes.
    /// </summary>
    public readonly struct ScriptHashKey : IEquatable<ScriptHashKey>
    {
        // Necessary to keep this alive
        private readonly byte[] arrRef;
        private readonly unsafe long* ptr;

        internal unsafe ScriptHashKey(ReadOnlySpan<byte> stackSpan)
        {
            Debug.Assert(stackSpan.Length == SessionScriptCache.SHA1Len, "Only one valid length for script hash keys");

            ptr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(stackSpan));
        }

        internal unsafe ScriptHashKey(byte[] pohArr)
            : this(pohArr.AsSpan())
        {
            arrRef = pohArr;
        }

        /// <summary>
        /// Copy key data.
        /// </summary>
        public unsafe void CopyTo(Span<byte> into)
        {
            new Span<byte>(ptr, SessionScriptCache.SHA1Len).CopyTo(into);
        }

        /// <inheritdoc/>
        public unsafe bool Equals(ScriptHashKey other)
        {
            Debug.Assert(SessionScriptCache.SHA1Len == 40, "Making a hard assumption that we're comparing 40 bytes");

            // We make an assumption that, since we're comparing hashes, if this is called
            // it will _probably_ return true.  So unconditionally check all 40 bytes in
            // as few instructions as possible.

            var a = ptr;
            var b = other.ptr;

            var aVec1 = Vector256.Load(a);
            var aVec2 = Vector256.Load(a + 1);

            var bVec1 = Vector256.Load(b);
            var bVec2 = Vector256.Load(b + 1);

            return Vector256.EqualsAll(aVec1, bVec1) & Vector256.EqualsAll(aVec2, bVec2);
        }

        /// <inheritdoc />
        public override unsafe int GetHashCode()
        => *(int*)ptr;

        /// <inheritdoc />
        public override bool Equals([NotNullWhen(true)] object obj)
        => obj is ScriptHashKey other && Equals(other);
    }
}