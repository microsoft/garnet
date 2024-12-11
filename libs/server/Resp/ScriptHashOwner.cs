// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Garnet.server
{
    /// <summary>
    /// Owner for memory used to store Lua script hashes on the heap.
    /// </summary>
    internal sealed class ScriptHashOwner : IMemoryOwner<byte>
    {
        private readonly Memory<byte> mem;

        /// <inheritdoc/>
        public Memory<byte> Memory => mem;

        internal ScriptHashOwner(Memory<byte> hashMem)
        {
            mem = hashMem;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}