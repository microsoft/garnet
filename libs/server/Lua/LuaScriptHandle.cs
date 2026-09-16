// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    internal enum LuaScriptChunkKind : byte
    {
        Text,
        GarnetGeneratedBinary
    }

    internal readonly record struct LuaScriptChunk(ReadOnlyMemory<byte> Data, LuaScriptChunkKind Kind);

    /// <summary>
    /// Used to track the lifetime a shared Lua script, which may end up backing multiple <see cref="LuaRunner"/>s.
    /// </summary>
    public sealed class LuaScriptHandle : IDisposable
    {
        /// <summary>
        /// Returns true if this <see cref="LuaScriptHandle"/> has been disposed.
        /// 
        /// Things kept alive by this handle should be discarded when this state
        /// is encountered.
        /// </summary>
        public bool IsDisposed { get; private set; }

        /// <summary>
        /// Source or internally compiled data for the associated Lua script.
        /// </summary>
        public ReadOnlyMemory<byte> ScriptData => Chunk.Data;

        internal LuaScriptChunk Chunk { get; }

        /// <summary>
        /// Creates a handle for Lua source text.
        /// </summary>
        /// <param name="scriptData">Lua source text.</param>
        /// <returns>A handle containing Lua source text.</returns>
        public static LuaScriptHandle FromSource(ReadOnlyMemory<byte> scriptData) => new(new LuaScriptChunk(scriptData, LuaScriptChunkKind.Text));

        internal LuaScriptHandle(LuaScriptChunk chunk) => Chunk = chunk;

        /// <inheritdoc/>
        public void Dispose()
        {
            IsDisposed = true;
        }
    }
}