// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
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
        /// Source (or compiled source) for the associated Lua script.
        /// </summary>
        public ReadOnlyMemory<byte> ScriptData { get; }

        public LuaScriptHandle(ReadOnlyMemory<byte> scriptData)
        {
            ScriptData = scriptData;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            IsDisposed = true;
        }
    }
}