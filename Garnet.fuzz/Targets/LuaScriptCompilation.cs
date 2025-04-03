// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Fuzz target for Lua script compilation.
    /// </summary>
    public sealed class LuaScriptCompilation : IFuzzerTarget
    {
        /// <inheritdoc/>
        public static void Fuzz(ReadOnlySpan<byte> input)
        {
            throw new NotImplementedException();
        }
    }
}
