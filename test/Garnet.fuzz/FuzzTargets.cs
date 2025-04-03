// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace Garnet.fuzz
{
    /// <summary>
    /// Different known fuzz targets.
    /// </summary>
    internal enum FuzzTargets
    {
        /// <summary>
        /// Fuzz parsing of requests into <see cref="RespCommand"/>s.
        /// </summary>
        RespCommandParsing,

        /// <summary>
        /// Fuzz compilation of Lua scripts in <see cref="LuaRunner"/>.
        /// </summary>
        LuaScriptCompilation,
    }
}
