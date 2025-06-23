// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Embedded.server;
using Garnet.server;

namespace Garnet.fuzz
{
    /// <summary>
    /// Different known fuzz targets.
    /// </summary>
    public enum FuzzTargets
    {
        /// <summary>
        /// Fuzz parsing of requests into <see cref="RespCommand"/>s.
        /// </summary>
        RespCommandParsing,

        /// <summary>
        /// Fuzz compilation of Lua scripts in <see cref="LuaRunner"/>.
        /// </summary>
        LuaScriptCompilation,

        /// <summary>
        /// Fuzz Garnet accepting resp commands, using <see cref="GarnetServerEmbedded"/>.
        /// </summary>
        GarnetEndToEnd,
    }
}