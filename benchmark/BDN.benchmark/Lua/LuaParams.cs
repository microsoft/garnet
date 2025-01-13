// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Code;
using Garnet.server;

namespace BDN.benchmark.Lua
{
    /// <summary>
    /// Lua parameters
    /// </summary>
    public readonly struct LuaParams
    {
        public readonly LuaMemoryManagementMode Mode { get; }
        public readonly bool MemoryLimit { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public LuaParams(LuaMemoryManagementMode mode, bool memoryLimit)
        {
            Mode = mode;
            MemoryLimit = memoryLimit;
        }

        /// <summary>
        /// Get the equivalent <see cref="LuaOptions"/>.
        /// </summary>
        public LuaOptions CreateOptions()
        => new(Mode, MemoryLimit ? "2m" : "");

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        => $"{Mode},{(MemoryLimit ? "Limit" : "None")}";
    }
}