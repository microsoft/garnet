// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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

        public readonly TimeSpan? Timeout { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public LuaParams(LuaMemoryManagementMode mode, bool memoryLimit, TimeSpan? timeout = null)
        {
            Mode = mode;
            MemoryLimit = memoryLimit;
            Timeout = timeout;
        }

        /// <summary>
        /// Get the equivalent <see cref="LuaOptions"/>.
        /// </summary>
        public LuaOptions CreateOptions()
        => new(Mode, MemoryLimit ? "2m" : "", Timeout ?? System.Threading.Timeout.InfiniteTimeSpan, LuaLoggingMode.Enable, []);

        /// <summary>
        /// String representation
        /// </summary>
        public override string ToString()
        {
            if (Timeout != null)
            {
                return $"{Mode},{(MemoryLimit ? "Limit" : "None")},{(Timeout == System.Threading.Timeout.InfiniteTimeSpan ? "-" : Timeout.ToString())}";
            }
            else
            {
                // Keep old format for benchmarks that don't care about timeouts
                return $"{Mode},{(MemoryLimit ? "Limit" : "None")}";
            }
        }
    }
}