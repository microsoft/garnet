﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Options for Lua scripting.
    /// </summary>
    public sealed class LuaOptions
    {
        private readonly ILogger logger;

        public LuaMemoryManagementMode MemoryManagementMode = LuaMemoryManagementMode.Native;
        public string MemoryLimit = "";

        /// <summary>
        /// Construct options with default options.
        /// </summary>
        public LuaOptions(ILogger logger = null)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Construct options with specific settings.
        /// </summary>
        public LuaOptions(LuaMemoryManagementMode memoryMode, string memoryLimit, ILogger logger = null) : this(logger)
        {
            MemoryManagementMode = memoryMode;
            MemoryLimit = memoryLimit;
        }

        /// <summary>
        /// Get the memory limit, if any, for each script invocation.
        /// </summary>
        internal long? GetMemoryLimitBytes()
        {
            if (string.IsNullOrEmpty(MemoryLimit))
            {
                return null;
            }

            if(MemoryManagementMode == LuaMemoryManagementMode.Native)
            {
                logger?.LogWarning("Lua script memory limit is ignored when mode = {0}", MemoryManagementMode);
                return null;
            }

            return GarnetServerOptions.ParseSize(MemoryLimit);
        }
    }

    /// <summary>
    /// Different Lua supported memory modes.
    /// </summary>
    public enum LuaMemoryManagementMode
    {
        /// <summary>
        /// Uses default Lua allocator - .NET host is unaware of allocations.
        /// </summary>
        Native = 0,

        /// <summary>
        /// Uses <see cref="NativeMemory"/> and informs .NET host of the allocations.
        /// 
        /// Limits are inexactly applied due to native memory allocation overhead.
        /// </summary>
        LimittedNative = 1,

        /// <summary>
        /// Places allocations on the POH using a naive, free-list based, allocator.
        /// 
        /// Limits are pre-allocated when scripts runs, which can increase allocation pressure.
        /// </summary>
        Managed = 2,
    }
}
