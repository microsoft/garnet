// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        public TimeSpan Timeout = System.Threading.Timeout.InfiniteTimeSpan;
        public LuaLoggingMode LogMode = LuaLoggingMode.Silent;
        public HashSet<string> AllowedFunctions = [];

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
        public LuaOptions(LuaMemoryManagementMode memoryMode, string memoryLimit, TimeSpan timeout, LuaLoggingMode logMode, IEnumerable<string> allowedFunctions, ILogger logger = null) : this(logger)
        {
            MemoryManagementMode = memoryMode;
            MemoryLimit = memoryLimit;
            Timeout = timeout;
            LogMode = logMode;
            AllowedFunctions = new HashSet<string>(allowedFunctions, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Get the memory limit, if any, for each script invocation.
        /// </summary>
        internal int? GetMemoryLimitBytes()
        {
            if (string.IsNullOrEmpty(MemoryLimit))
            {
                return null;
            }

            if (MemoryManagementMode == LuaMemoryManagementMode.Native)
            {
                logger?.LogWarning("Lua script memory limit is ignored when mode = {MemoryManagementMode}", MemoryManagementMode);
                return null;
            }

            var ret = GarnetServerOptions.ParseSize(MemoryLimit, out _);
            if (ret is > int.MaxValue or < 1_024)
            {
                logger?.LogWarning("Lua script memory limit is out of range [1K, 2GB] = {MemoryLimit} and will be ignored", MemoryLimit);
                return null;
            }

            return (int)ret;
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
        Tracked = 1,

        /// <summary>
        /// Places allocations on the POH using a naive, free-list based, allocator.
        /// 
        /// Limits are pre-allocated when scripts runs, which can increase allocation pressure.
        /// </summary>
        Managed = 2,
    }

    /// <summary>
    /// Behavior of redis.log(...) when called in a Lua script.
    /// </summary>
    public enum LuaLoggingMode
    {
        /// <summary>
        /// Calls to redis.log(...) pass through and are record in Garnet's configered logging provider.
        /// 
        /// redis.LOG_DEBUG is mapped to <see cref="LogLevel.Debug"/>.
        /// redis.LOG_VERBOSE is mapped to <see cref="LogLevel.Information"/>.
        /// redis.LOG_NOTICE is mapped to <see cref="LogLevel.Warning"/>.
        /// redis.LOG_WARNING is mapped to <see cref="LogLevel.Error"/>.
        /// </summary>
        Enable = 0,

        /// <summary>
        /// Calls to redis.log(...) succeed, but do nothing.
        /// </summary>
        Silent = 1,

        /// <summary>
        /// Calls to redis.log(...) raise an error reporting logging is disabled.
        /// </summary>
        Disable = 2,
    }
}