// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Fuzz target for Lua script compilation.
    /// </summary>
    public sealed class LuaScriptCompilation : IFuzzerTarget
    {
        private static readonly IEnumerable<LuaOptions> Options =
            [
                new LuaOptions(LuaMemoryManagementMode.Native, "", Timeout.InfiniteTimeSpan, LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Native, "", TimeSpan.FromSeconds(5), LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Tracked, "", Timeout.InfiniteTimeSpan, LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Tracked, "", TimeSpan.FromSeconds(5), LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Tracked, "10m", Timeout.InfiniteTimeSpan, LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Tracked, "10m", TimeSpan.FromSeconds(5), LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Managed, "", Timeout.InfiniteTimeSpan, LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Managed, "", TimeSpan.FromSeconds(5), LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Managed, "10m", Timeout.InfiniteTimeSpan, LuaLoggingMode.Silent, []),
                new LuaOptions(LuaMemoryManagementMode.Managed, "10m", TimeSpan.FromSeconds(5), LuaLoggingMode.Silent, []),
            ];

        /// <inheritdoc/>
        public static void Fuzz(ReadOnlySpan<byte> input)
        {
            IFuzzerTarget.PrepareInput(ref input);

            foreach (var op in Options)
            {
                try
                {
                    using var runner = new LuaRunner(op.MemoryManagementMode, op.GetMemoryLimitBytes(), op.LogMode, op.AllowedFunctions, input.ToArray());

                    runner.CompileForRunner();
                    _ = runner.RunForRunner([], []);
                }
                catch (GarnetException)
                {
                    // expected!
                }
            }
        }
    }
}