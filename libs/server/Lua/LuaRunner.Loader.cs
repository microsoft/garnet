// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using KeraLua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    // Code around the loader block goes here.
    internal sealed partial class LuaRunner
    {
        /// <summary>
        /// Simple cache of allowed functions to loader block.
        /// </summary>
        private sealed record LoaderBlockCache(HashSet<string> AllowedFunctions, ReadOnlyMemory<byte> LoaderBlockBytes);

        /// <summary>
        /// Lua code loaded into VM more or less immediately.
        /// 
        /// Bits of this get replaced by <see cref="PrepareLoaderBlockBytes(HashSet{string}, ILogger)"/>.
        /// </summary>
        private const string LoaderBlock = @"
-- disable for sandboxing purposes
import = function () end

-- unpack moved after Lua 5.1, this provides Redis compat
local unpack = table.unpack

-- common functions for handling error replies from Garnet
local chain_func = function(f1, f2)
    return function(...)
        return f1(f2, ...)
    end
end

local error_wrapper_r0 = function(rawFunc, ...) 
    local err = rawFunc(...)
    if err then
        error(err, 0)
    end
end

local error_wrapper_r1 = function(rawFunc, ...) 
    local r1, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1
end

local error_wrapper_r2 = function(rawFunc, ...) 
    local r1, r2, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1, r2
end

local unpackTrampolineRef = garnet_unpack_trampoline
local error_wrapper_rvar_check = function(err, ...)
    if err then
        error(err, 0)
    end

    return ...
end

local error_wrapper_rvar = function(rawFunc, ...)
    -- variable numbers of returns require extra work
    -- and requires that the error token is FIRST, not last
    -- and a count of expected returns is around to handle
    -- trailing nils
    local rets = {rawFunc(...)}
    local err = rets[1]
    local count = rets[2]
    if err then
        error(err, 0)
    end

    return error_wrapper_rvar_check(unpackTrampolineRef(rets, count))
end

-- cutdown os for sandboxing purposes
local osClockRef = os.clock
os = {
    clock = osClockRef
}

-- define cjson for (optional) inclusion into sandbox_env
local cjson = {
    encode = chain_func(error_wrapper_r1, garnet_cjson_encode);
    decode = chain_func(error_wrapper_r1, garnet_cjson_decode);
}

-- define bit for (optional) inclusion into sandbox_env
local garnetBitopRef = chain_func(error_wrapper_r1, garnet_bitop)
local bit = {
    tobit = chain_func(error_wrapper_r1, garnet_bit_tobit);
    tohex = chain_func(error_wrapper_r1, garnet_bit_tohex);
    bnot = function(...) return garnetBitopRef(0, ...); end;
    bor = function(...) return garnetBitopRef(1, ...); end;
    band = function(...) return garnetBitopRef(2, ...); end;
    bxor = function(...) return garnetBitopRef(3, ...); end;
    lshift = function(...) return garnetBitopRef(4, ...); end;
    rshift = function(...) return garnetBitopRef(5, ...); end;
    arshift = function(...) return garnetBitopRef(6, ...); end;
    rol = function(...) return garnetBitopRef(7, ...); end;
    ror = function(...) return garnetBitopRef(8, ...); end;
    bswap = chain_func(error_wrapper_r1, garnet_bit_bswap);
}

-- define cmsgpack for (optional) inclusion into sandbox_env
local cmsgpack = {
    pack = chain_func(error_wrapper_r1, garnet_cmsgpack_pack);
    unpack = chain_func(error_wrapper_rvar, garnet_cmsgpack_unpack);
}

-- define struct for (optional) inclusion into sandbox_env
local struct = {
    pack = chain_func(error_wrapper_r1, garnet_struct_pack);
    unpack = chain_func(error_wrapper_rvar, garnet_struct_unpack);
    size = chain_func(error_wrapper_r1, garnet_struct_size);
}

-- define redis for (optional, but almost always) inclusion into sandbox_env
local garnetCallRef = chain_func(error_wrapper_r1, garnet_call)
local pCallRef = pcall
local redis = {
    status_reply = function(text)
        return text
    end,

    error_reply = function(text)
        return { err = 'ERR ' .. text }
    end,

    call = garnetCallRef,

    pcall = function(...)
        local success, errOrRes = pCallRef(garnetCallRef, ...)
        if success then
            return errOrRes
        end

        return { err = errOrRes }
    end,

    sha1hex = chain_func(error_wrapper_r1, garnet_sha1hex),

    LOG_DEBUG = 0,
    LOG_VERBOSE = 1,
    LOG_NOTICE = 2,
    LOG_WARNING = 3,

    log = chain_func(error_wrapper_r0, garnet_log),

    REPL_ALL = 3,
    REPL_AOF = 1,
    REPL_REPLICA = 2,
    REPL_SLAVE = 2,
    REPL_NONE = 0,

    set_repl = function(...)
        -- this is a giant footgun, straight up not implementing it
        error('ERR redis.set_repl is not supported in Garnet', 0)
    end,

    replicate_commands = function(...)
        return true
    end,

    breakpoint = function(...)
        -- this is giant and weird, not implementing
        error('ERR redis.breakpoint is not supported in Garnet', 0)
    end,

    debug = function(...)
        -- this is giant and weird, not implementing
        error('ERR redis.debug is not supported in Garnet', 0)
    end,

    acl_check_cmd = chain_func(error_wrapper_r1, garnet_acl_check_cmd),
    setresp = chain_func(error_wrapper_r0, garnet_setresp),

    REDIS_VERSION = garnet_REDIS_VERSION,
    REDIS_VERSION_NUM = garnet_REDIS_VERSION_NUM
}

-- added after Lua 5.1, removing to maintain Redis compat
string.pack = nil
string.unpack = nil
string.packsize = nil
math.maxinteger = nil
math.type = nil
math.mininteger = nil
math.tointeger = nil
math.ult = nil
table.pack = nil
table.unpack = nil
table.move = nil

-- in Lua 5.1 but not 5.4, so implemented on the .NET side
local loadstring = chain_func(error_wrapper_r2, garnet_loadstring)
math.atan2 = chain_func(error_wrapper_r1, garnet_atan2)
math.cosh = chain_func(error_wrapper_r1, garnet_cosh)
math.frexp = chain_func(error_wrapper_r2, garnet_frexp)
math.ldexp = chain_func(error_wrapper_r1, garnet_ldexp)
math.log10 = chain_func(error_wrapper_r1, garnet_log10)
math.pow = chain_func(error_wrapper_r1, garnet_pow)
math.sinh = chain_func(error_wrapper_r1, garnet_sinh)
math.tanh = chain_func(error_wrapper_r1, garnet_tanh)
table.maxn = chain_func(error_wrapper_r1, garnet_maxn)

local collectgarbageRef = collectgarbage
local setMetatableRef = setmetatable
local rawsetRef = rawset

-- prevent modification to metatables for readonly tables
-- Redis accomplishes this by patching Lua, we'd rather ship
-- vanilla Lua and do it in code
local setmetatable = function(table, metatable)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

    return setMetatableRef(table, metatable)
end

-- prevent bypassing metatables to update readonly tables
-- as above, Redis prevents this with a patch to Lua
local rawset = function(table, key, value)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

    return rawsetRef(table, key, value)
end

-- technically deprecated in 5.1, but available in Redis
-- this is only 'sort of' correct as 5.4 doesn't expose the same
-- gc primitives
local gcinfo = function()
    return collectgarbageRef('count'), 0
end

-- global object used for the sandbox environment
--
-- replacements are performed before VM initialization
-- to allow configuring available functions
sandbox_env = {
    _VERSION = _VERSION;

    KEYS = KEYS;
    ARGV = ARGV;

!!SANDBOX_ENV REPLACEMENT TARGET!!
}
-- timeout error must be raised on Lua
local debugRef = debug
local force_timeout = function()
    error('ERR Lua script exceeded configured timeout', 0)
end
function request_timeout()
    debugRef.sethook(force_timeout, '', 1)
end
-- no reference to outermost set of globals (_G) should survive sandboxing
sandbox_env._G = sandbox_env
-- lock down a table, recursively doing the same to all table members
local rawGetRef = rawget
local readonly_metatable = {
    __index = function(onTable, key)
        return rawGetRef(onTable, key)
    end,
    __newindex = function(onTable, key, value)
        error('Attempt to modify a readonly table', 0)
    end
}
function recursively_readonly_table(table)
    if table.__readonly then
        return table
    end

    table.__readonly = true

    for key, value in pairs(table) do
        if type(value) == 'table' and key ~= 'KEYS' and key ~= 'ARGV' then
            recursively_readonly_table(value)
        end
    end

    setMetatableRef(table, readonly_metatable)
end
-- do resets in the Lua side to minimize pinvokes
function reset_keys_and_argv(fromKey, fromArgv)
    local keyRef = sandbox_env.KEYS
    local keyCount = #keyRef
    for i = fromKey, keyCount do
        table.remove(keyRef)
    end

    local argvRef = sandbox_env.ARGV
    local argvCount = #argvRef
    for i = fromArgv, argvCount do
        table.remove(argvRef)
    end
end
-- force new 'global' environment to be readonly
recursively_readonly_table(sandbox_env)
-- responsible for sandboxing user provided code
function load_sandboxed(source)
    local rawFunc, err = load(source, nil, nil, sandbox_env)

    return err, rawFunc
end
";
        internal static readonly HashSet<string> DefaultAllowedFunctions = [
            // Built ins
            "assert",
            "collectgarbage",
            "coroutine",
            "error",
            "gcinfo",
            // Intentionally not supporting getfenv, as it's too weird to backport to Lua 5.4
            "getmetatable",
            "ipairs",
            "load",
            "loadstring",
            "math",
            "next",
            "pairs",
            "pcall",
            "rawequal",
            "rawget",
            // Note rawset is proxied to implement readonly tables
            "rawset",
            "select",
            // Intentionally not supporting setfenv, as it's too weird to backport to Lua 5.4
            // Note setmetatable is proxied to implement readonly tables
            "setmetatable",
            "string",
            "table",
            "tonumber",
            "tostring",
            "type",
            // Note unpack is actually table.unpack, and defined in the loader block
            "unpack",
            "xpcall",

            // Runtime libs
            "bit",
            "cjson",
            "cmsgpack",
            // Note os only contains clock due to definition in the loader block
            "os",
            // Note struct is actually implemented by Lua 5.4's string.pack/unpack/size
            "struct",

            // Interface force communicating back with Garnet
            "redis",
        ];

        /// <summary>
        /// Most recently generated loader block.
        /// 
        /// Since this typically does not change, we keep it around for future sessions.
        /// </summary>
        private static LoaderBlockCache CachedLoaderBlock;

        /// <summary>
        /// Modifies <see cref="LoaderBlock"/> to account for <paramref name="allowedFunctions"/>, and converts to ops for future loading.
        /// </summary>
        internal static ReadOnlyMemory<byte> PrepareLoaderBlockBytes(HashSet<string> allowedFunctions, ILogger logger)
        {
            // If nothing is explicitly allowed, fallback to our defaults
            if (allowedFunctions.Count == 0)
            {
                allowedFunctions = DefaultAllowedFunctions;
            }

            // Most of the time this list never changes, so reuse the work
            var cache = CachedLoaderBlock;
            if (cache != null && ReferenceEquals(cache.AllowedFunctions, allowedFunctions))
            {
                return cache.LoaderBlockBytes;
            }

            // Build the subset of a Lua table where we export all these functions
            var wholeIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var replacement = new StringBuilder();
            foreach (var wholeRef in allowedFunctions.Where(static x => !x.Contains('.')))
            {
                if (!DefaultAllowedFunctions.Contains(wholeRef))
                {
                    // Skip functions not intentionally exported
                    continue;
                }

                _ = replacement.AppendLine($"    {wholeRef}={wholeRef};");
                _ = wholeIncludes.Add(wholeRef);
            }

            // Partial includes (ie. os.clock) need special handling
            var partialIncludes = allowedFunctions.Where(static x => x.Contains('.')).Select(static x => (Leading: x[..x.IndexOf('.')], Trailing: x[(x.IndexOf('.') + 1)..]));
            foreach (var grouped in partialIncludes.GroupBy(static t => t.Leading, StringComparer.OrdinalIgnoreCase))
            {
                if (wholeIncludes.Contains(grouped.Key))
                {
                    // Including a subset of something included in whole doesn't affect things
                    continue;
                }

                if (!DefaultAllowedFunctions.Contains(grouped.Key))
                {
                    // Skip functions not intentionally exported
                    continue;
                }

                _ = replacement.AppendLine($"    {grouped.Key}={{");
                foreach (var part in grouped.Select(static t => t.Trailing).Distinct().OrderBy(static t => t))
                {
                    _ = replacement.AppendLine($"        {part}={grouped.Key}.{part};");
                }
                _ = replacement.AppendLine("    };");
            }

            var decl = replacement.ToString();
            var finalLoaderBlock = LoaderBlock.Replace("!!SANDBOX_ENV REPLACEMENT TARGET!!", decl);

            // Compile bytecode is smaller and faster to load, so pay for the compilation once up front
            byte[] asOps;
            using (var compilingState = new LuaStateWrapper(LuaMemoryManagementMode.Native, null, logger))
            {
                // This is equivalent to:
                // string.load(<loader block>, true)

                compilingState.GetGlobal(LuaType.Table, "string\0"u8);
                if (!compilingState.TryPushBuffer("dump"u8))
                {
                    throw new Exception("Loading string should not fail");
                }
                _ = compilingState.RawGet(LuaType.Function, 1);

                compilingState.Remove(1);

                if (compilingState.LoadString(Encoding.UTF8.GetBytes(finalLoaderBlock)) != LuaStatus.OK)
                {
                    throw new InvalidOperationException("Compiling function should not fail");
                }

                compilingState.PushBoolean(true);

                if (compilingState.PCall(2, 1) != LuaStatus.OK)
                {
                    throw new InvalidOperationException("Dumping function should not fail");
                }

                compilingState.KnownStringToBuffer(1, out var ops);

                asOps = ops.ToArray();
            }

            // Save off for next caller
            //
            // Inherently race-y, but that's fine - worst case we do a little extra work
            var newCache = new LoaderBlockCache(allowedFunctions, asOps);
            CachedLoaderBlock = newCache;

            return newCache.LoaderBlockBytes;
        }

        /// <summary>
        /// Take a chunk of Lua code and convert it to binary ops.
        /// 
        /// These ops are faster to load into a runtime than parsing the whole source file again.
        /// </summary>
        internal static byte[] CompileSource(ReadOnlySpan<byte> source)
        {
            // This is equivalent to calling 
            //
            // string.dump(<function equivalent to source>, true)
            //
            // Which gives us the opcode version of source on the stack

            using var state = new LuaStateWrapper(LuaMemoryManagementMode.Native, null, null);

            state.GetGlobal(LuaType.Table, "string\0"u8);
            var pushRes = state.TryPushBuffer("dump"u8);
            Debug.Assert(pushRes, "Pushing 'dump' should never fail");
            _ = state.RawGet(LuaType.Function, 1);

            state.Remove(1);

            if (state.LoadString(source) != LuaStatus.OK)
            {
                // If we're going to fail, just keep the source as is - a future load attempt will fail it too
                return source.ToArray();
            }

            state.PushBoolean(true);

            if (state.PCall(2, 1) != LuaStatus.OK)
            {
                // If we're going to fail, just keep the source as is - a future load attempt will fail it too
                return source.ToArray();
            }

            state.KnownStringToBuffer(1, out var ops);

            return ops.ToArray();
        }
    }
}