// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using KeraLua;
using charptr_t = nint;
using lua_State = nint;
using size_t = nuint;

namespace Garnet.server
{
    /// <summary>
    /// Lua runtime methods we want that are not provided by <see cref="KeraLua.Lua"/>.
    /// 
    /// Long term we'll want to try and push these upstreams and move to just using KeraLua, 
    /// but for now we're just defining them ourselves.
    /// </summary>
    internal static partial class NativeMethods
    {
        private const string LuaLibraryName = "lua54";

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_tolstring
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial charptr_t lua_tolstring(lua_State L, int index, out size_t len);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushlstring
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial charptr_t lua_pushlstring(lua_State L, charptr_t s, size_t len);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_loadbufferx
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial LuaStatus luaL_loadbufferx(lua_State luaState, charptr_t buff, size_t sz, charptr_t name, charptr_t mode);

        // GC Transition suppressed - only do this after auditing the Lua method and confirming constant-ish, fast, runtime w/o allocations

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_gettop
        /// 
        /// Does basically nothing, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_gettop
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial int lua_gettop(lua_State luaState);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_type
        /// 
        /// Does some very basic ifs and then returns, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_type
        /// And
        /// see: https://www.lua.org/source/5.4/lapi.c.html#index2value
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial LuaType lua_type(lua_State L, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushnil
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushnil
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushnil(lua_State L);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushinteger
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushinteger
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushinteger(lua_State L, long num);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushboolean
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushboolean
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushboolean(lua_State L, int b);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_toboolean
        /// 
        /// Does some very basic ifs and then returns an int, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_toboolean
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial int lua_toboolean(lua_State L, int ix);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_settop
        /// 
        /// We aren't pushing complex types, so none of the close logic should run.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_settop
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_settop(lua_State L, int num);

        /// <summary>
        /// Returns true if the given index on the stack holds a string or a number.
        /// 
        /// Sets <paramref name="str"/> to the string equivalent if so, otherwise leaves it empty.
        /// 
        /// <paramref name="str"/> only remains valid as long as the buffer remains on the stack,
        /// use with care.
        /// 
        /// Note that is changes the value on the stack to be a string if it returns true, regardless of
        /// what it was originally.
        /// </summary>
        internal static bool CheckBuffer(lua_State luaState, int index, out ReadOnlySpan<byte> str)
        {
            var type = lua_type(luaState, index);

            if (type is not LuaType.String and not LuaType.Number)
            {
                str = [];
                return false;
            }

            var start = lua_tolstring(luaState, index, out var len);
            unsafe
            {
                str = new ReadOnlySpan<byte>((byte*)start, (int)len);
                return true;
            }
        }

        /// <summary>
        /// Call when value at index is KNOWN to be a string or number
        /// 
        /// <paramref name="str"/> only remains valid as long as the buffer remains on the stack,
        /// use with care.
        /// 
        /// Note that is changes the value on the stack to be a string if it returns true, regardless of
        /// what it was originally.
        /// </summary>
        internal static void KnownStringToBuffer(lua_State luaState, int index, out ReadOnlySpan<byte> str)
        {
            var start = lua_tolstring(luaState, index, out var len);
            unsafe
            {
                str = new ReadOnlySpan<byte>((byte*)start, (int)len);
            }
        }

        /// <summary>
        /// Pushes given span to stack as a string.
        /// 
        /// Provided data is copied, and can be reused once this call returns.
        /// </summary>
        internal static unsafe void PushBuffer(lua_State luaState, ReadOnlySpan<byte> str)
        {
            fixed (byte* ptr = str)
            {
                _ = lua_pushlstring(luaState, (charptr_t)ptr, (size_t)str.Length);
            }
        }

        /// <summary>
        /// Push given span to stack, and compiles it.
        /// 
        /// Provided data is copied, and can be reused once this call returns.
        /// </summary>
        internal static unsafe LuaStatus LoadBuffer(lua_State luaState, ReadOnlySpan<byte> str)
        {
            fixed (byte* ptr = str)
            {
                return luaL_loadbufferx(luaState, (charptr_t)ptr, (size_t)str.Length, (charptr_t)UIntPtr.Zero, (charptr_t)UIntPtr.Zero);
            }
        }

        /// <summary>
        /// Get the top index on the stack.
        /// 
        /// 0 indicates empty.
        /// 
        /// Differs from <see cref="Lua.GetTop"/> by suppressing GC transition.
        /// </summary>
        internal static int GetTop(lua_State luaState)
        => lua_gettop(luaState);

        /// <summary>
        /// Gets the type of the value at the stack index.
        /// 
        /// Differs from <see cref="Lua.Type(int)"/> by suppressing GC transition.
        /// </summary>
        internal static LuaType Type(lua_State luaState, int index)
        => lua_type(luaState, index);

        /// <summary>
        /// Pushes a nil value onto the stack.
        /// 
        /// Differs from <see cref="Lua.PushNil"/> by suppressing GC transition.
        /// </summary>
        internal static void PushNil(lua_State luaState)
        => lua_pushnil(luaState);

        /// <summary>
        /// Pushes a double onto the stack.
        /// 
        /// Differs from <see cref="Lua.PushInteger"/> by suppressing GC transition.
        /// </summary>
        internal static void PushInteger(lua_State luaState, long num)
        => lua_pushinteger(luaState, num);

        /// <summary>
        /// Pushes a boolean onto the stack.
        /// 
        /// Differs from <see cref="Lua.PushBoolean(bool)"/> by suppressing GC transition.
        /// </summary>
        internal static void PushBoolean(lua_State luaState, bool b)
        => lua_pushboolean(luaState, b ? 1 : 0);

        /// <summary>
        /// Read a boolean off the stack
        /// 
        /// Differs from <see cref="Lua.ToBoolean(int)"/> by suppressing GC transition.
        /// </summary>
        internal static bool ToBoolean(lua_State luaState, int index)
        => lua_toboolean(luaState, index) != 0;

        /// <summary>
        /// Remove some number of items from the stack.
        /// 
        /// Differs form <see cref="Lua.Pop(int)"/> by suppressing GC transition.
        /// </summary>
        internal static void Pop(lua_State luaState, int num)
        => lua_settop(luaState, -num - 1);
    }
}