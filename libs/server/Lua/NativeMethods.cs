﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using KeraLua;
using static System.Net.WebRequestMethods;
using charptr_t = System.IntPtr;
using lua_Integer = System.Int64;
using lua_State = System.IntPtr;
using size_t = System.UIntPtr;
using voidptr_t = System.IntPtr;

namespace Garnet.server
{
    /// <summary>
    /// Lua runtime methods we want that are not provided by <see cref="KeraLua.Lua"/>.
    /// 
    /// Long term we'll want to try and push these upstreams and move to just using KeraLua, 
    /// but for now we're just defining them ourselves.
    /// </summary>
    internal static class NativeMethods
    {
        // TODO: LibraryImport?
        // TODO: Suppress GC transition (requires Lua audit)

        private const string LuaLibraryName = "lua54";

        /// <summary>
        /// see: https://www.lua.org/manual/5.3/manual.html#lua_tolstring
        /// </summary>
        [DllImport(LuaLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern charptr_t lua_tolstring(lua_State L, int index, out size_t len);

        /// <summary>
        /// see: https://www.lua.org/manual/5.3/manual.html#lua_type
        /// </summary>
        [DllImport(LuaLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern LuaType lua_type(lua_State L, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.3/manual.html#lua_pushlstring
        /// </summary>
        [DllImport(LuaLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern charptr_t lua_pushlstring(lua_State L, charptr_t s, size_t len);

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

            if (type != LuaType.String && type != LuaType.Number)
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
                lua_pushlstring(luaState, (charptr_t)ptr, (size_t)str.Length);
            }
        }
    }
}
