// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using KeraLua;
using charptr_t = nint;
using intptr_t = nint;
using lua_CFunction = nint;
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

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_newstate
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial nint luaL_newstate();

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_newstate
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial nint lua_newstate(lua_CFunction allocFunc, charptr_t ud);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_openlibs
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void luaL_openlibs(lua_State luaState);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_close
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void lua_close(lua_State luaState);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_checkstack
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_checkstack(lua_State luaState, int n);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_checknumber
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial double luaL_checknumber(lua_State luaState, int n);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_rawlen
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_rawlen(lua_State luaState, int n);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pcallk
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_pcallk(lua_State luaState, int nargs, int nresults, int msgh, nint ctx, nint k);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_callk
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_callk(lua_State luaState, int nargs, int nresults, nint ctx, nint k);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_rawseti
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void lua_rawseti(lua_State luaState, int index, long i);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_rawset
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void lua_rawset(lua_State luaState, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_rawgeti
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_rawgeti(lua_State luaState, int index, long n);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_rawget
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_rawget(lua_State luaState, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_ref
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int luaL_ref(lua_State luaState, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#luaL_unref
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int luaL_unref(lua_State luaState, int index, int refVal);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_createtable
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void lua_createtable(lua_State luaState, int narr, int nrec);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_getglobal
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_getglobal(lua_State luaState, charptr_t name);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_setglobal
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial void lua_setglobal(lua_State luaState, charptr_t name);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_error
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        private static partial int lua_error(lua_State luaState);

        // GC Transition suppressed - only do this after auditing the Lua method and confirming constant-ish, fast, runtime w/o allocations

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_gettop
        /// 
        /// Does basically nothing, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_gettop
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
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
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial LuaType lua_type(lua_State L, int index);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushnil
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushnil
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushnil(lua_State L);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushinteger
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushinteger
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushinteger(lua_State L, long num);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushboolean
        /// 
        /// Does some very small writes, and stack size is pre-validated, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushboolean
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushboolean(lua_State L, int b);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_toboolean
        /// 
        /// Does some very basic ifs and then returns an int, so suppressing GC transition.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_toboolean
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial int lua_toboolean(lua_State L, int ix);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_tointegerx
        /// 
        /// We should always have checked this is actually a number before calling, 
        /// so the expensive paths won't be taken.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_tointegerx
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial long lua_tointegerx(lua_State L, int idex, intptr_t pisnum);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_settop
        /// 
        /// We aren't pushing complex types, so none of the close logic should run.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_settop
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_settop(lua_State L, int num);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_atpanic
        /// 
        /// Just changing a global value, should be quick.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_atpanic
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial lua_CFunction lua_atpanic(lua_State luaState, lua_CFunction panicf);

        /// <summary>
        /// see: https://www.lua.org/manual/5.4/manual.html#lua_pushcclosure
        /// 
        /// We never call this with n != 0, so does very little.
        /// see: https://www.lua.org/source/5.4/lapi.c.html#lua_pushcclosure
        /// </summary>
        [LibraryImport(LuaLibraryName)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl), typeof(CallConvSuppressGCTransition)])]
        private static partial void lua_pushcclosure(lua_State luaState, lua_CFunction fn, int n);

        // Helper methods for using the pinvokes defined above

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
            // See: https://www.lua.org/source/5.4/lapi.c.html#lua_tolstring
            //
            // If lua_tolstring fails, it will set len == 0 and start == NULL
            var start = lua_tolstring(luaState, index, out var len);

            unsafe
            {
                str = new ReadOnlySpan<byte>((byte*)start, (int)len);
                return start != (charptr_t)(void*)null;
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
        internal static unsafe ref byte PushBuffer(lua_State luaState, ReadOnlySpan<byte> str)
        {
            nint inLuaPtr;
            fixed (byte* ptr = str)
            {
                inLuaPtr = lua_pushlstring(luaState, (charptr_t)ptr, (size_t)str.Length);
            }

            return ref Unsafe.AsRef<byte>((void*)inLuaPtr);
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
        /// Read a long off the stack
        /// 
        /// Differs from <see cref="Lua.ToInteger(int)"/> by suppressing GC transition.
        /// </summary>
        internal static long ToInteger(lua_State luaState, int index)
        => lua_tointegerx(luaState, index, 0);

        /// <summary>
        /// Remove some number of items from the stack.
        /// 
        /// Differs form <see cref="Lua.Pop(int)"/> by suppressing GC transition.
        /// </summary>
        internal static void Pop(lua_State luaState, int num)
        => lua_settop(luaState, -num - 1);

        /// <summary>
        /// Update the panic function.
        /// 
        /// Differs from <see cref="Lua.AtPanic(LuaFunction)"/> by taking a function pointer
        /// and suppressing GC transition.
        /// </summary>
        internal static unsafe nint AtPanic(lua_State luaState, delegate* unmanaged[Cdecl]<nint, int> panicFunc)
        => lua_atpanic(luaState, (nint)panicFunc);

        /// <summary>
        /// Create a new Lua state.
        /// </summary>
        internal static unsafe nint NewState()
        => luaL_newstate();

        /// <summary>
        /// Create a new Lua state.
        /// 
        /// Differs from <see cref="Lua.Lua(LuaAlloc, charptr_t)"/> by taking a function pointer.
        /// </summary>
        internal static unsafe nint NewState(delegate* unmanaged[Cdecl]<nint, nint, nuint, nuint, nint> allocFunc, nint ud)
        => lua_newstate((nint)allocFunc, ud);

        /// <summary>
        /// Open all standard Lua libraries.
        /// </summary>
        internal static void OpenLibs(lua_State luaState)
        => luaL_openlibs(luaState);

        /// <summary>
        /// Close the state, releasing all associated resources.
        /// </summary>
        internal static void Close(lua_State luaState)
        => lua_close(luaState);

        /// <summary>
        /// Reserve space on the stack, returning false if that was not possible.
        /// </summary>
        internal static bool CheckStack(lua_State luaState, int n)
        => lua_checkstack(luaState, n) == 1;

        /// <summary>
        /// Read a number, as a double, out of the stack.
        /// </summary>
        internal static double CheckNumber(lua_State luaState, int n)
        => luaL_checknumber(luaState, n);

        /// <summary>
        /// Gets the length of an object on the stack, ignoring metatable methods.
        /// </summary>
        internal static int RawLen(lua_State luaState, int n)
        => lua_rawlen(luaState, n);

        /// <summary>
        /// Push a function onto the stack.
        /// </summary>
        internal static void PushCFunction(lua_State luaState, nint ptr)
        => lua_pushcclosure(luaState, ptr, 0);

        /// <summary>
        /// Perform a protected call with the given number of arguments, expecting the given number of returns.
        /// </summary>
        internal static LuaStatus PCall(lua_State luaState, int nargs, int nrets)
        => (LuaStatus)lua_pcallk(luaState, nargs, nrets, 0, 0, 0);

        /// <summary>
        /// Perform a call with the given number of arguments, expecting the given number of returns.
        /// </summary>
        internal static void Call(lua_State luaState, int nargs, int nrets)
        => lua_callk(luaState, nargs, nrets, 0, 0);

        /// <summary>
        /// Equivalent of t[i] = v, where t is the table at the given index and v is the value on the top of the stack.
        /// 
        /// Ignores metatable methods.
        /// </summary>
        internal static void RawSetInteger(lua_State luaState, int index, long i)
        => lua_rawseti(luaState, index, i);

        /// <summary>
        /// Equivalent to t[k] = v, where t is the value at the given index, v is the value on the top of the stack, and k is the value just below the top.
        /// 
        /// Ignores metatable methods.
        /// </summary>
        internal static void RawSet(lua_State luaState, int index)
        => lua_rawset(luaState, index);

        /// <summary>
        /// Pushes onto the stack the value t[n], where t is the table at the given index.
        /// 
        /// Ignores metatable methods.
        /// </summary>
        internal static LuaType RawGetInteger(lua_State luaState, int index, long n)
        => (LuaType)lua_rawgeti(luaState, index, n);

        /// <summary>
        /// Pushes onto the stack the value t[k], where t is the value at the given index and k is the value on the top of the stack.
        /// 
        /// Ignores metatable methods.
        /// </summary>
        internal static LuaType RawGet(lua_State luaState, int index)
        => (LuaType)lua_rawget(luaState, index);

        /// <summary>
        /// Creates and reference a reference in that table at the given index.
        /// </summary>
        internal static int Ref(lua_State luaState, int index)
        => luaL_ref(luaState, index);

        /// <summary>
        /// Free a ref previously created with <see cref="Ref"/>.
        /// </summary>
        internal static int Unref(lua_State luaState, int index, int refVal)
        => luaL_unref(luaState, index, refVal);

        /// <summary>
        /// Create a new table and push it on the stack.
        /// 
        /// Reserves capacity for the given number of elements and hints at capacity for non-sequence records.
        /// </summary>
        internal static void CreateTable(lua_State luaState, int elements, int records)
        => lua_createtable(luaState, elements, records);

        /// <summary>
        /// Load a global under the given name onto the stack.
        /// </summary>
        internal static unsafe LuaType GetGlobal(lua_State luaState, ReadOnlySpan<byte> nullTerminatedName)
        {
            Debug.Assert(nullTerminatedName[^1] == 0, "Global name must be null terminated");

            fixed (byte* ptr = nullTerminatedName)
            {
                return (LuaType)lua_getglobal(luaState, (nint)ptr);
            }
        }

        /// <summary>
        /// Pops the top item on the stack, and stores it under the given name as a global.
        /// </summary>
        internal static unsafe void SetGlobal(lua_State luaState, ReadOnlySpan<byte> nullTerminatedName)
        {
            Debug.Assert(nullTerminatedName[^1] == 0, "Global name must be null terminated");

            fixed (byte* ptr = nullTerminatedName)
            {
                lua_setglobal(luaState, (nint)ptr);
            }
        }

        /// <summary>
        /// Sets the index of the top elements on the stack.
        /// 
        /// 0 == empty
        /// 
        /// Items above this point can no longer be safely accessed.
        /// </summary>
        internal static void SetTop(lua_State lua_State, int top)
        => lua_settop(lua_State, top);

        /// <summary>
        /// Raise an error, using the top of the stack as an error item.
        /// 
        /// This method never returns, so be careful calling it.
        /// </summary>
        internal static int Error(lua_State luaState)
        => lua_error(luaState);
    }
}