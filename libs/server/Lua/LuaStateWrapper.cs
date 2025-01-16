// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using KeraLua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// For performance purposes, we need to track some additional state alongside the
    /// raw Lua runtime.
    /// 
    /// This type does that.
    /// </summary>
    internal struct LuaStateWrapper : IDisposable
    {
        private const int LUA_MINSTACK = 20;

        private GCHandle customAllocatorHandle;

        private nint state;
        private int curStackSize;

        /// <summary>
        /// Current top item in the stack.
        /// 
        /// 0 implies the stack is empty.
        /// </summary>
        internal int StackTop { get; private set; }

        internal unsafe LuaStateWrapper(LuaMemoryManagementMode memMode, int? memLimitBytes, ILogger logger)
        {
            // As an emergency thing, we need a logger (any logger) if Lua is going to panic
            // TODO: Consider a better way to do this?
            LuaStateWrapperTrampolines.PanicLogger ??= logger;

            ILuaAllocator customAllocator =
                (memMode, memLimitBytes) switch
                {
                    (LuaMemoryManagementMode.Native, null) => null,
                    (LuaMemoryManagementMode.Tracked, _) => new LuaTrackedAllocator(memLimitBytes),
                    (LuaMemoryManagementMode.Managed, null) => new LuaManagedAllocator(),
                    (LuaMemoryManagementMode.Managed, _) => new LuaLimitedManagedAllocator(memLimitBytes.Value),
                    _ => throw new InvalidOperationException($"Unexpected mode/limit combination: {memMode}/{memLimitBytes}")
                };

            if (customAllocator != null)
            {
                customAllocatorHandle = GCHandle.Alloc(customAllocator, GCHandleType.Normal);
                var stateUserData = (nint)customAllocatorHandle;

                state = NativeMethods.NewState(&LuaStateWrapperTrampolines.LuaAllocateBytes, stateUserData);
            }
            else
            {
                state = NativeMethods.NewState();
            }

            NativeMethods.OpenLibs(state);

            _ = NativeMethods.AtPanic(state, &LuaStateWrapperTrampolines.LuaAtPanic);

            curStackSize = LUA_MINSTACK;
            StackTop = 0;

            AssertLuaStackExpected();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (state != 0)
            {
                NativeMethods.Close(state);
                state = 0;
            }

            if (customAllocatorHandle.IsAllocated)
            {
                customAllocatorHandle.Free();
                customAllocatorHandle = default;
            }
        }

        /// <summary>
        /// Call when ambient state indicates that the Lua stack is in fact empty.
        /// 
        /// Maintains <see cref="StackTop"/> to avoid unnecessary p/invokes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExpectLuaStackEmpty()
        {
            StackTop = 0;
            AssertLuaStackExpected();
        }

        /// <summary>
        /// Ensure there's enough space on the Lua stack for <paramref name="additionalCapacity"/> more items.
        /// 
        /// Throws if there is not.
        /// 
        /// Maintains <see cref="StackTop"/> to avoid unnecessary p/invokes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ForceMinimumStackCapacity(int additionalCapacity)
        {
            var availableSpace = curStackSize - StackTop;

            if (availableSpace >= additionalCapacity)
            {
                return;
            }

            var needed = additionalCapacity - availableSpace;
            if (!NativeMethods.CheckStack(state, needed))
            {
                throw new GarnetException("Could not reserve additional capacity on the Lua stack");
            }

            curStackSize += additionalCapacity;
        }

        /// <summary>
        /// Call when the Lua runtime calls back into .NET code.
        /// 
        /// Figures out the state of the Lua stack once, to avoid unnecessary p/invokes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CallFromLuaEntered(IntPtr luaStatePtr)
        {
            Debug.Assert(luaStatePtr == state, "Unexpected Lua state presented");

            StackTop = NativeMethods.GetTop(state);
            curStackSize = StackTop > LUA_MINSTACK ? StackTop : LUA_MINSTACK;
        }

        /// <summary>
        /// This should be used for all CheckBuffer calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool CheckBuffer(int index, out ReadOnlySpan<byte> str)
        {
            AssertLuaStackIndexInBounds(index);

            return NativeMethods.CheckBuffer(state, index, out str);
        }

        /// <summary>
        /// This should be used for all Type calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly LuaType Type(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.Type(state, stackIndex);
        }

        /// <summary>
        /// This should be used for all PushBuffer calls into Lua.
        /// 
        /// If the string is a constant, consider registering it in the constructor and using <see cref="PushConstantString"/> instead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushBuffer(ReadOnlySpan<byte> buffer)
        {
            AssertLuaStackNotFull();

            _ = ref NativeMethods.PushBuffer(state, buffer);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushNil calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushNil()
        {
            AssertLuaStackNotFull();

            NativeMethods.PushNil(state);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushInteger calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushInteger(long number)
        {
            AssertLuaStackNotFull();

            NativeMethods.PushInteger(state, number);

            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushBoolean calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushBoolean(bool b)
        {
            AssertLuaStackNotFull();

            NativeMethods.PushBoolean(state, b);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all Pop calls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Pop(int num)
        {
            NativeMethods.Pop(state, num);

            UpdateStackTop(-num);
        }

        /// <summary>
        /// This should be used for all PCalls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LuaStatus PCall(int args, int rets)
        {
            // We have to copy this off, as once we PCall curStackTop could be modified
            var oldStackTop = StackTop;

            var res = NativeMethods.PCall(state, args, rets);

            if (res != LuaStatus.OK || rets < 0)
            {
                StackTop = NativeMethods.GetTop(state);
                AssertLuaStackExpected();
            }
            else
            {
                var newPosition = oldStackTop - (args + 1) + rets;
                var update = newPosition - StackTop;
                UpdateStackTop(update);
            }

            return res;
        }

        /// <summary>
        /// This should be used for all Calls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Call(int args, int rets)
        {
            // We have to copy this off, as once we PCall curStackTop could be modified
            var oldStackTop = StackTop;

            NativeMethods.Call(state, args, rets);

            if (rets < 0)
            {
                StackTop = NativeMethods.GetTop(state);
                AssertLuaStackExpected();
            }
            else
            {
                var newPosition = oldStackTop - (args + 1) + rets;
                var update = newPosition - StackTop;
                UpdateStackTop(update);
            }
        }

        /// <summary>
        /// This should be used for all RawSetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RawSetInteger(int stackIndex, int tableIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            NativeMethods.RawSetInteger(state, stackIndex, tableIndex);
            UpdateStackTop(-1);
        }

        /// <summary>
        /// This should be used for all RawGetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LuaType RawGetInteger(LuaType? expectedType, int stackIndex, int tableIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);
            AssertLuaStackNotFull();

            var actual = NativeMethods.RawGetInteger(state, stackIndex, tableIndex);
            Debug.Assert(expectedType == null || actual == expectedType, "Unexpected type received");

            UpdateStackTop(1);

            return actual;
        }

        /// <summary>
        /// This should be used for all RawGets into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly LuaType RawGet(LuaType? expectedType, int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            var actual = NativeMethods.RawGet(state, stackIndex);
            Debug.Assert(expectedType == null || actual == expectedType, "Unexpected type received");

            AssertLuaStackExpected();

            return actual;
        }

        /// <summary>
        /// This should be used for all Refs into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int Ref()
        {
            var ret = NativeMethods.Ref(state, (int)LuaRegistry.Index);
            UpdateStackTop(-1);

            return ret;
        }

        /// <summary>
        /// This should be used for all Unrefs into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void Unref(LuaRegistry registry, int reference)
        => NativeMethods.Unref(state, (int)registry, reference);

        /// <summary>
        /// This should be used for all CreateTables into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CreateTable(int numArr, int numRec)
        {
            AssertLuaStackNotFull();

            NativeMethods.CreateTable(state, numArr, numRec);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all GetGlobals into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        internal void GetGlobal(LuaType expectedType, ReadOnlySpan<byte> nullTerminatedGlobalName)
        {
            AssertLuaStackNotFull();

            var type = NativeMethods.GetGlobal(state, nullTerminatedGlobalName);
            Debug.Assert(type == expectedType, "Unexpected type received");

            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all LoadBuffers into Lua.
        /// 
        /// Note that this is different from pushing a buffer, as the loaded buffer is compiled.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LuaStatus LoadBuffer(ReadOnlySpan<byte> buffer)
        {
            AssertLuaStackNotFull();

            var ret = NativeMethods.LoadBuffer(state, buffer);

            UpdateStackTop(1);

            return ret;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void KnownStringToBuffer(int stackIndex, out ReadOnlySpan<byte> str)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            Debug.Assert(Type(stackIndex) is LuaType.String or LuaType.Number, "Called with non-string, non-number");

            NativeMethods.KnownStringToBuffer(state, stackIndex, out str);
        }

        /// <summary>
        /// This should be used for all CheckNumbers into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly double CheckNumber(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.CheckNumber(state, stackIndex);
        }

        /// <summary>
        /// This should be used for all ToBooleans into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool ToBoolean(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.ToBoolean(state, stackIndex);
        }

        /// <summary>
        /// This should be used for all RawLens into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long RawLen(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.RawLen(state, stackIndex);
        }

        /// <summary>
        /// This should be used for all PushCFunctions into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void PushCFunction(delegate* unmanaged[Cdecl]<nint, int> function)
        {
            NativeMethods.PushCFunction(state, (nint)function);
            UpdateStackTop(1);
        }

        /// <summary>
        /// Call to register a function in the Lua global namespace.
        /// </summary>
        internal unsafe void Register(ReadOnlySpan<byte> nullTerminatedName, delegate* unmanaged[Cdecl]<nint, int> function)
        {
            PushCFunction(function);

            NativeMethods.SetGlobal(state, nullTerminatedName);
            UpdateStackTop(-1);
        }

        /// <summary>
        /// This should be used to push all known constants strings into Lua.
        /// 
        /// This avoids extra copying of data between .NET and Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushConstantString(int constStringRegistryIndex)
        => RawGetInteger(LuaType.String, (int)LuaRegistry.Index, constStringRegistryIndex);

        // Rarely used

        /// <summary>
        /// Remove everything from the Lua stack.
        /// </summary>
        internal void ClearStack()
        {
            NativeMethods.SetTop(state, 0);
            StackTop = 0;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// Clear the stack and raise an error with the given message.
        /// </summary>
        internal int RaiseError(string msg)
        {
            ClearStack();

            var b = Encoding.UTF8.GetBytes(msg);
            PushBuffer(b);
            return RaiseErrorFromStack();
        }

        /// <summary>
        /// Raise an error, where the top of the stack is the error message.
        /// </summary>
        internal readonly int RaiseErrorFromStack()
        {
            Debug.Assert(StackTop != 0, "Expected error message on the stack");

            return NativeMethods.Error(state);
        }

        /// <summary>
        /// Helper to update <see cref="StackTop"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateStackTop(int by)
        {
            StackTop += by;
            AssertLuaStackExpected();
        }

        // Conditional compilation checks

        /// <summary>
        /// Check that the given index refers to a valid part of the stack.
        /// </summary>
        [Conditional("DEBUG")]
        private readonly void AssertLuaStackIndexInBounds(int stackIndex)
        {
            Debug.Assert(stackIndex == (int)LuaRegistry.Index || (stackIndex > 0 && stackIndex <= StackTop), "Lua stack index out of bounds");
        }

        /// <summary>
        /// Check that the Lua stack top is where expected in DEBUG builds.
        /// </summary>
        [Conditional("DEBUG")]
        private readonly void AssertLuaStackExpected()
        {
            Debug.Assert(NativeMethods.GetTop(state) == StackTop, "Lua stack not where expected");
        }

        /// <summary>
        /// Check that there's space to push some number of elements.
        /// </summary>
        [Conditional("DEBUG")]
        private readonly void AssertLuaStackNotFull(int probe = 1)
        {
            Debug.Assert((StackTop + probe) <= curStackSize, "Lua stack should have been grown before pushing");
        }
    }

    /// <summary>
    /// Holds static functions for Lua-to-.NET interop.
    /// 
    /// We annotate these as "unmanaged callers only" as a micro-optimization.
    /// See: https://devblogs.microsoft.com/dotnet/improvements-in-native-code-interop-in-net-5-0/#unmanagedcallersonly
    /// </summary>
    internal static class LuaStateWrapperTrampolines
    {
        /// <summary>
        /// Controls the singular logger that will be used for panic invocations.
        /// 
        /// Because Lua is panic'ing, we're about to crash.  This being process wide is hacky,
        /// but we're in hacky situtation.
        /// </summary>
        internal static ILogger PanicLogger { get; set; }

        /// <summary>
        /// Called when Lua encounters an unrecoverable error.
        /// 
        /// When this returns, Lua is going to terminate the process, so plan accordingly.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int LuaAtPanic(nint luaState)
        {
            // See: https://www.lua.org/manual/5.4/manual.html#4.4

            string errorMsg;

            var stackTop = NativeMethods.GetTop(luaState);
            if (stackTop >= 1)
            {
                if (NativeMethods.CheckBuffer(luaState, stackTop, out var msgBuf))
                {
                    errorMsg = Encoding.UTF8.GetString(msgBuf);
                }
                else
                {
                    var type = NativeMethods.Type(luaState, stackTop);

                    errorMsg = $"Unexpected error type: {type}";
                }
            }
            else
            {
                errorMsg = "No error on stack";
            }

            PanicLogger?.LogCritical("Lua Panic '{errorMsg}', stack size {stackTop}", errorMsg, stackTop);

            return 0;
        }

        /// <summary>
        /// Provides data for Lua allocations.
        /// 
        /// All returns data must be PINNED or unmanaged, Lua does not allow it to move.
        /// 
        /// If allocation cannot be performed, null is returned.
        /// </summary>
        /// <param name="udPtr">Pointer to user data provided during <see cref="Lua.SetAllocFunction"/></param>
        /// <param name="ptr">Either null (if new alloc) or pointer to existing allocation being resized or freed.</param>
        /// <param name="osize">If <paramref name="ptr"/> is not null, the <paramref name="nsize"/> value passed when allocation was obtained or resized.</param>
        /// <param name="nsize">The desired size of the allocation, in bytes.</param>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static unsafe nint LuaAllocateBytes(nint udPtr, nint ptr, nuint osize, nuint nsize)
        {
            // See: https://www.lua.org/manual/5.4/manual.html#lua_Alloc

            var handle = (GCHandle)udPtr;
            Debug.Assert(handle.IsAllocated, "GCHandle should always be valid");

            var customAllocator = (ILuaAllocator)handle.Target;

            if (ptr != IntPtr.Zero)
            {
                // Now osize is the size used to (re)allocate ptr last

                ref var dataRef = ref Unsafe.AsRef<byte>((void*)ptr);

                if (nsize == 0)
                {
                    customAllocator.Free(ref dataRef, (int)osize);

                    return 0;
                }
                else
                {
                    ref var ret = ref customAllocator.ResizeAllocation(ref dataRef, (int)osize, (int)nsize, out var failed);
                    if (failed)
                    {
                        return 0;
                    }

                    var retPtr = (nint)Unsafe.AsPointer(ref ret);

                    return retPtr;
                }
            }
            else
            {
                // Now osize is the size of the object being allocated, but nsize is the desired size

                ref var ret = ref customAllocator.AllocateNew((int)nsize, out var failed);
                if (failed)
                {
                    return 0;
                }

                var retPtr = (nint)Unsafe.AsPointer(ref ret);

                return retPtr;
            }
        }
    }
}