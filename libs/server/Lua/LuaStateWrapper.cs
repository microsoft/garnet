// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
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
        internal const int LUA_MINSTACK = 20;

        private readonly ILuaAllocator customAllocator;

        private GCHandle customAllocatorHandle;

        private int stateUpdateLock;
        private nint state;
        private int curStackSize;

        /// <summary>
        /// Current top item in the stack.
        /// 
        /// 0 implies the stack is empty.
        /// </summary>
        internal int StackTop { get; private set; }

        /// <summary>
        /// If execution has left this wrapper in a dangerous (but not illegal) state, this will be set.
        /// 
        /// At the next convenient point, this <see cref="LuaStateWrapper"/> should be disposed and recreated.
        /// </summary>
        internal bool NeedsDispose { get; private set; }

        internal unsafe LuaStateWrapper(LuaMemoryManagementMode memMode, int? memLimitBytes, ILogger logger)
        {
            // As an emergency thing, we need a logger (any logger) if Lua is going to panic
            // TODO: Consider a better way to do this?
            LuaStateWrapperTrampolines.PanicLogger ??= logger;

            customAllocator =
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
            // Synchronize with respect to hook'ing
            while (Interlocked.CompareExchange(ref stateUpdateLock, 1, 0) != 0)
            {
                _ = Thread.Yield();
            }

            // make sure we only close once
            if (state != 0)
            {

                NativeMethods.Close(state);
                state = 0;
            }

            _ = Interlocked.Exchange(ref stateUpdateLock, 0);

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
        /// Returns false if space cannot be obtained.
        /// 
        /// Maintains <see cref="StackTop"/> to avoid unnecessary p/invokes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEnsureMinimumStackCapacity(int additionalCapacity)
        {
            var availableSpace = curStackSize - StackTop;

            if (availableSpace >= additionalCapacity)
            {
                return true;
            }

            var needed = additionalCapacity - availableSpace;
            if (!NativeMethods.CheckStack(state, needed))
            {
                return false;
            }

            curStackSize += additionalCapacity;

            return true;
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
        /// Call when the Lua runtime calls back into .NET code AND we know enough details about the caller
        /// to elide some p/invoke.
        /// 
        /// Calls from user provided scripts should go through <see cref="CallFromLuaEntered(nint)"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void KnownCallFromLuaEntered(IntPtr luaStatePtr, [ConstantExpected] int knownArgumentCount)
        {
            Debug.Assert(luaStatePtr == state, "Unexpected Lua state presented");
            Debug.Assert(knownArgumentCount is >= 0 and <= LUA_MINSTACK, "known argument must be >= 0 and <= LUA_MINSTACK");
            Debug.Assert(NativeMethods.GetTop(state) == knownArgumentCount, "knowArgumentCount is incorrect");

            StackTop = knownArgumentCount;
            curStackSize = LUA_MINSTACK;
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
        internal bool TryPushBuffer(ReadOnlySpan<byte> buffer)
        {
            AssertLuaStackNotFull();

            EnterInfallibleAllocationRegion();

            NativeMethods.PushBuffer(state, buffer);
            UpdateStackTop(1);

            return TryExitInfallibleAllocationRegion();
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
        internal void PushNumber(double number)
        {
            AssertLuaStackNotFull();

            NativeMethods.PushNumber(state, number);

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
        /// This should be used for all RawSetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// 
        /// Takes <paramref name="tableArraySize"/>, the size of the array portion of the table being updated.
        /// 
        /// Incorrectly specifying this <paramref name="tableArraySize"/> cause OOMs which lead to crashes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RawSetInteger(int tableArraySize, int stackIndex, int tableIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            Debug.Assert(tableIndex >= 1 && tableIndex <= tableArraySize, "Assigning index in table could cause allocation");

            NativeMethods.RawSetInteger(state, stackIndex, tableIndex);
            UpdateStackTop(-1);
        }

        /// <summary>
        /// This should be used for all RawSet into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RawSet(int tableRecordCount, int stackIndex, ref int setRecordCount)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            setRecordCount++;
            Debug.Assert(tableRecordCount >= 1 && setRecordCount <= tableRecordCount, "Assigning key in table could cause allocation");

            NativeMethods.RawSet(state, stackIndex);
            UpdateStackTop(-2);
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
        /// 
        /// Note this will CRASH if there is insufficient memory to create the ref.
        /// Accordingly, there should only be a fixed number of these calls against any <see cref="LuaStateWrapper"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryRef(out int ret)
        {
            AssertLuaStackNotEmpty();

            EnterInfallibleAllocationRegion();

            ret = NativeMethods.Ref(state, (int)LuaRegistry.Index);
            UpdateStackTop(-1);

            return TryExitInfallibleAllocationRegion();
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
        internal bool TryCreateTable(int numArr, int numRec)
        {
            AssertLuaStackNotFull();

            EnterInfallibleAllocationRegion();

            NativeMethods.CreateTable(state, numArr, numRec);
            UpdateStackTop(1);

            return TryExitInfallibleAllocationRegion();
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
        /// This should be used for all SetGlobals into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        internal bool TrySetGlobal(ReadOnlySpan<byte> nullTerminatedGlobalName)
        {
            AssertLuaStackNotEmpty();

            EnterInfallibleAllocationRegion();

            NativeMethods.SetGlobal(state, nullTerminatedGlobalName);

            UpdateStackTop(-1);

            return TryExitInfallibleAllocationRegion();
        }

        /// <summary>
        /// This should be used for all LoadBuffers into Lua.
        /// 
        /// Note that this is different from pushing a buffer, as the loaded buffer is compiled and executed.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LuaStatus LoadBuffer(ReadOnlySpan<byte> buffer)
        {
            AssertLuaStackNotFull(2);

            // Note that https://www.lua.org/source/5.4/lauxlib.c.html#luaL_loadbufferx is implemented in terms of
            // a PCall, so we don't have to worry about crashes.
            var ret = NativeMethods.LoadBuffer(state, buffer);

            if (ret != LuaStatus.OK)
            {
                StackTop = NativeMethods.GetTop(state);
            }
            else
            {
                UpdateStackTop(1);
            }

            AssertLuaStackExpected();

            return ret;
        }

        /// <summary>
        /// This should be used for all LoadStrings into Lua.
        /// 
        /// Note that this is different from pushing or loading buffer, as the loaded buffer is compiled but NOT executed.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        internal LuaStatus LoadString(ReadOnlySpan<byte> buffer)
        {
            AssertLuaStackNotFull(2);

            // Note that https://www.lua.org/source/5.4/lauxlib.h.html#luaL_loadbuffer is implemented in terms of
            // a PCall, so we don't have to worry about crashes.
            var ret = NativeMethods.LoadString(state, buffer);

            if (ret != LuaStatus.OK)
            {
                StackTop = NativeMethods.GetTop(state);
            }
            else
            {
                UpdateStackTop(1);
            }

            AssertLuaStackExpected();

            return ret;
        }

        /// <summary>
        /// Call to convert a number on the stack to a string in the same slot.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryNumberToString(int stackIndex, out ReadOnlySpan<byte> str)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            Debug.Assert(Type(stackIndex) is LuaType.Number, "Called with non-number");

            EnterInfallibleAllocationRegion();

            var convRes = NativeMethods.CheckBuffer(state, stackIndex, out str);
            Debug.Assert(convRes, "Conversion failed, this should not happen");

            return TryExitInfallibleAllocationRegion();
        }

        /// <summary>
        /// Call when value at index is KNOWN to be a string.
        /// 
        /// <paramref name="str"/> only remains valid as long as the buffer remains on the stack,
        /// use with care.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void KnownStringToBuffer(int stackIndex, out ReadOnlySpan<byte> str)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            Debug.Assert(Type(stackIndex) is LuaType.String, "Called with non-string");

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
            AssertLuaStackNotFull();

            NativeMethods.PushCFunction(state, (nint)function);
            UpdateStackTop(1);
        }

        /// <summary>
        /// Call to register a function in the Lua global namespace.
        /// </summary>
        internal unsafe bool TryRegister(ReadOnlySpan<byte> nullTerminatedName, delegate* unmanaged[Cdecl]<nint, int> function)
        {
            PushCFunction(function);

            EnterInfallibleAllocationRegion();

            NativeMethods.SetGlobal(state, nullTerminatedName);
            UpdateStackTop(-1);

            return TryExitInfallibleAllocationRegion();
        }

        /// <summary>
        /// This should be used to push all known constants strings into Lua.
        /// 
        /// This avoids extra copying of data between .NET and Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushConstantString(int constStringRegistryIndex)
        => RawGetInteger(LuaType.String, (int)LuaRegistry.Index, constStringRegistryIndex);

        /// <summary>
        /// This should be used for all Nexts into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int Next(int tableIndex)
        {
            AssertLuaStackIndexInBounds(tableIndex);

            // Will always remove 1 key, and _may_ push 2 new values for a net growth of 1
            AssertLuaStackNotFull();

            var ret = NativeMethods.Next(state, tableIndex);

            if (ret == 0)
            {
                // Removed key, so net negative one
                UpdateStackTop(-1);
                return ret;
            }


            UpdateStackTop(1);

            return ret;
        }

        /// <summary>
        /// This should be used for all PushValues into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushValue(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);
            AssertLuaStackNotFull();

            NativeMethods.PushValue(state, stackIndex);

            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all Removes into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Remove(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            NativeMethods.Rotate(state, stackIndex, -1);
            NativeMethods.Pop(state, 1);

            UpdateStackTop(-1);
        }

        /// <summary>
        /// This should be used for all Rotates into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void Rotate(int stackIndex, int n)
        {
            AssertLuaStackIndexInBounds(stackIndex);
            Debug.Assert(Math.Abs(n) <= (StackTop - stackIndex), "Rotation cannot be larger than slice being rotated");

            NativeMethods.Rotate(state, stackIndex, n);
        }

        // Rarely used

        /// <summary>
        /// This should be used to set all debug hooks for Lua when multiple threads are involved.
        /// 
        /// This can fail if there's a thread race to close the state.
        /// 
        /// This is the ONLY thread-safe method on <see cref="LuaStateWrapper"/>.
        /// </summary>
        internal unsafe bool TrySetHook(delegate* unmanaged[Cdecl]<nint, nint, void> hook, LuaHookMask mask, int count)
        {
            while (Interlocked.CompareExchange(ref stateUpdateLock, 1, 0) != 0)
            {
                _ = Thread.Yield();
            }

            if (state == 0)
            {
                _ = Interlocked.Exchange(ref stateUpdateLock, 0);

                return false;
            }

            NativeMethods.SetHook(state, hook, mask, count);

            _ = Interlocked.Exchange(ref stateUpdateLock, 0);

            return true;
        }

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
        /// Helper to update <see cref="StackTop"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateStackTop(int by)
        {
            StackTop += by;
            AssertLuaStackExpected();
        }

        /// <summary>
        /// Enter a region where allocation calls against the <see cref="customAllocator"/> cannot fail.
        /// 
        /// Must be paired with a call to <see cref="TryExitInfallibleAllocationRegion"/>, which indicates if
        /// an OOM should be raised.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly void EnterInfallibleAllocationRegion()
        {
            if (customAllocator == null)
            {
                return;
            }

            customAllocator.EnterInfallibleAllocationRegion();
        }

        /// <summary>
        /// Exit a previously entered infallible allocation region.
        /// 
        /// If an allocation occurred that SHOULD have failed, false is returned.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryExitInfallibleAllocationRegion()
        {
            if (customAllocator == null)
            {
                return true;
            }

            var ret = customAllocator.TryExitInfallibleAllocationRegion();

            if (!ret)
            {
                NeedsDispose = true;
            }

            return ret;
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

        /// <summary>
        /// Check that there's space to push some number of elements.
        /// </summary>
        [Conditional("DEBUG")]
        private readonly void AssertLuaStackNotEmpty()
        {
            Debug.Assert(StackTop > 0, "Lua stack should not be empty when called");
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
        /// <param name="udPtr">Pointer to user data provided during allocation function registration</param>
        /// <param name="ptr">Either null (if new alloc) or pointer to existing allocation being resized or freed.</param>
        /// <param name="osize">If <paramref name="ptr"/> is not null, the <paramref name="nsize"/> value passed when allocation was obtained or resized.</param>
        /// <param name="nsize">The desired size of the allocation, in bytes.</param>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static unsafe nint LuaAllocateBytes(nint udPtr, nint ptr, nuint osize, nuint nsize)
        {
            try
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
            catch (Exception e)
            {
                PanicLogger?.LogCritical(e, "Exception raised in LuaAllocateBytes, this is likely to crash the process");

                throw;
            }
        }
    }
}