// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using KeraLua;

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

        private readonly Lua state;

        private int curStackSize;

        internal LuaStateWrapper(Lua state)
        {
            this.state = state;

            curStackSize = LUA_MINSTACK;
            StackTop = 0;

            AssertLuaStackExpected();
        }

        /// <inheritdoc/>
        public readonly void Dispose()
        {
            state.Dispose();
        }

        /// <summary>
        /// Current top item in the stack.
        /// 
        /// 0 implies the stack is empty.
        /// </summary>
        internal int StackTop { get; private set; }

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
            if (!state.CheckStack(needed))
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
            Debug.Assert(luaStatePtr == state.Handle, "Unexpected Lua state presented");

            StackTop = NativeMethods.GetTop(state.Handle);
            curStackSize = StackTop > LUA_MINSTACK ? StackTop : LUA_MINSTACK;
        }

        /// <summary>
        /// This should be used for all CheckBuffer calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool CheckBuffer(int index, out ReadOnlySpan<byte> str)
        {
            AssertLuaStackIndexInBounds(index);

            return NativeMethods.CheckBuffer(state.Handle, index, out str);
        }

        /// <summary>
        /// This should be used for all Type calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly LuaType Type(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.Type(state.Handle, stackIndex);
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

            NativeMethods.PushBuffer(state.Handle, buffer);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushNil calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushNil()
        {
            AssertLuaStackNotFull();

            NativeMethods.PushNil(state.Handle);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushInteger calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushInteger(long number)
        {
            AssertLuaStackNotFull();

            NativeMethods.PushInteger(state.Handle, number);

            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all PushBoolean calls into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushBoolean(bool b)
        {
            AssertLuaStackNotFull();

            NativeMethods.PushBoolean(state.Handle, b);
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
            NativeMethods.Pop(state.Handle, num);

            UpdateStackTop(-num);
        }

        /// <summary>
        /// This should be used for all Calls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Call(int args, int rets)
        {
            // We have to copy this off, as once we Call curStackTop could be modified
            var oldStackTop = StackTop;
            state.Call(args, rets);

            if (rets < 0)
            {
                StackTop = NativeMethods.GetTop(state.Handle);
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
        /// This should be used for all PCalls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LuaStatus PCall(int args, int rets)
        {
            // We have to copy this off, as once we Call curStackTop could be modified
            var oldStackTop = StackTop;
            var res = state.PCall(args, rets, 0);

            if (res != LuaStatus.OK || rets < 0)
            {
                StackTop = NativeMethods.GetTop(state.Handle);
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
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RawSetInteger(int stackIndex, int tableIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            state.RawSetInteger(stackIndex, tableIndex);
            UpdateStackTop(-1);
        }

        /// <summary>
        /// This should be used for all RawSets into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RawSet(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            state.RawSet(stackIndex);
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

            var actual = state.RawGetInteger(stackIndex, tableIndex);
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

            var actual = state.RawGet(stackIndex);
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
            var ret = state.Ref(LuaRegistry.Index);
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
        {
            state.Unref(registry, reference);
        }

        /// <summary>
        /// This should be used for all CreateTables into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CreateTable(int numArr, int numRec)
        {
            AssertLuaStackNotFull();

            state.CreateTable(numArr, numRec);
            UpdateStackTop(1);
        }

        /// <summary>
        /// This should be used for all GetGlobals into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="StackTop"/> to minimize p/invoke calls.
        /// </summary>
        internal void GetGlobal(LuaType expectedType, string globalName)
        {
            AssertLuaStackNotFull();

            var type = state.GetGlobal(globalName);
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

            var ret = NativeMethods.LoadBuffer(state.Handle, buffer);

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

            Debug.Assert(NativeMethods.Type(state.Handle, stackIndex) is LuaType.String or LuaType.Number, "Called with non-string, non-number");

            NativeMethods.KnownStringToBuffer(state.Handle, stackIndex, out str);
        }

        /// <summary>
        /// This should be used for all CheckNumbers into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly double CheckNumber(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return state.CheckNumber(stackIndex);
        }

        /// <summary>
        /// This should be used for all ToBooleans into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool ToBoolean(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return NativeMethods.ToBoolean(state.Handle, stackIndex);
        }

        /// <summary>
        /// This should be used for all RawLens into Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long RawLen(int stackIndex)
        {
            AssertLuaStackIndexInBounds(stackIndex);

            return state.RawLen(stackIndex);
        }

        /// <summary>
        /// Call to register a function in the Lua global namespace.
        /// </summary>
        internal readonly void Register(string name, LuaFunction func)
        => state.Register(name, func);

        /// <summary>
        /// This should be used to push all known constants strings into Lua.
        /// 
        /// This avoids extra copying of data between .NET and Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PushConstantString(int constStringRegistryIndex, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        => RawGetInteger(LuaType.String, (int)LuaRegistry.Index, constStringRegistryIndex);

        // Rarely used

        /// <summary>
        /// Remove everything from the Lua stack.
        /// </summary>
        internal void ClearStack()
        {
            state.SetTop(0);
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
            return RaiseErrorFromStack();
        }

        /// <summary>
        /// Raise an error, where the top of the stack is the error message.
        /// </summary>
        internal readonly int RaiseErrorFromStack()
        {
            Debug.Assert(StackTop != 0, "Expected error message on the stack");

            return state.Error();
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
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly void AssertLuaStackIndexInBounds(int stackIndex)
        {
            Debug.Assert(stackIndex == (int)LuaRegistry.Index || (stackIndex > 0 && stackIndex <= StackTop), "Lua stack index out of bounds");
        }

        /// <summary>
        /// Check that the Lua stack top is where expected in DEBUG builds.
        /// </summary>
        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly void AssertLuaStackExpected()
        {
            Debug.Assert(NativeMethods.GetTop(state.Handle) == StackTop, "Lua stack not where expected");
        }

        /// <summary>
        /// Check that there's space to push some number of elements.
        /// </summary>
        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly void AssertLuaStackNotFull(int probe = 1)
        {
            Debug.Assert((StackTop + probe) <= curStackSize, "Lua stack should have been grown before pushing");
        }
    }
}