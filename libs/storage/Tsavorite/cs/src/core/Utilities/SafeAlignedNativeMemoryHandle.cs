// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Tsavorite.core.Utilities
{
    /// <summary>
    /// A SafeHandle wrapper around unmanaged _aligned_ memory.
    /// </summary>
    internal sealed unsafe class SafeNativeMemoryHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        /// <summary>
        /// Gets the length of the allocated memory.
        /// </summary>
        public int Length { get; }

        /// <summary>
        /// Returns a raw pointer to the unmanaged memory.
        /// </summary>
        public unsafe byte* Pointer => (byte*)handle.ToPointer();

        /// <summary>
        /// Initializes a new instance of <see cref="SafeNativeMemoryHandle"/>.
        /// </summary>
        /// <param name="address">The pointer to the unmanaged memory.</param>
        /// <param name="length">The length of the memory block.</param>
        public SafeNativeMemoryHandle(IntPtr address, int length)
            : base(ownsHandle: true)
        {
            SetHandle(address);
            Length = length;
        }

        /// <summary>
        /// Releases the unmanaged memory when the handle is disposed or finalized.
        /// </summary>
        /// <returns>True if the handle was released successfully.</returns>
        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                NativeMemory.AlignedFree((void*)handle);
                GC.RemoveMemoryPressure(Length);
            }
            return true;
        }

        /// <summary>
        /// Allocates aligned unmanaged memory.
        /// </summary>
        /// <param name="byteCount">The total number of bytes to allocate.</param>
        /// <param name="alignment">The alignment in bytes. This must be power of 2.</param>
        /// <returns>A <see cref="SafeNativeMemoryHandle"/> that wraps the allocated memory.</returns>
        public static SafeNativeMemoryHandle Allocate(int byteCount, uint alignment)
        {
            var totalLength = (uint)byteCount + alignment; // TODO: Over allocation for temporarily, fix.
            var ptr = NativeMemory.AlignedAlloc(totalLength, alignment);
            NativeMemory.Clear(ptr, totalLength);
            GC.AddMemoryPressure(byteCount);
            return new SafeNativeMemoryHandle((IntPtr)ptr, byteCount);
        }
    }
}