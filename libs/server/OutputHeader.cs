// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Flags for store outputs.
    /// </summary>
    [Flags]
    public enum OutputFlags : byte
    {
        /// <summary>
        /// No flags set
        /// </summary>
        None = 0,

        /// <summary>
        /// Remove key
        /// </summary>
        RemoveKey = 1,

        /// <summary>
        /// Wrong type of value
        /// </summary>
        WrongType = 1 << 1,

        /// <summary>
        /// Operation failed due to NaN/infinity
        /// </summary>
        NanOrInfinity
    }

    /// <summary>
    /// Object output header (sometimes used as footer)
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct OutputHeader
    {
        /// <summary>
        /// Expected size of this struct
        /// </summary>
        public const int Size = 4;

        /// <summary>
        /// Some result of operation (e.g., number of items added successfully)
        /// </summary>
        [FieldOffset(0)]
        public int result1;
    }
}