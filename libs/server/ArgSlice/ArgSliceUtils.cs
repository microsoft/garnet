// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// ArgSlice utils
    /// </summary>
    public static class ArgSliceUtils
    {
        /// <summary>
        /// Compute hash slot of given ArgSlice
        /// </summary>
        public static unsafe ushort HashSlot(ArgSlice argSlice)
            => NumUtils.HashSlot(argSlice.ptr, argSlice.length);
    }
}