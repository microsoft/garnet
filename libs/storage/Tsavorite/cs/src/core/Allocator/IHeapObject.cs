// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// This is the base interface from which any value of object type must derive for <see name="ObjectAllocator"/>.
    /// </summary>
    public interface IHeapObject : IDisposable
    {
        /// <summary>
        /// Total size of the object in memory, including .NET object overheads.
        /// </summary>
        long MemorySize { get; set; }

        /// <summary>
        /// Total serialized size of the object; the size it will take when written to disk or other storage.
        /// </summary>
        long DiskSize { get; set; }
    }
}
