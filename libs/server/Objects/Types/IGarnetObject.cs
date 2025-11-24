// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Interface representing Garnet object
    /// </summary>
    public interface IGarnetObject : IHeapObject
    {
        /// <summary>
        /// Type of object
        /// </summary>
        byte Type { get; }

        /// <summary>
        /// Operator on object
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="sizeChange"></param>
        /// <returns></returns>
        bool Operate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion, out long sizeChange);

        /// <summary>
        /// Scan the items of the collection
        /// </summary>
        /// <param name="start">Shift the scan to this index</param>
        /// <param name="items">The matching items in the collection</param>
        /// <param name="cursor">The cursor in the current page</param>
        /// <param name="count">The number of items being taken in one iteration</param>
        /// <param name="pattern">A patter used to match the members of the collection</param>
        /// <param name="patternLength">The number of characters in the pattern</param>
        /// <returns></returns>
        unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false);
    }
}