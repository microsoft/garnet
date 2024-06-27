// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Tsavorite.core;

namespace Garnet.server
{
    public abstract class CustomObjectFunctions
    {
        /// <summary>
        /// Whether we need an initial update, given input, if item does not already exist in store
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        public virtual bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output) => true;

        /// <summary>
        /// Create initial value, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we need to cancel the update</returns>
        public abstract bool InitialUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Update given value in place, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public abstract bool InPlaceUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Update to new value in new location, given key, input, and old value. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="oldValue">Old value</param>
        /// <param name="newValue">New value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public abstract bool CopyUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject oldValue, IGarnetObject newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Read value, given key and input and generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="readInfo">Advanced arguments</param>
        /// <returns>True if done, false if not found</returns>
        public abstract bool Reader(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo);
    }
}
