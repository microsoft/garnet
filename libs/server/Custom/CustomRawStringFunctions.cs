// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom functions on raw strings
    /// </summary>
    public abstract class CustomRawStringFunctions
    {
        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Input as ReadOnlySpan of byte</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe ReadOnlySpan<byte> GetNextArg(ref StringInput input, scoped ref int offset) =>
            CustomCommandUtils.GetNextArg(ref input, ref offset);

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        protected static ReadOnlySpan<byte> GetFirstArg(ref StringInput input) => CustomCommandUtils.GetFirstArg(ref input);

        /// <summary>
        /// Whether we need an initial update, given input, if item does not already exist in store
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="writer">Output</param>
        public virtual bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer) => true;

        /// <summary>
        /// Whether we need to need to perform an update, given old value and input
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="oldValue">Old value</param>
        /// <param name="writer">Output</param>
        public virtual bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer) => true;

        /// <summary>
        /// Length of initial value, given input
        /// </summary>
        /// <param name="input">Input</param>
        /// <returns></returns>
        public abstract int GetInitialLength(ref StringInput input);

        /// <summary>
        /// Length of updated value, given old value and input
        /// </summary>
        /// <param name="value">Old value</param>
        /// <param name="input">Input</param>
        public abstract int GetLength(ReadOnlySpan<byte> value, ref StringInput input);

        /// <summary>
        /// Create initial value, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="writer">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we need to cancel the update</returns>
        public abstract bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo);

        /// <summary>
        /// Update given value in place, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="valueLength">New value length (should be no larger than current length)</param>
        /// <param name="writer">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public abstract bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo);

        /// <summary>
        /// Update to new value in new location, given key, input, and old value. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="oldValue">Old value</param>
        /// <param name="newValue">New value</param>
        /// <param name="writer">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public abstract bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo);

        /// <summary>
        /// Read value, given key and input and generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="writer">Output</param>
        /// <param name="readInfo">Advanced arguments</param>
        /// <returns>True if done, false if not found</returns>
        public abstract bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo);
    }
}