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
        /// Create output as simple string, from given string
        /// </summary>
        protected static unsafe void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, string simpleString) => CustomCommandUtils.WriteSimpleString(ref output, simpleString);

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString) => CustomCommandUtils.WriteBulkString(ref output, bulkString);

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        protected static unsafe void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output) => CustomCommandUtils.WriteNullBulkString(ref output);

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref (IMemoryOwner<byte>, int) output, string errorMessage) => CustomCommandUtils.WriteError(ref output, errorMessage);

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Input as ReadOnlySpan of byte</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe ReadOnlySpan<byte> GetNextArg(ReadOnlySpan<byte> input, scoped ref int offset) => CustomCommandUtils.GetNextArg(input, ref offset);

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        protected static ReadOnlySpan<byte> GetFirstArg(ReadOnlySpan<byte> input) => CustomCommandUtils.GetFirstArg(input);

        /// <summary>
        /// Whether we need an initial update, given input, if item does not already exist in store
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        public virtual bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output) => throw new NotImplementedException();

        /// <summary>
        /// Create initial value, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we need to cancel the update</returns>
        public virtual bool InitialUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => Updater(key, input, value, ref output, ref rmwInfo);

        /// <summary>
        /// Update given value in place, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public virtual bool Updater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        /// <summary>
        /// Read value, given key and input and generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="readInfo">Advanced arguments</param>
        /// <returns>True if done, false if not found</returns>
        public virtual bool Reader(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo) => throw new NotImplementedException();
    }
}