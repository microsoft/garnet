// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    public abstract class CustomObjectFunctions
    {
        /// <summary>
        /// Create output as simple string, from given string
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="simpleString">The simple string to write.</param>
        protected static void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, string simpleString) => CustomCommandUtils.WriteSimpleString(ref output, simpleString);

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="bulkString">The bulk string to write.</param>
        protected static void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString) => CustomCommandUtils.WriteBulkString(ref output, bulkString);

        /// <summary>
        /// Create output as bulk string, from given IEnumerable of byte arrays
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="bulkStrings">The bulk strings to write.</param>
        protected static void WriteBulkString(ref (IMemoryOwner<byte>, int) output, IEnumerable<byte[]> bulkStrings) => CustomCommandUtils.WriteBulkString(ref output, bulkStrings);

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        protected static void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output) => CustomCommandUtils.WriteNullBulkString(ref output);

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="errorMessage">The error message to write.</param>
        protected static void WriteError(ref (IMemoryOwner<byte>, int) output, string errorMessage) => CustomCommandUtils.WriteError(ref output, errorMessage);

        /// <summary>
        /// Create output as error message, from given ReadOnlySpan
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="errorMessage">The error message to write.</param>
        protected static void WriteError(ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<byte> errorMessage) => CustomCommandUtils.WriteError(ref output, errorMessage);

        /// <summary>
        /// Writes the specified bytes directly to the output.
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="bytes">The bytes to write.</param>
        protected static void WriteDirect(ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<byte> bytes) => CustomCommandUtils.WriteDirect(ref output, bytes);

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Object Store input</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        protected static ReadOnlySpan<byte> GetNextArg(ref ObjectInput input, scoped ref int offset) => CustomCommandUtils.GetNextArg(ref input, ref offset);

        /// <summary>
        /// Get argument from input as string, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Object Store input</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a string</returns>
        protected static string GetNextString(ref ObjectInput input, scoped ref int offset) => Encoding.UTF8.GetString(CustomCommandUtils.GetNextArg(ref input, ref offset));

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input">Object Store input</param>
        /// <returns>First argument as a span</returns>
        protected static ReadOnlySpan<byte> GetFirstArg(ref ObjectInput input) => CustomCommandUtils.GetFirstArg(ref input);

        /// <summary>
        /// Whether we need an initial update, given input, if item does not already exist in store
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        /// <returns>True if an initial update is needed, otherwise false</returns>
        public virtual bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref (IMemoryOwner<byte>, int) output) => throw new NotImplementedException();

        /// <summary>
        /// Create initial value, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we need to cancel the update</returns>
        public virtual bool InitialUpdater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => Updater(key, ref input, value, ref output, ref rmwInfo);

        /// <summary>
        /// Update given value in place, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public virtual bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        /// <summary>
        /// Read value, given key and input and generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="output">Output</param>
        /// <param name="readInfo">Advanced arguments</param>
        /// <returns>True if done, false if not found</returns>
        public virtual bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo) => throw new NotImplementedException();

        /// <summary>
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate a wrong number of arguments for the given command.
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithWrongNumberOfArguments(ref (IMemoryOwner<byte>, int) output, string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName));
            return AbortWithErrorMessage(ref output, errorMessage);
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<byte> errorMessage)
        {
            CustomCommandUtils.WriteError(ref output, errorMessage);
            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(ref (IMemoryOwner<byte>, int) output, string errorMessage)
        {
            CustomCommandUtils.WriteError(ref output, errorMessage);
            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a syntax error message.
        /// </summary>
        /// <param name="output">The output buffer and its length.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithSyntaxError(ref (IMemoryOwner<byte>, int) output)
        {
            CustomCommandUtils.WriteError(ref output, CmdStrings.RESP_SYNTAX_ERROR);
            return true;
        }
    }
}