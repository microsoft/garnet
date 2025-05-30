// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    public abstract class CustomObjectFunctions
    {
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
        /// <param name="writer">Output</param>
        /// <returns>True if an initial update is needed, otherwise false</returns>
        public virtual bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref ObjectInput input, ref RespMemoryWriter writer) => throw new NotImplementedException();

        /// <summary>
        /// Create initial value, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="writer">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we need to cancel the update</returns>
        public virtual bool InitialUpdater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => Updater(key, ref input, value, ref writer, ref rmwInfo);

        /// <summary>
        /// Update given value in place, given key and input. Optionally generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="writer">Output</param>
        /// <param name="rmwInfo">Advanced arguments</param>
        /// <returns>True if done, false if we have no space to update in place</returns>
        public virtual bool Updater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        /// <summary>
        /// Read value, given key and input and generate output for command.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="value">Value</param>
        /// <param name="writer">Output</param>
        /// <param name="readInfo">Advanced arguments</param>
        /// <returns>True if done, false if not found</returns>
        public virtual bool Reader(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref ReadInfo readInfo) => throw new NotImplementedException();

        /// <summary>
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate a wrong number of arguments for the given command.
        /// </summary>
        /// <param name="writer">The output buffer and its length.</param>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithWrongNumberOfArguments(ref RespMemoryWriter writer, string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName));
            return AbortWithErrorMessage(ref writer, errorMessage);
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="writer">The output buffer and its length.</param>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(ref RespMemoryWriter writer, scoped ReadOnlySpan<byte> errorMessage)
        {
            writer.WriteError(errorMessage);
            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="writer">The output buffer and its length.</param>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(ref RespMemoryWriter writer, string errorMessage)
        {
            writer.WriteError(errorMessage);
            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a syntax error message.
        /// </summary>
        /// <param name="writer">The output buffer and its length.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithSyntaxError(ref RespMemoryWriter writer)
        {
            writer.WriteError(CmdStrings.RESP_SYNTAX_ERROR);
            return true;
        }
    }
}