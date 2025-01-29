// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Text;

namespace Garnet.server
{
    /// <summary>
    /// Provides extension methods for handling custom object output writing operations.
    /// </summary>
    public static class RespCustomObjectOutputWriterExtensions
    {
        /// <summary>
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate a wrong number of arguments for the given command.
        /// </summary>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithWrongNumberOfArguments(this ref (IMemoryOwner<byte>, int) output, string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName));

            return output.AbortWithErrorMessage(errorMessage);
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(this ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<byte> errorMessage)
        {
            CustomCommandUtils.WriteError(ref output, errorMessage);

            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="errorMessage">Error message to print to result stream.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        public static bool AbortWithErrorMessage(this ref (IMemoryOwner<byte>, int) output, string errorMessage)
        {
            CustomCommandUtils.WriteError(ref output, errorMessage);

            return true;
        }
    }
}