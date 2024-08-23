// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - sorted set
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate a wrong number of arguments for the given command.
        /// </summary>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithWrongNumberOfArguments(string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName));

            return AbortWithErrorMessage(errorMessage);
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message
        /// </summary>
        /// <param name="errorMessage">Error message to print to result stream</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(ReadOnlySpan<byte> errorMessage)
        {
            // Print error message to result stream
            while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Tries to parse the input as "LEFT" or "RIGHT" and returns the corresponding OperationDirection.
        /// If parsing fails, returns OperationDirection.Unknown.
        /// </summary>
        /// <param name="input">The input to parse.</param>
        /// <returns>The parsed OperationDirection, or OperationDirection.Unknown if parsing fails.</returns>
        public OperationDirection GetOperationDirection(ArgSlice input)
        {
            // Optimize for the common case
            if (input.ReadOnlySpan.SequenceEqual("LEFT"u8))
                return OperationDirection.Left;
            if (input.ReadOnlySpan.SequenceEqual("RIGHT"u8))
                return OperationDirection.Right;
            // Rare case: try making upper case and retry
            MakeUpperCase(input.ptr);
            if (input.ReadOnlySpan.SequenceEqual("LEFT"u8))
                return OperationDirection.Left;
            if (input.ReadOnlySpan.SequenceEqual("RIGHT"u8))
                return OperationDirection.Right;
            return OperationDirection.Unknown;
        }
    }
}