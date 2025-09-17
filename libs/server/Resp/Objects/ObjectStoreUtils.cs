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
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate an unknown subcommand or wrong number of arguments for the given command.
        /// </summary>
        /// <param name="subCommand">Name of the subcommand that caused the error message.</param>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(string subCommand, string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrUnknownSubCommandOrWrongNumberOfArguments, subCommand, cmdName));

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
            while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="format">The format string for the error message.</param>
        /// <param name="arg0">The first argument to format.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(string format, object arg0)
        {
            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(format, arg0)));
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="format">The format string for the error message.</param>
        /// <param name="arg0">The first argument to format.</param>
        /// <param name="arg1">The second argument to format.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(string format, object arg0, object arg1)
        {
            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(format, arg0, arg1)));
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="format">The format string for the error message.</param>
        /// <param name="arg0">The first argument to format.</param>
        /// <param name="arg1">The second argument to format.</param>
        /// <param name="arg2">The third argument to format.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(string format, object arg0, object arg1, object arg2)
        {
            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(format, arg0, arg1, arg2)));
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message.
        /// </summary>
        /// <param name="format">The format string for the error message.</param>
        /// <param name="args">The arguments to format.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(string format, params object[] args)
        {
            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(format, args)));
        }
    }
}