// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
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
        /// Reads the n tokens from the current buffer, and returns
        /// total tokens read
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        private int ReadLeftToken(int count, ref byte* ptr)
        {
            int totalTokens = 0;
            while (totalTokens < count)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var _, ref ptr, recvBufferPtr + bytesRead))
                    break;
                totalTokens++;
            }

            return totalTokens;
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs
        /// an error message to indicate a wrong number of arguments for the given command.
        /// </summary>
        /// <param name="cmdName">Name of the command that caused the error message.</param>
        /// <param name="count">Number of remaining tokens belonging to this command on the receive buffer.</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithWrongNumberOfArguments(string cmdName, int count)
        {
            var errorMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName));

            return AbortWithErrorMessage(count, errorMessage);
        }

        /// <summary>
        /// Aborts the execution of the current object store command and outputs a given error message
        /// </summary>
        /// <param name="count">Number of remaining tokens belonging to this command on the receive buffer.</param>
        /// <param name="errorMessage">Error message to print to result stream</param>
        /// <returns>true if the command was completely consumed, false if the input on the receive buffer was incomplete.</returns>
        private bool AbortWithErrorMessage(int count, ReadOnlySpan<byte> errorMessage)
        {
            // Abort command and discard any remaining tokens on the input buffer
            var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);

            if (!DrainCommands(bufSpan, count))
                return false;

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationDirection GetOperationDirection(ReadOnlySpan<byte> input)
        {
#if NET8_0_OR_GREATER
            if (Ascii.EqualsIgnoreCase(input, "RIGHT"))
            {
                return OperationDirection.Right;
            }
            else if (Ascii.EqualsIgnoreCase(input, "LEFT"))
            {
                return OperationDirection.Left;
            }
#else
            string inputString = Encoding.UTF8.GetString(input);
            if (inputString.Equals("RIGHT", StringComparison.OrdinalIgnoreCase))
            {
                return OperationDirection.Right;
            }
            else if (inputString.Equals("LEFT", StringComparison.OrdinalIgnoreCase))
            {
                return OperationDirection.Left;
            }
#endif
            else
            {
                return OperationDirection.Unknown;
            }
        }
    }
}