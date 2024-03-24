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
            // Abort command and discard any remaining tokens on the input buffer
            var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);

            if (!DrainCommands(bufSpan, count))
                return false;

            // Print error message to result stream
            var errorMessage = Encoding.ASCII.GetBytes($"-ERR wrong number of arguments for {cmdName} command.\r\n");
            while (!RespWriteUtils.WriteResponse(errorMessage, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}