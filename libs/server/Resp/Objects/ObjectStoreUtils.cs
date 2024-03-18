// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        /// Send the error of missing arguments for any command
        /// </summary>
        /// <param name="cmdName"></param>
        //todo move this method to write utils ??
        private void WriteErrorTokenNumberInCommand(string cmdName)
        {
            var errorMessage = Encoding.ASCII.GetBytes($"-ERR wrong number of arguments for {cmdName} command.\r\n");
            while (!RespWriteUtils.WriteResponse(errorMessage, ref dcurr, dend))
                SendAndReset();
        }

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
    }
}