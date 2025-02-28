// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Text;
using Garnet.networking;

namespace Garnet.common
{
    public enum CountResponseType
    {
        Bytes,
        Tokens,
        Newlines
    }

    public unsafe class LightClientRequest : IDisposable
    {
        readonly LightClient client;
        byte[] responseBuffer;
        int bytesReceived = 0;
        bool extraWhiteSpaceIsEmptyParameter = false;

        /// <summary>
        /// How to count the response length
        /// </summary>
        public CountResponseType countResponseType;

        public EndPoint EndPoint { get; set; }

        public LightClientRequest(EndPoint endpoint, int optType,
                                  LightClient.OnResponseDelegateUnsafe onReceive = null,
                                  SslClientAuthenticationOptions sslOptions = null,
                                  CountResponseType countResponseType = CountResponseType.Tokens,
                                  bool extraWhiteSpaceIsEmptyParameter = false)
        {
            this.countResponseType = countResponseType;
            this.extraWhiteSpaceIsEmptyParameter = extraWhiteSpaceIsEmptyParameter;
            client = new LightClient(endpoint, optType, onReceive ?? LightReceive, sslOptions: sslOptions);
            client.Connect();
            EndPoint = endpoint;
        }

        public byte[] SendCommand(string cmd, int numTokens = 1, bool returnAccumulatedBuffer = false)
        {
            responseBuffer = null;
            byte[] buf = BuildRequest(cmd);
            client.Send(buf, buf.Length, numTokens);
            client.CompletePendingRequests();
            return client.ResponseBuffer;
        }

        public byte[] SendCommand(byte[] cmd, int numTokens = 1)
        {
            client.Send(cmd, cmd.Length, numTokens);
            client.CompletePendingRequests();
            return client.ResponseBuffer;
        }

        public string Execute(string cmd, int responseLength, int bytesPerSend = int.MaxValue)
        {
            var bytes = SendCommandChunks(cmd, bytesPerSend, responseLength, true);
            return Encoding.ASCII.GetString(bytes, 0, bytesReceived);
        }

        public string Execute(string cmd1, string cmd2, int responseLength, int bytesPerSend = int.MaxValue)
        {
            byte[] buf1 = BuildRequest(cmd1);
            byte[] buf2 = BuildRequest(cmd2);
            var buf = new byte[buf1.Length + buf2.Length];
            buf1.CopyTo(buf, 0);
            buf2.CopyTo(buf, buf1.Length);

            var bytes = SendCommandChunks(buf, bytesPerSend, responseLength, true);
            return Encoding.ASCII.GetString(bytes, 0, bytesReceived);
        }

        public byte[] SendCommandChunks(string cmd, int bytesPerSend, int numTokens = 1, bool returnAccumulatedBuffer = false)
            => SendCommandChunks(BuildRequest(cmd), bytesPerSend, numTokens, returnAccumulatedBuffer);

        byte[] SendCommandChunks(byte[] buf, int bytesPerSend, int numTokens = 1, bool returnAccumulatedBuffer = false)
        {
            bytesPerSend = Math.Min(bytesPerSend, buf.Length);
            byte[] data = new byte[bytesPerSend];
            responseBuffer = new byte[BufferSizeUtils.ServerBufferSize(new MaxSizeSettings())];

            int i = 0;
            bytesReceived = 0;

            while (i < buf.Length)
            {
                int chunk = i + bytesPerSend < buf.Length ? bytesPerSend : buf.Length - i;
                Array.Copy(buf, i, data, 0, chunk);
                if (i + chunk < buf.Length)
                    client.Send(data, chunk, numTokens);
                else
                    client.Send(data, chunk, numTokens);
                numTokens = 0;
                i += chunk;
            }
            client.CompletePendingRequests();
            return returnAccumulatedBuffer ? responseBuffer : client.ResponseBuffer;
        }

        public byte[] SendCommands(string cmd1, string cmd2, int numTokensCmd1 = 1, int numTokensCmd2 = 1)
        {
            byte[] buf1 = BuildRequest(cmd1);
            byte[] buf2 = BuildRequest(cmd2);
            var buf = new byte[buf1.Length + buf2.Length];

            responseBuffer = new byte[BufferSizeUtils.ServerBufferSize(new MaxSizeSettings())];
            bytesReceived = 0;

            buf1.CopyTo(buf, 0);
            buf2.CopyTo(buf, buf1.Length);
            client.Send(buf, buf.Length, numTokensCmd1 + numTokensCmd2);
            client.CompletePendingRequests();

            return responseBuffer;
        }

        private (int, int) LightReceive(byte* buf, int bytesRead, int opType)
        {
            int count = 0;
            for (int i = 0; i < bytesRead; i++)
            {
                switch (countResponseType)
                {
                    case CountResponseType.Bytes:
                        count++;
                        break;
                    case CountResponseType.Newlines:
                        if (buf[i] == '\n')
                            count++;
                        break;
                    case CountResponseType.Tokens:
                        // check for null value '$-1'
                        if (buf[i] == '$' && buf[i + 1] == '-' && buf[i + 2] == '1')
                        {
                            count++;
                        }
                        // check for error
                        else if (buf[i] == '-' && i == 0)
                        {
                            count++;
                        }
                        else if (buf[i] == '$' || buf[i] == '+' || buf[i] == ':' || buf[i] == '*')
                        {
                            count++;
                        }
                        break;
                    default:
                        break;
                }

                // Appending value to accumulated buffer
                if (responseBuffer != null)
                {
                    if (bytesReceived == responseBuffer.Length)
                    {
                        var _tmp = new byte[responseBuffer.Length * 2];
                        Array.Copy(responseBuffer, _tmp, responseBuffer.Length);
                        responseBuffer = _tmp;
                    }
                    responseBuffer[bytesReceived++] = buf[i];
                }
            }
            return (bytesRead, count);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() => client.Dispose();

        /// <summary>
        /// Formats the command for RESP
        /// </summary>
        private byte[] BuildRequest(string cmd)
        {
            StringBuilder sb = new(cmd.Length * 2);
            var tokenStart = 0;
            var count = 0;
            var prevSp = true;
            var quoteMode = false;
            var escapedChar = false;
            var hadEscapedChar = 0;

            void AppendTo(int to)
            {
                count++;
                sb = sb.Append('$');
                if (hadEscapedChar == 0)
                {
                    sb = sb.Append(to - tokenStart);
                    sb = sb.Append("\r\n");
                    sb = sb.Append(cmd[tokenStart..to]);
                }
                else
                {
                    sb = sb.Append(to - tokenStart - hadEscapedChar);
                    sb = sb.Append("\r\n");
                    sb = sb.Append(cmd[tokenStart..to].Replace("\\", ""));
                    hadEscapedChar = 0;
                }
                sb = sb.Append("\r\n");
            }

            for (var i = 0; i < cmd.Length; ++i)
            {
                if (escapedChar)
                {
                    escapedChar = false;
                    continue;
                }

                if (cmd[i] == ' ')
                {
                    if (quoteMode)
                    {
                        continue;
                    }

                    if (prevSp)
                    {
                        if (extraWhiteSpaceIsEmptyParameter && cmd[i - 1] == ' ')
                        {
                            AppendTo(tokenStart);
                        }

                        tokenStart++;
                        continue;
                    }

                    prevSp = true;
                    AppendTo(i);
                    tokenStart = i + 1;
                    continue;
                }

                if (cmd[i] == '"')
                {
                    if (quoteMode)
                    {
                        quoteMode = false;
                        AppendTo(i);
                        tokenStart = i + 1;
                    }
                    else
                    {
                        quoteMode = true;
                        tokenStart++;
                    }

                    prevSp = true;
                    continue;
                }

                prevSp = false;

                if (cmd[i] == '\\')
                {
                    escapedChar = true;
                    hadEscapedChar++;
                }
            }

            if (tokenStart < cmd.Length)
            {
                AppendTo(cmd.Length);
            }
            else if (extraWhiteSpaceIsEmptyParameter && prevSp
                  && tokenStart == cmd.Length && cmd[cmd.Length - 1] == ' ')
            {
                AppendTo(tokenStart);
            }

            sb = sb.Insert(0, '*' + count.ToString() + "\r\n");

            return Encoding.ASCII.GetBytes(sb.ToString());
        }
    }
}