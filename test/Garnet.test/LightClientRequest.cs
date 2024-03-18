// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Text;
using Garnet.common;
using Garnet.networking;

namespace Garnet.test
{
    public unsafe class LightClientRequest : IDisposable
    {
        readonly LightClient client;
        byte[] responseBuffer;
        int bytesReceived = 0;
        readonly bool countResponseLength;

        public string Address { get; set; }
        public int Port { get; set; }

        public LightClientRequest(string address, int port, int optType, LightClient.OnResponseDelegateUnsafe onReceive = null, SslClientAuthenticationOptions sslOptions = null, bool countResponseLength = false)
        {
            this.countResponseLength = countResponseLength;
            client = new LightClient(address, port, optType, onReceive == null ? LightReceive : onReceive, sslOptions: sslOptions);
            client.Connect();
            Address = address;
            Port = port;
        }

        public byte[] SendCommand(string cmd, int numTokens = 1, bool returnAccumulatedBuffer = false)
        {
            responseBuffer = null;
            byte[] buf = BuildRequest(cmd);
            client.Send(buf, buf.Length, numTokens);
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
                if (countResponseLength)
                {
                    count++;
                }
                else
                {
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
        private static byte[] BuildRequest(string cmd)
        {
            var tokens = cmd.Split(' ');
            string msg = "*" + tokens.Length + "\r\n";

            for (int i = 0; i < tokens.Length; i++)
            {
                msg += "$" + tokens[i].Length + "\r\n" + tokens[i] + "\r\n";
            }

            return Encoding.ASCII.GetBytes(msg);
        }
    }
}