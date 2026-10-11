// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Minimal synchronous RESP client. Unlike a multiplexer it never reorders or coalesces, so a test can
    /// place commands precisely on either side of a suspension and observe exactly when each reply lands.
    /// It is also allocation-light on the steady-state path, so a test can attribute process-wide
    /// allocation to the server rather than to itself.
    /// </summary>
    internal sealed class RawRespSession : IDisposable
    {
        readonly Socket socket;
        readonly Stream stream;
        readonly byte[] readBuffer;
        readonly byte[] writeBuffer;
        readonly byte[] replyBuffer;
        int readStart, readEnd;

        /// <param name="useTls">Whether the server under test terminates TLS.</param>
        /// <param name="bufferSize">Capacity of the send and reply buffers; a command and its reply must fit.</param>
        internal RawRespSession(bool useTls = false, int bufferSize = 64 * 1024)
        {
            readBuffer = new byte[bufferSize];
            writeBuffer = new byte[bufferSize];
            replyBuffer = new byte[bufferSize];

            var endpoint = TestUtils.EndPoint;
            socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
            };
            socket.Connect(endpoint);
            socket.ReceiveTimeout = (int)TimeSpan.FromSeconds(90).TotalMilliseconds;

            var netStream = new NetworkStream(socket, ownsSocket: false);
            if (useTls)
            {
                var ssl = new SslStream(netStream, leaveInnerStreamOpen: false,
                    TestUtils.ValidateServerCertificate);
                ssl.AuthenticateAsClient(new SslClientAuthenticationOptions
                {
                    ClientCertificates = [TestUtils.GetClientCertificate()],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                });
                stream = ssl;
            }
            else
            {
                stream = netStream;
            }
        }

        /// <summary>Sends one command and returns its reply as text.</summary>
        internal string Execute(params string[] args)
        {
            Send(args);
            return ReadReply();
        }

        /// <summary>Sends one command and asserts the reply, without allocating on the steady-state path.</summary>
        internal void ExecuteExpect(ReadOnlySpan<byte> expectedReply, params string[] args)
        {
            Send(args);
            var len = ReadReplyBytes();
            if (!replyBuffer.AsSpan(0, len).SequenceEqual(expectedReply))
                Assert.Fail($"Expected '{Describe(expectedReply)}' but got '{Describe(replyBuffer.AsSpan(0, len))}'");
        }

        /// <summary>Sends one command, writing the whole frame in a single socket write.</summary>
        internal void Send(params string[] args) => SendBatch(args);

        /// <summary>
        /// Sends several commands in a single socket write, so the server sees them in one receive.
        /// Each element is one command's tokens.
        /// </summary>
        internal void SendPipeline(params string[][] commands)
        {
            var n = 0;
            foreach (var cmd in commands)
                n = Encode(cmd, n);
            stream.Write(writeBuffer, 0, n);
            stream.Flush();
        }

        void SendBatch(string[] args)
        {
            var n = Encode(args, 0);
            stream.Write(writeBuffer, 0, n);
            stream.Flush();
        }

        int Encode(string[] args, int at)
        {
            writeBuffer[at++] = (byte)'*';
            at += WriteInt(args.Length, at);
            at = WriteCrLf(at);
            foreach (var a in args)
            {
                writeBuffer[at++] = (byte)'$';
                at += WriteInt(Encoding.UTF8.GetByteCount(a), at);
                at = WriteCrLf(at);
                at += Encoding.UTF8.GetBytes(a, writeBuffer.AsSpan(at));
                at = WriteCrLf(at);
            }
            return at;
        }

        int WriteInt(int value, int at)
        {
            _ = value.TryFormat(writeBuffer.AsSpan(at), out var written,
                provider: CultureInfo.InvariantCulture);
            return written;
        }

        int WriteCrLf(int at)
        {
            writeBuffer[at++] = (byte)'\r';
            writeBuffer[at++] = (byte)'\n';
            return at;
        }

        /// <summary>Reads one RESP reply, including its framing, as text.</summary>
        internal string ReadReply()
        {
            var len = ReadReplyBytes();
            return Encoding.UTF8.GetString(replyBuffer, 0, len);
        }

        /// <summary>
        /// Reads one complete RESP reply into the reusable reply buffer and returns its length. The reply
        /// retains its framing, so a bulk string reads as "$5\r\nhello\r\n".
        /// </summary>
        internal int ReadReplyBytes()
        {
            var len = 0;
            ReadOne(ref len);
            return len;
        }

        void ReadOne(ref int len)
        {
            var lineStart = len;
            var lineLen = ReadLineInto(ref len);
            if (lineLen == 0)
                throw new IOException("Empty RESP reply");

            switch (replyBuffer[lineStart])
            {
                case (byte)'+':
                case (byte)'-':
                case (byte)':':
                case (byte)'_':
                    return;
                case (byte)'$':
                case (byte)'=':
                    var size = ParseLen(lineStart + 1, lineLen - 1);
                    if (size < 0) return;
                    ReadInto(size + 2, ref len);
                    return;
                case (byte)'*':
                case (byte)'~':
                case (byte)'>':
                    var count = ParseLen(lineStart + 1, lineLen - 1);
                    for (var i = 0; i < count; i++)
                        ReadOne(ref len);
                    return;
                default:
                    throw new IOException($"Unexpected RESP type '{(char)replyBuffer[lineStart]}'");
            }
        }

        int ParseLen(int at, int count)
        {
            var v = 0;
            var neg = false;
            for (var i = 0; i < count; i++)
            {
                var c = replyBuffer[at + i];
                if (c == '-') { neg = true; continue; }
                v = (v * 10) + (c - '0');
            }
            return neg ? -v : v;
        }

        /// <summary>Appends one CRLF-terminated line to the reply buffer; returns the length excluding CRLF.</summary>
        int ReadLineInto(ref int len)
        {
            var start = len;
            while (true)
            {
                var b = ReadByte();
                replyBuffer[len++] = b;
                if (b == '\n' && len - start >= 2 && replyBuffer[len - 2] == '\r')
                    return len - start - 2;
            }
        }

        void ReadInto(int count, ref int len)
        {
            for (var i = 0; i < count; i++)
                replyBuffer[len++] = ReadByte();
        }

        byte ReadByte()
        {
            if (readStart == readEnd)
            {
                readStart = 0;
                readEnd = stream.Read(readBuffer, 0, readBuffer.Length);
                if (readEnd <= 0)
                    throw new IOException("Connection closed by server");
            }
            return readBuffer[readStart++];
        }

        static string Describe(ReadOnlySpan<byte> bytes) =>
            Encoding.UTF8.GetString(bytes).Replace("\r\n", "\\r\\n");

        /// <summary>Closes the connection with a TCP reset, without a graceful shutdown handshake.</summary>
        internal void AbortAndDispose()
        {
            try { socket.LingerState = new LingerOption(true, 0); } catch { }
            Dispose();
        }

        public void Dispose()
        {
            try { stream?.Dispose(); } catch { }
            try { socket?.Dispose(); } catch { }
        }
    }
}