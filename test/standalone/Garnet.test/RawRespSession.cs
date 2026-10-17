// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Net;
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
        /// <param name="receiveBufferSize">
        /// When positive, the size of the socket's receive buffer. Setting it fixes the window the client
        /// advertises, where the default lets the kernel grow it to megabytes, so a client that stops reading
        /// backs the server's socket up after a predictable amount of data.
        /// </param>
        /// <param name="endpoint">The server to connect to; the standalone test server when omitted.</param>
        internal RawRespSession(bool useTls = false, int bufferSize = 64 * 1024, int receiveBufferSize = 0, EndPoint endpoint = null)
        {
            readBuffer = new byte[bufferSize];
            writeBuffer = new byte[bufferSize];
            replyBuffer = new byte[bufferSize];

            endpoint ??= TestUtils.EndPoint;
            socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
            };

            // Must be set before connecting, or the window advertised in the handshake is already fixed
            if (receiveBufferSize > 0)
                socket.ReceiveBufferSize = receiveBufferSize;

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

        /// <summary>How long a read waits for the server to send before it fails.</summary>
        internal TimeSpan ReceiveTimeout
        {
            set => socket.ReceiveTimeout = (int)value.TotalMilliseconds;
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
        /// Waits for the server to send something, without consuming it.
        /// </summary>
        /// <param name="timeout">How long to wait before reporting that nothing arrived.</param>
        /// <returns>True if a reply is available or the connection ended; false if the server sent nothing.</returns>
        internal bool WaitForReply(TimeSpan timeout)
            => readStart < readEnd || socket.Poll(timeout, SelectMode.SelectRead);

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

        /// <summary>
        /// Sends <paramref name="first"/> followed by only the opening of <paramref name="second"/>, waits for
        /// the reply to <paramref name="first"/> -- by which time the server holds an incomplete frame -- and
        /// then sends the rest of <paramref name="second"/>.
        /// </summary>
        /// <param name="first">A command the server answers at once.</param>
        /// <param name="second">The command that is delivered in two parts.</param>
        /// <returns>The reply to <paramref name="first"/>.</returns>
        internal string SendSplitAfterReply(string[] first, string[] second)
        {
            var firstEnd = Encode(first, 0);
            var end = Encode(second, firstEnd);
            var cut = firstEnd + (end - firstEnd) / 2;

            stream.Write(writeBuffer, 0, cut);
            stream.Flush();
            var reply = ReadReply();

            stream.Write(writeBuffer, cut, end - cut);
            stream.Flush();
            return reply;
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
                case (byte)'%':
                    var pairs = ParseLen(lineStart + 1, lineLen - 1);
                    for (var i = 0; i < pairs * 2; i++)
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

        /// <summary>
        /// Reads exactly <paramref name="expected"/>.Length bytes and asserts they are those bytes, comparing as
        /// they arrive so that a reply far larger than the session's buffers can be checked.
        /// </summary>
        /// <param name="expected">The bytes the server must send next.</param>
        /// <param name="what">What is being read, for the failure message.</param>
        internal void ExpectBytes(ReadOnlySpan<byte> expected, string what)
        {
            var done = 0;
            while (done < expected.Length)
            {
                if (readStart == readEnd)
                {
                    var open = false;
                    try
                    {
                        open = Refill();
                    }
                    catch (IOException e)
                    {
                        Assert.Fail($"{what}: nothing more arrived after {done} of {expected.Length} bytes ({e.Message})");
                    }

                    if (!open)
                        Assert.Fail($"{what}: the connection closed after {done} of {expected.Length} bytes");
                }

                var n = Math.Min(readEnd - readStart, expected.Length - done);
                var same = readBuffer.AsSpan(readStart, n).CommonPrefixLength(expected.Slice(done, n));
                if (same < n)
                    Assert.Fail($"{what}: byte {done + same} of {expected.Length} is 0x{readBuffer[readStart + same]:x2}, " +
                        $"expected 0x{expected[done + same]:x2}");

                readStart += n;
                done += n;
            }
        }

        /// <summary>Asserts that the server sends nothing more and closes the connection.</summary>
        /// <param name="what">What was being read, for the failure message.</param>
        internal void ExpectEndOfStream(string what)
        {
            if (readStart == readEnd)
            {
                var open = false;
                try
                {
                    open = Refill();
                }
                catch (IOException e)
                {
                    Assert.Fail($"{what}: the connection did not close cleanly ({e.Message})");
                }

                if (!open)
                    return;
            }

            var extra = readBuffer.AsSpan(readStart, Math.Min(readEnd - readStart, 32));
            Assert.Fail($"{what}: {readEnd - readStart} byte(s) arrived where the connection should have closed, " +
                $"starting '{Describe(extra)}'");
        }

        /// <summary>Refills the read buffer, returning false once the server has closed the connection.</summary>
        bool Refill()
        {
            readStart = readEnd = 0;
            readEnd = stream.Read(readBuffer, 0, readBuffer.Length);
            return readEnd > 0;
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