// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace Garnet.test
{
    /// <summary>
    /// Minimal RESP client over a bare socket. Used instead of a multiplexer so a test can hold hundreds
    /// of connections cheaply, and so it can observe reply timing and ordering directly.
    /// </summary>
    internal sealed class RawRespClient : IDisposable
    {
        readonly Socket socket;

        /// <summary>
        /// Null on a plaintext connection, which keeps that path on the raw socket calls the allocation
        /// measurements depend on.
        /// </summary>
        readonly SslStream tls;
        readonly byte[] buffer = new byte[4096];
        int bufferStart;
        int bufferEnd;

        internal RawRespClient(System.Net.EndPoint endPoint, bool useTls = false, int receiveBufferSize = 0)
        {
            socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                ReceiveTimeout = 60_000,
                SendTimeout = 60_000
            };

            // Shrinking the receive window lets a test stop reading and have the server's sends block
            // against a full socket in far less data than an auto-tuned buffer would need.
            if (receiveBufferSize > 0)
                socket.ReceiveBufferSize = receiveBufferSize;

            socket.Connect(endPoint);

            if (!useTls)
                return;

            tls = new SslStream(new NetworkStream(socket, ownsSocket: false), leaveInnerStreamOpen: false,
                TestUtils.ValidateServerCertificate);
            tls.AuthenticateAsClient(new SslClientAuthenticationOptions
            {
                ClientCertificates = [TestUtils.GetClientCertificate()],
                TargetHost = "GarnetTest",
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
            });
        }

        /// <summary>Writes a whole payload, over TLS when this connection is encrypted.</summary>
        void Write(byte[] payload, int offset, int count)
        {
            if (tls != null)
            {
                tls.Write(payload, offset, count);
                tls.Flush();
                return;
            }

            var sent = 0;
            while (sent < count)
                sent += socket.Send(payload, offset + sent, count - sent, SocketFlags.None);
        }

        /// <summary>Reads whatever is available, over TLS when this connection is encrypted.</summary>
        int Read(byte[] destination, int offset, int count)
            => tls != null
                ? tls.Read(destination, offset, count)
                : socket.Receive(destination, offset, count, SocketFlags.None);

        internal static byte[] Command(params string[] args)
        {
            var sb = new StringBuilder();
            _ = sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
                _ = sb.Append('$').Append(Encoding.UTF8.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");
            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Writes all commands in a single send, so the server sees them as one pipelined batch.
        /// </summary>
        internal void Send(params byte[][] commands)
        {
            var total = 0;
            foreach (var command in commands)
                total += command.Length;

            var payload = new byte[total];
            var offset = 0;
            foreach (var command in commands)
            {
                Buffer.BlockCopy(command, 0, payload, offset, command.Length);
                offset += command.Length;
            }

            Write(payload, 0, payload.Length);
        }

        /// <summary>
        /// Sends a pre-encoded payload. Allocation-free, unlike <see cref="Send"/>, so a caller can
        /// attribute measured allocation to the server.
        /// </summary>
        internal void SendRaw(byte[] payload) => Write(payload, 0, payload.Length);

        /// <summary>
        /// Discards exactly <paramref name="count"/> reply bytes without allocating.
        /// </summary>
        internal void Consume(int count)
        {
            while (count > 0)
            {
                var buffered = bufferEnd - bufferStart;
                if (buffered == 0)
                {
                    bufferStart = bufferEnd = 0;
                    var read = Read(buffer, 0, buffer.Length);
                    if (read == 0)
                        throw new IOException("Connection closed by server");
                    bufferEnd = read;
                    continue;
                }

                var take = Math.Min(buffered, count);
                bufferStart += take;
                count -= take;
            }
        }

        /// <summary>
        /// Compacts the read buffer and blocks for more bytes.
        /// </summary>
        void FillBuffer()
        {
            if (bufferStart > 0)
            {
                Buffer.BlockCopy(buffer, bufferStart, buffer, 0, bufferEnd - bufferStart);
                bufferEnd -= bufferStart;
                bufferStart = 0;
            }

            var read = Read(buffer, bufferEnd, buffer.Length - bufferEnd);
            if (read == 0)
                throw new IOException("Connection closed by server");
            bufferEnd += read;
        }

        /// <summary>
        /// Reads a RESP integer reply without allocating, so a caller can poll the server inside an
        /// allocation measurement without attributing its own garbage to the server.
        /// </summary>
        internal int ReadInteger()
        {
            while (true)
            {
                for (var i = bufferStart; i < bufferEnd - 1; i++)
                {
                    if (buffer[i] != (byte)'\r' || buffer[i + 1] != (byte)'\n')
                        continue;

                    if (buffer[bufferStart] != (byte)':')
                        throw new IOException($"Expected an integer reply, got '{(char)buffer[bufferStart]}'");

                    var value = 0;
                    for (var digit = bufferStart + 1; digit < i; digit++)
                        value = (value * 10) + (buffer[digit] - (byte)'0');

                    bufferStart = i + 2;
                    return value;
                }

                FillBuffer();
            }
        }

        /// <summary>
        /// Reads a bulk string reply and returns its payload.
        /// </summary>
        internal string ReadBulkString()
        {
            var header = ReadLine();
            if (header[0] != '$')
                throw new IOException($"Expected a bulk string reply, got '{header}'");

            var length = int.Parse(header[1..], CultureInfo.InvariantCulture);
            if (length < 0)
                return null;

            var payload = new byte[length];
            var copied = 0;
            while (copied < length)
            {
                if (bufferEnd == bufferStart)
                    FillBuffer();

                var take = Math.Min(bufferEnd - bufferStart, length - copied);
                Buffer.BlockCopy(buffer, bufferStart, payload, copied, take);
                bufferStart += take;
                copied += take;
            }

            Consume(2);
            return Encoding.UTF8.GetString(payload);
        }

        /// <summary>
        /// Reads one CRLF-terminated protocol line, without its terminator.
        /// </summary>
        internal string ReadLine()
        {
            while (true)
            {
                for (var i = bufferStart; i < bufferEnd - 1; i++)
                {
                    if (buffer[i] != (byte)'\r' || buffer[i + 1] != (byte)'\n')
                        continue;

                    var line = Encoding.UTF8.GetString(buffer, bufferStart, i - bufferStart);
                    bufferStart = i + 2;
                    return line;
                }

                FillBuffer();
            }
        }

        public void Dispose()
        {
            // Disposed before the shutdown so the TLS close-notify goes out while the socket is still
            // writable; a parked session on the other end has to see a clean close, not a reset.
            try { tls?.Dispose(); } catch { }
            try { socket.Shutdown(SocketShutdown.Both); } catch { }
            socket.Dispose();
        }
    }
}