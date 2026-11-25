// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers how the server's receive buffer grows to hold a message larger than the network buffer pool's largest size class. The pool cannot
    /// recycle a buffer that size, so as a pinned array it would linger as garbage in the never-compacted pinned object heap once released, and
    /// every size class it doubled through on the way up would too. Instead the buffer grows toward the size the message declares by up to 8x
    /// at a time until 128 MB, and past the largest size class into native memory.
    /// </summary>
    [TestFixture]
    public class ReceiveBufferGrowthTests : TestBase
    {
        /// <summary>Above the pool's largest size class (1 MB), and several doublings past it.</summary>
        const int LargeValueLength = 6 * 1024 * 1024;

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        void StartServer(bool useTls)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTls);
            server.Start();
        }

        /// <summary>A raw RESP connection, over TLS or plain TCP.</summary>
        sealed class Connection : IDisposable
        {
            readonly Stream stream;

            public Connection(bool useTls)
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                socket.Connect(TestUtils.EndPoint);
                stream = new NetworkStream(socket, ownsSocket: true);
                if (useTls)
                {
                    var ssl = new SslStream(stream, leaveInnerStreamOpen: false, TestUtils.ValidateServerCertificate);
                    ssl.AuthenticateAsClient(new SslClientAuthenticationOptions
                    {
                        ClientCertificates = [TestUtils.GetClientCertificate()],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                    });
                    stream = ssl;
                }
                stream = new BufferedStream(stream);
            }

            public void Send(byte[] command)
            {
                stream.Write(command);
                stream.Flush();
            }

            public string ReadLine()
            {
                var line = new StringBuilder();
                int b;
                while ((b = stream.ReadByte()) != '\n')
                {
                    if (b < 0)
                        throw new IOException("connection closed");
                    if (b != '\r')
                        line.Append((char)b);
                }
                return line.ToString();
            }

            public byte[] ReadBulkString()
            {
                var header = ReadLine();
                ClassicAssert.IsTrue(header.StartsWith('$'), $"expected a bulk string, got '{header}'");
                var value = new byte[int.Parse(header[1..])];
                stream.ReadExactly(value);
                _ = ReadLine();
                return value;
            }

            public void Dispose() => stream.Dispose();
        }

        static byte[] Command(params byte[][] args)
        {
            using var ms = new MemoryStream();
            ms.Write(Encoding.ASCII.GetBytes($"*{args.Length}\r\n"));
            foreach (var arg in args)
            {
                ms.Write(Encoding.ASCII.GetBytes($"${arg.Length}\r\n"));
                ms.Write(arg);
                ms.Write("\r\n"u8);
            }
            return ms.ToArray();
        }

        static byte[] Command(params string[] args) => Command(Array.ConvertAll(args, Encoding.ASCII.GetBytes));

        static byte[] LargeValue()
        {
            var value = new byte[LargeValueLength];
            new Random(0).NextBytes(value);
            return value;
        }

        /// <summary>Reads a counter from the server's network buffer pool, over a connection of its own.</summary>
        static long PoolCounter(string name, bool useTls)
        {
            using var connection = new Connection(useTls);
            connection.Send(Command("INFO", "BPSTATS"));
            var stats = Encoding.ASCII.GetString(connection.ReadBulkString());

            var marker = name + "=";
            var start = stats.IndexOf("server_socket_0:", StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(start, 0, stats);
            start = stats.IndexOf(marker, start, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(start, 0, $"'{name}' not found in BPSTATS: {stats}");
            start += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], start);
            return long.Parse(stats[start..end]);
        }

        /// <summary>Reads a byte counter from the server's network buffer pool, which BPSTATS formats with a unit.</summary>
        static long PoolCounterBytes(string name)
        {
            using var connection = new Connection(useTls: false);
            connection.Send(Command("INFO", "BPSTATS"));
            var stats = Encoding.ASCII.GetString(connection.ReadBulkString());
            var start = stats.IndexOf(name + "=", stats.IndexOf("server_socket_0:", StringComparison.Ordinal), StringComparison.Ordinal) + name.Length + 1;
            var value = stats[start..stats.IndexOfAny([',', '\r', '\n'], start)];
            var (suffix, scale) = value.EndsWith("GB", StringComparison.Ordinal) ? ("GB", 1L << 30)
                : value.EndsWith("MB", StringComparison.Ordinal) ? ("MB", 1L << 20)
                : value.EndsWith("KB", StringComparison.Ordinal) ? ("KB", 1L << 10)
                : ("", 1L);
            return (long)(double.Parse(value[..^suffix.Length], CultureInfo.InvariantCulture) * scale);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ALargeMessageGrowsTheReceiveBufferIntoNativeMemoryOnce(bool useTls)
        {
            StartServer(useTls);
            var value = LargeValue();

            using (var connection = new Connection(useTls))
            {
                connection.Send(Command(Encoding.ASCII.GetBytes("SET"), Encoding.ASCII.GetBytes("large"), value));
                ClassicAssert.AreEqual("+OK", connection.ReadLine());
                connection.Send(Command("GET", "large"));
                ClassicAssert.IsTrue(value.AsSpan().SequenceEqual(connection.ReadBulkString()), "the value did not round-trip");
            }

            ClassicAssert.AreEqual(0, PoolCounter("totalOutOfBoundAllocations", useTls),
                "a receive buffer above the pool's largest size class was allocated as a pinned array");
            // 128 KB grows 8x to the pool's largest class (1 MB), then once more to the 8 MB that holds the message.
            ClassicAssert.AreEqual(1, PoolCounter("totalNativeAllocations", useTls),
                "the receive buffer did not grow toward the size the message declares");
        }

        /// <summary>
        /// The server disposes connection handlers from its own thread, so a handler can be disposed while its receive thread is replacing the
        /// receive buffer. The buffer installed after the disposal must still be returned: otherwise the network buffer pool never drains and the
        /// server never finishes disposing. Holds the replacement between allocating the new buffer and installing it.
        /// </summary>
        [TestCase(false)]
        [TestCase(true)]
        public async Task DisposingTheServerWhileAReceiveBufferIsReplacedReleasesTheNewBuffer(bool useTls)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();
            const ExceptionInjectionType Swap = ExceptionInjectionType.ReceiveBuffer_Hold_Before_Swap;
            const ExceptionInjectionType Disposed = ExceptionInjectionType.Handler_Signal_After_Dispose;
            var timeout = TimeSpan.FromSeconds(30);

            StartServer(useTls);
            using var connection = new Connection(useTls);
            ExceptionInjectionHelper.EnableException(Swap);
            try
            {
                // The server stops reading while the replacement is held, so the send runs in the background.
                var send = Task.Run(() => connection.Send(Command(Encoding.ASCII.GetBytes("SET"), Encoding.ASCII.GetBytes("large"), LargeValue())));
                var elapsed = Stopwatch.StartNew();
                while (ExceptionInjectionHelper.WaitingOnClear(Swap) == 0)
                {
                    ClassicAssert.Less(elapsed.Elapsed, timeout, "the receive buffer was never replaced");
                    await Task.Delay(1);
                }

                ExceptionInjectionHelper.EnableException(Disposed);
                var disposing = server;
                server = null;
                var dispose = Task.Run(disposing.Dispose);
                await ExceptionInjectionHelper.WaitOnClearAsync(Disposed).WaitAsync(timeout);

                // The handler has released its buffers; now let the replacement install its new one.
                ExceptionInjectionHelper.DisableException(Swap);

                ClassicAssert.IsTrue(dispose.Wait(timeout),
                    "the server did not finish disposing: a receive buffer installed after its handler was disposed was never returned to the pool");
                try
                {
                    await send;
                }
                catch (IOException)
                {
                    // The server closed the connection mid-send
                }
            }
            finally
            {
                // Releases a replacement still held if the test failed early.
                ExceptionInjectionHelper.DisableException(Swap);
                ExceptionInjectionHelper.DisableException(Disposed);
            }
        }

        [Test]
        public void ADeclaredLengthGrowsTheReceiveBufferAtMost8xAheadOfTheBytesSent()
        {
            StartServer(useTls: false);
            const long MB = 1024 * 1024;

            // Declares a 1,000,000,000 byte argument but sends only part of it, and keeps the message incomplete.
            using var connection = new Connection(useTls: false);
            connection.Send(Encoding.ASCII.GetBytes("*2\r\n$4\r\nPING\r\n$1000000000\r\n"));
            var chunk = new byte[MB];
            void SendMB(int count)
            {
                for (var i = 0; i < count; i++)
                    connection.Send(chunk);
            }

            void WaitForNativeAllocations(int count)
            {
                var elapsed = Stopwatch.StartNew();
                while (PoolCounter("totalNativeAllocations", useTls: false) < count)
                {
                    ClassicAssert.Less(elapsed.Elapsed, TimeSpan.FromSeconds(30), "the receive buffer did not grow as the bytes arrived");
                    Thread.Sleep(10);
                }
                ClassicAssert.AreEqual(count, PoolCounter("totalNativeAllocations", useTls: false));
            }

            // 3 MB fills the base buffer, which grows 8x to the pool's largest class (1 MB), and then that, which grows 8x to 8 MB.
            SendMB(3);
            WaitForNativeAllocations(1);
            var live = PoolCounterBytes("liveBytes");
            ClassicAssert.GreaterOrEqual(live, 8 * MB, $"the receive buffer did not grow 8x toward the declared length ({live} bytes live)");
            ClassicAssert.Less(live, 16 * MB, $"the receive buffer grew more than 8x ahead of the bytes sent ({live} bytes live)");

            // Filling 8 MB, 64 MB and then 128 MB grows the buffer 8x to 64 MB, to the 128 MB limit of 8x growth, and then only 2x, to 256 MB.
            SendMB(126);
            WaitForNativeAllocations(4);
            live = PoolCounterBytes("liveBytes");
            ClassicAssert.GreaterOrEqual(live, 256 * MB, $"the receive buffer did not grow past 128 MB ({live} bytes live)");
            ClassicAssert.Less(live, 512 * MB, $"the receive buffer grew more than 2x past 128 MB ({live} bytes live)");
        }
    }
}