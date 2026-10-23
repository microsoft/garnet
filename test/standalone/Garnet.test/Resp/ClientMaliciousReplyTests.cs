// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests that a client connected to a server sending a malformed or oversized array reply fails its pending
    /// request and closes the connection, rather than hanging or exhausting memory or stack.
    /// </summary>
    [TestFixture]
    public class ClientMaliciousReplyTests : TestBase
    {
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        public enum Client
        {
            GarnetClient,
            GarnetLightClient,
            GarnetClientSession,
        }

        public enum Reply
        {
            CountNoBufferCanHold,
            CountBeyondTheReceiveLimit,
            NestingBeyondTheLimit,
            BulkStringBeyondTheLimit,
            UnexpectedElementType,
        }

        static string Encode(Reply reply)
        {
            switch (reply)
            {
                case Reply.CountNoBufferCanHold:
                    return "*2147483647\r\n";
                case Reply.CountBeyondTheReceiveLimit:
                    return "*357913942\r\n";
                case Reply.NestingBeyondTheLimit:
                    var sb = new StringBuilder();
                    for (var i = 0; i < 100_000; i++)
                        _ = sb.Append("*1\r\n");
                    return sb.ToString();
                case Reply.BulkStringBeyondTheLimit:
                    return "*1\r\n$536870913\r\n";
                default:
                    return "*2\r\n+a\r\n!x\r\n";
            }
        }

        /// <summary>
        /// Accepts one connection, answers its first request with <paramref name="reply"/>, and reports whether
        /// the client then closed the connection.
        /// </summary>
        static async Task<bool> ServeAsync(TcpListener listener, byte[] reply)
        {
            using var socket = await listener.AcceptSocketAsync().ConfigureAwait(false);
            var buffer = new byte[4096];
            try
            {
                if (await socket.ReceiveAsync(buffer, SocketFlags.None).ConfigureAwait(false) == 0)
                    return false;

                _ = await socket.SendAsync(reply, SocketFlags.None).ConfigureAwait(false);

                while (await socket.ReceiveAsync(buffer, SocketFlags.None).ConfigureAwait(false) > 0)
                {
                }
            }
            catch (SocketException)
            {
                // A reset is a close as well
            }

            return true;
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task AMaliciousArrayReplyFailsTheRequestAndClosesTheConnection(
            [Values] Client client,
            [Values] Reply reply)
        {
            var endpoint = new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort);
            var listener = new TcpListener(endpoint);
            listener.Start();
            try
            {
                var server = ServeAsync(listener, Encoding.ASCII.GetBytes(Encode(reply)));

                Task request;
                IDisposable connection;
                switch (client)
                {
                    case Client.GarnetClient:
                        var gc = new GarnetClient(endpoint);
                        connection = gc;
                        gc.Connect();
                        request = gc.ExecuteForStringArrayResultAsync("PING");
                        break;
                    case Client.GarnetLightClient:
                        var glc = new GarnetLightClient(endpoint);
                        connection = glc;
                        glc.Connect();
                        request = glc.PingAsync();
                        break;
                    default:
                        var gcs = new GarnetClientSession(endpoint, new());
                        connection = gcs;
                        gcs.Connect();
                        request = gcs.ExecuteForArrayAsync("PING");
                        break;
                }

                using (connection)
                {
                    var completed = await Task.WhenAny(request, Task.Delay(Timeout)).ConfigureAwait(false);
                    ClassicAssert.AreSame(request, completed, "The pending request hung");
                    ClassicAssert.IsTrue(request.IsFaulted || request.IsCanceled, "The malicious reply was accepted");

                    completed = await Task.WhenAny(server, Task.Delay(Timeout)).ConfigureAwait(false);
                    ClassicAssert.AreSame(server, completed, "The client kept the connection open");
                    ClassicAssert.IsTrue(await server.ConfigureAwait(false), "The client closed the connection before reading the reply");
                }
            }
            finally
            {
                listener.Stop();
            }
        }
    }
}