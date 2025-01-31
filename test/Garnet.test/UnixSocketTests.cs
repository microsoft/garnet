﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using NUnit.Framework.Legacy;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class UnixSocketTests
    {
        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public async Task Ping_DoesNotThrow([Values] bool useTls)
        {
            var unixSocketPath = "./unix-socket-ping-test.sock";
            var unixSocketEndpoint = new UnixDomainSocketEndPoint(unixSocketPath);

            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, unixSocketEndpoint, enableTLS: useTls, unixSocketPath: unixSocketPath);
            server.Start();

            using var client = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig([unixSocketEndpoint], useTLS: useTls));
            var db = client.GetDatabase(0);

            ClassicAssert.IsTrue(client.IsConnected);
            Assert.DoesNotThrowAsync(() => db.PingAsync());
        }

        [Test]
        public async Task SetGet_Equals([Values] bool useTls, [Values(256, 256 * 2048)] int valueBufferLength)
        {
            var unixSocketPath = "./unix-socket-set-get-test.sock";
            var unixSocketEndpoint = new UnixDomainSocketEndPoint(unixSocketPath);

            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, unixSocketEndpoint, enableTLS: useTls, unixSocketPath: unixSocketPath);
            server.Start();

            using var client = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig([unixSocketEndpoint], useTLS: useTls));
            var db = client.GetDatabase(0);

            var buffer = ArrayPool<byte>.Shared.Rent(valueBufferLength);
            buffer.AsSpan().Fill(0x42);

            var result = await db.StringSetAsync("mykey", buffer);
            ClassicAssert.IsTrue(result);

            var actualValue = (byte[])await db.StringGetAsync("mykey");

            ClassicAssert.IsTrue(buffer.AsSpan().SequenceEqual(actualValue));

            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}