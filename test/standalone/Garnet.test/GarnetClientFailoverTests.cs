// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetClientFailoverTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [TestCase(FailoverOption.FORCE, "FORCE")]
        [TestCase(FailoverOption.TAKEOVER, "TAKEOVER")]
        public void FailoverOptionBytesArePlainAndDoNotAllocate(FailoverOption option, string expected)
        {
            byte[] optionBytes = FailoverUtils.GetFailoverOptionBytes(option);
            Assert.That(Encoding.ASCII.GetString(optionBytes), Is.EqualTo(expected));
            for (int iteration = 0; iteration < 1000; iteration++)
                _ = FailoverUtils.GetFailoverOptionBytes(option);

            int byteCount = 0;
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            for (int iteration = 0; iteration < 10000; iteration++)
                byteCount += FailoverUtils.GetFailoverOptionBytes(option).Length;
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

            Assert.That(byteCount, Is.EqualTo(expected.Length * 10000));
            Assert.That(allocatedBytes, Is.Zero);
        }

        [TestCase(FailoverOption.DEFAULT)]
        [TestCase(FailoverOption.FORCE)]
        [TestCase(FailoverOption.TAKEOVER)]
        public async Task FailoverSendsPlainOptionArgument(FailoverOption option)
        {
            using CancellationTokenSource cancellation = new(TimeSpan.FromSeconds(30));
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                Task<string[]> request = ReadRequestAndReplyAsync(listener, cancellation.Token);
                using GarnetClient client = new((IPEndPoint)listener.LocalEndpoint);
                await client.ConnectAsync(cancellation.Token);
                Assert.That(await client.Failover(option, cancellation.Token), Is.True);

                string[] expected = option == FailoverOption.DEFAULT
                    ? ["CLUSTER", "FAILOVER"]
                    : ["CLUSTER", "FAILOVER", option.ToString()];
                Assert.That(await request, Is.EqualTo(expected));
            }
            finally
            {
                listener.Stop();
            }
        }

        private static async Task<string[]> ReadRequestAndReplyAsync(TcpListener listener, CancellationToken cancellationToken)
        {
            using TcpClient connection = await listener.AcceptTcpClientAsync(cancellationToken);
            using NetworkStream stream = connection.GetStream();
            using StreamReader reader = new(stream, Encoding.ASCII, leaveOpen: true);
            string arrayHeader = await reader.ReadLineAsync(cancellationToken);
            Assert.That(arrayHeader, Does.StartWith("*"));
            int argumentCount = int.Parse(arrayHeader.AsSpan(1), CultureInfo.InvariantCulture);
            string[] arguments = new string[argumentCount];
            for (int index = 0; index < argumentCount; index++)
            {
                string argumentHeader = await reader.ReadLineAsync(cancellationToken);
                Assert.That(argumentHeader, Does.StartWith("$"));
                int argumentLength = int.Parse(argumentHeader.AsSpan(1), CultureInfo.InvariantCulture);
                char[] argument = new char[argumentLength];
                Assert.That(await reader.ReadBlockAsync(argument.AsMemory(), cancellationToken), Is.EqualTo(argumentLength));
                Assert.That(await reader.ReadLineAsync(cancellationToken), Is.Empty);
                arguments[index] = new string(argument);
            }

            await stream.WriteAsync("+OK\r\n"u8.ToArray(), cancellationToken);
            return arguments;
        }
    }
}