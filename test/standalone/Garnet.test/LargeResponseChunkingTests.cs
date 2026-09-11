// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A response element larger than the send buffer cannot be written atomically. The chunking
    /// helper for this case already exists and SCAN uses it, but several commands still write user
    /// data through the atomic path, where an over-sized element fails the whole command and kills
    /// the session. These cover the commands that can return unbounded user data.
    ///
    /// Run twice: once with the adaptive budget disabled, so the send buffer is the 128 KB default,
    /// and once with a budget small enough to pin both directions at their floors. The second arm is
    /// what proves the atomic-first fallback is genuinely size-independent rather than merely correct
    /// at the shipped default.
    /// </summary>
    [TestFixture("0", null, Description = "default send buffer")]
    [TestFixture("64k", "16k", Description = "send buffer floored by the budget")]
    public class LargeResponseChunkingTests : TestBase
    {
        // Comfortably larger than the 128 KB default send buffer.
        const int OversizedElement = 200 * 1024;

        readonly string networkBufferMemoryBudget;
        readonly string networkSendBufferMinSize;

        GarnetServer server;

        public LargeResponseChunkingTests(string networkBufferMemoryBudget, string networkSendBufferMinSize)
        {
            this.networkBufferMemoryBudget = networkBufferMemoryBudget;
            this.networkSendBufferMinSize = networkSendBufferMinSize;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true,
                networkBufferMemoryBudget: networkBufferMemoryBudget,
                networkSendBufferMinSize: networkSendBufferMinSize);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 15_000 };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        static byte[] Resp(params string[] parts)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(parts.Length).Append("\r\n");
            foreach (var p in parts)
                sb.Append('$').Append(p.Length).Append("\r\n").Append(p).Append("\r\n");
            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Reads until <paramref name="expected"/> appears, or the connection drops. A dropped
        /// connection is the failure mode under test: the session is killed mid-response.
        /// </summary>
        static string ReadUntil(Socket s, string expected)
        {
            var sb = new StringBuilder();
            var buf = new byte[64 * 1024];
            while (true)
            {
                int n;
                try
                {
                    n = s.Receive(buf);
                }
                catch (SocketException e)
                {
                    return $"<socket error: {e.SocketErrorCode}> after {sb.Length} bytes";
                }
                if (n == 0) return $"<connection closed> after {sb.Length} bytes";
                sb.Append(Encoding.ASCII.GetString(buf, 0, n));
                if (sb.Length >= expected.Length && sb.ToString().Contains(expected))
                    return sb.ToString();
            }
        }

        static void ExpectOk(Socket s) =>
            StringAssert.StartsWith("+OK\r\n", ReadUntil(s, "+OK\r\n"));

        [Test]
        public void KeysReturnsKeyLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigKey = new string('k', OversizedElement);

            s.Send(Resp("SET", bigKey, "v"));
            ExpectOk(s);

            s.Send(Resp("KEYS", "*"));
            var reply = ReadUntil(s, bigKey);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains($"${OversizedElement}\r\n", reply);
            StringAssert.Contains(bigKey, reply);
        }

        /// <summary>
        /// Set-operation results are built as a managed collection and written straight through the atomic
        /// path, unlike SMEMBERS which routes through the growing memory writer.
        /// </summary>
        [Test]
        public void SetIntersectReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('i', OversizedElement);

            s.Send(Resp("SADD", "sa", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));
            s.Send(Resp("SADD", "sb", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("SINTER", "sa", "sb"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigMember, reply);
        }

        [Test]
        public void SetUnionReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('u', OversizedElement);

            s.Send(Resp("SADD", "ua", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("SUNION", "ua", "ub"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigMember, reply);
        }

        /// <summary>
        /// The blocking pops are the dangerous ones: the element is removed from the list before the reply is
        /// written, so a failed write loses the item outright rather than merely killing the session.
        /// </summary>
        [Test]
        public void BlockingPopReturnsElementLargerThanSendBufferWithoutLosingIt()
        {
            using var s = Connect();
            var bigElement = new string('b', OversizedElement);

            s.Send(Resp("RPUSH", "blist", bigElement));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("BLPOP", "blist", "0"));
            var reply = ReadUntil(s, bigElement);

            StringAssert.StartsWith("*2\r\n", reply);
            StringAssert.Contains(bigElement, reply);

            // If the write had failed after the destructive pop the element would be gone with no reply.
            s.Send(Resp("LLEN", "blist"));
            StringAssert.StartsWith(":0\r\n", ReadUntil(s, ":0\r\n"));
        }

        [Test]
        public void BlockingSortedSetPopReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('z', OversizedElement);

            s.Send(Resp("ZADD", "bzset", "1", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("BZPOPMIN", "bzset", "0"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.StartsWith("*3\r\n", reply);
            StringAssert.Contains(bigMember, reply);
        }

        /// <summary>
        /// BLMOVE writes the moved element after it has already been removed from the source list.
        /// </summary>
        [Test]
        public void BlockingMoveReturnsElementLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigElement = new string('v', OversizedElement);

            s.Send(Resp("RPUSH", "srclist", bigElement));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("BLMOVE", "srclist", "dstlist", "LEFT", "RIGHT", "0"));
            var reply = ReadUntil(s, bigElement);

            StringAssert.Contains(bigElement, reply);

            s.Send(Resp("LLEN", "dstlist"));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));
        }

        [Test]
        public void SortedSetRandomMemberReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('r', OversizedElement);

            s.Send(Resp("ZADD", "zrand", "1", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("ZRANDMEMBER", "zrand"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.Contains(bigMember, reply);
        }

        [Test]
        public void SetMembersReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('m', OversizedElement);

            s.Send(Resp("SADD", "myset", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("SMEMBERS", "myset"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigMember, reply);
        }

        [Test]
        public void ListRangeReturnsElementLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigItem = new string('l', OversizedElement);

            s.Send(Resp("RPUSH", "mylist", bigItem));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("LRANGE", "mylist", "0", "-1"));
            var reply = ReadUntil(s, bigItem);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigItem, reply);
        }

        [Test]
        public void SortedSetRangeReturnsMemberLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigMember = new string('z', OversizedElement);

            s.Send(Resp("ZADD", "myzset", "1", bigMember));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("ZRANGE", "myzset", "0", "-1"));
            var reply = ReadUntil(s, bigMember);

            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigMember, reply);
        }

        [Test]
        public void HashGetAllReturnsValueLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigValue = new string('h', OversizedElement);

            s.Send(Resp("HSET", "myhash", "f", bigValue));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("HGETALL", "myhash"));
            var reply = ReadUntil(s, bigValue);

            StringAssert.StartsWith("*2\r\n", reply);
            StringAssert.Contains(bigValue, reply);
        }

        /// <summary>
        /// Guards the parameterisation itself. Without this, the budgeted arm could silently stop
        /// binding and become a duplicate of the default arm, and every oversized test above would
        /// still pass while proving nothing about a floored buffer.
        /// </summary>
        [Test]
        public void TheConfiguredArmActuallyBindsTheBufferSize()
        {
            using var s = Connect();
            s.Send(Encoding.ASCII.GetBytes("*2\r\n$4\r\nINFO\r\n$7\r\nBPSTATS\r\n"));
            var buf = new byte[32 * 1024];
            var reply = Encoding.ASCII.GetString(buf, 0, s.Receive(buf));

            var marker = "targetBufferSize=";
            var at = reply.IndexOf(marker, StringComparison.Ordinal);
            ClassicAssert.Greater(at, -1, "targetBufferSize missing from INFO BPSTATS");
            // Format.MemoryBytes emits e.g. "128KB", so take the whole token and scale by its suffix.
            var token = new string([.. reply[(at + marker.Length)..].TakeWhile(c => c != ',' && c != '\r')]);
            var scale = token.EndsWith("KB", StringComparison.Ordinal) ? 1L << 10
                : token.EndsWith("MB", StringComparison.Ordinal) ? 1L << 20 : 1L;
            var value = (long)(double.Parse(scale == 1 ? token : token[..^2],
                System.Globalization.CultureInfo.InvariantCulture) * scale);

            if (networkBufferMemoryBudget == "0")
                ClassicAssert.AreEqual(128 * 1024, value, "the disabled arm must sit at the configured size");
            else
                ClassicAssert.LessOrEqual(value, 64 * 1024, "the budgeted arm must be driven below the configured size");
        }

        /// <summary>
        /// The small-element path must be untouched by the oversized fallback: an ordinary multi-element
        /// reply is still correct and still whole.
        /// </summary>
        [Test]
        public void ManySmallElementsAreUnaffected()
        {
            using var s = Connect();
            const int Count = 5000;

            for (var i = 0; i < Count; i++)
            {
                s.Send(Resp("RPUSH", "small", $"item-{i}"));
                ReadUntil(s, $":{i + 1}\r\n");
            }

            s.Send(Resp("LRANGE", "small", "0", "-1"));
            var reply = ReadUntil(s, $"item-{Count - 1}\r\n");

            StringAssert.StartsWith($"*{Count}\r\n", reply);
            for (var i = 0; i < Count; i += 500)
                StringAssert.Contains($"item-{i}\r\n", reply);
        }


        /// <summary>
        /// CLIENT GETNAME writes the name as the first element of the response, so the buffer is
        /// empty when the over-sized write is attempted. That is the case where flushing cannot make
        /// progress and the chunking fallback has to be entered directly.
        /// </summary>
        [Test]
        public void ClientGetNameReturnsNameLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigName = new string('n', OversizedElement);

            s.Send(Resp("CLIENT", "SETNAME", bigName));
            ExpectOk(s);

            s.Send(Resp("CLIENT", "GETNAME"));
            var reply = ReadUntil(s, bigName);

            StringAssert.DoesNotContain("<connection closed>", reply);
            StringAssert.DoesNotContain("<socket error", reply);
            StringAssert.StartsWith($"${OversizedElement}\r\n", reply);
            StringAssert.Contains(bigName, reply);
        }

        /// <summary>
        /// SUBSCRIBE echoes the channel name back to the subscriber, so an over-sized channel name
        /// takes the same atomic-write path as an over-sized key.
        /// </summary>
        [Test]
        public void SubscribeEchoesChannelLargerThanSendBuffer()
        {
            using var s = Connect();
            var bigChannel = new string('c', OversizedElement);

            s.Send(Resp("SUBSCRIBE", bigChannel));
            var reply = ReadUntil(s, bigChannel);

            StringAssert.DoesNotContain("<connection closed>", reply);
            StringAssert.DoesNotContain("<socket error", reply);
            StringAssert.Contains(bigChannel, reply);
        }

        /// <summary>
        /// PUBSUB CHANNELS reports channel names registered by other connections, so it can return an
        /// over-sized element even though the querying connection never sent one.
        /// </summary>
        [Test]
        public void PubSubChannelsReturnsChannelLargerThanSendBuffer()
        {
            var bigChannel = new string('c', OversizedElement);

            using var subscriber = Connect();
            subscriber.Send(Resp("SUBSCRIBE", bigChannel));
            _ = ReadUntil(subscriber, bigChannel);

            using var querier = Connect();
            querier.Send(Resp("PUBSUB", "CHANNELS"));
            var reply = ReadUntil(querier, bigChannel);

            StringAssert.DoesNotContain("<connection closed>", reply);
            StringAssert.DoesNotContain("<socket error", reply);
            StringAssert.StartsWith("*1\r\n", reply);
            StringAssert.Contains(bigChannel, reply);
        }

        /// <summary>
        /// Sweep of the commands that can return unbounded user data. Each seeds a single
        /// over-sized element and asserts the reply arrives whole rather than killing the session.
        /// </summary>
        [TestCase("GET", new[] { "SET|K|V" }, new[] { "GET|K" }, TestName = "Oversized_GET")]
        [TestCase("MGET", new[] { "SET|K|V" }, new[] { "MGET|K" }, TestName = "Oversized_MGET")]
        [TestCase("GETRANGE", new[] { "SET|K|V" }, new[] { "GETRANGE|K|0|-1" }, TestName = "Oversized_GETRANGE")]
        [TestCase("GETDEL", new[] { "SET|K|V" }, new[] { "GETDEL|K" }, TestName = "Oversized_GETDEL")]
        [TestCase("HRANDFIELD", new[] { "HSET|K|V|x" }, new[] { "HRANDFIELD|K" }, TestName = "Oversized_HRANDFIELD")]
        [TestCase("SRANDMEMBER", new[] { "SADD|K|V" }, new[] { "SRANDMEMBER|K" }, TestName = "Oversized_SRANDMEMBER")]
        [TestCase("SPOP", new[] { "SADD|K|V" }, new[] { "SPOP|K" }, TestName = "Oversized_SPOP")]
        [TestCase("LPOP", new[] { "RPUSH|K|V" }, new[] { "LPOP|K" }, TestName = "Oversized_LPOP")]
        [TestCase("ZRANDMEMBER", new[] { "ZADD|K|1|V" }, new[] { "ZRANDMEMBER|K" }, TestName = "Oversized_ZRANDMEMBER")]
        [TestCase("SCAN", new[] { "SET|V|x" }, new[] { "SCAN|0" }, TestName = "Oversized_SCAN")]
        [TestCase("HSCAN", new[] { "HSET|K|V|x" }, new[] { "HSCAN|K|0" }, TestName = "Oversized_HSCAN")]
        [TestCase("SSCAN", new[] { "SADD|K|V" }, new[] { "SSCAN|K|0" }, TestName = "Oversized_SSCAN")]
        [TestCase("ZSCAN", new[] { "ZADD|K|1|V" }, new[] { "ZSCAN|K|0" }, TestName = "Oversized_ZSCAN")]
        [TestCase("COMMAND_GETKEYS", new string[0], new[] { "COMMAND|GETKEYS|GET|V" }, TestName = "Oversized_COMMAND_GETKEYS")]
        [TestCase("CLIENT_LIST", new[] { "CLIENT|SETNAME|V" }, new[] { "CLIENT|LIST" }, TestName = "Oversized_CLIENT_LIST")]
        public void OversizedElementSurvives(string label, string[] setup, string[] probe)
        {
            using var s = Connect();
            var big = new string('x', OversizedElement);

            foreach (var step in setup)
            {
                s.Send(Resp(Array.ConvertAll(step.Split('|'), t => t == "V" ? big : t)));
                var ack = ReadUntil(s, "\r\n");
                StringAssert.DoesNotContain("<connection closed>", ack, $"{label}: setup failed");
            }

            foreach (var step in probe)
                s.Send(Resp(Array.ConvertAll(step.Split('|'), t => t == "V" ? big : t)));

            var reply = ReadUntil(s, big);
            StringAssert.DoesNotContain("<connection closed>", reply, $"{label}: session killed by over-sized element");
            StringAssert.DoesNotContain("<socket error", reply, $"{label}: session killed by over-sized element");
            StringAssert.Contains(big, reply, $"{label}: over-sized element missing from reply");
        }

        /// <summary>
        /// An element that exactly fills the send buffer, and one a single byte over, are the
        /// boundary between the atomic path and the fallback.
        /// </summary>
        [TestCase(128 * 1024 - 32)]
        [TestCase(128 * 1024)]
        [TestCase(128 * 1024 + 1)]
        public void BoundarySizedElementsRoundTrip(int size)
        {
            using var s = Connect();
            var item = new string('b', size);

            s.Send(Resp("RPUSH", "boundary", item));
            StringAssert.StartsWith(":1\r\n", ReadUntil(s, ":1\r\n"));

            s.Send(Resp("LRANGE", "boundary", "0", "-1"));
            var reply = ReadUntil(s, item);

            StringAssert.StartsWith($"*1\r\n${size}\r\n", reply);
            StringAssert.Contains(item, reply);
        }
    }
}