using System;
using System.Linq;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class DiskANNServiceTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void CreateIndex()
        {
            var index = NativeDiskANNMethods.create_index(0, 0, 0, 0, 0, 0, 0, 0, 0);
            NativeDiskANNMethods.drop_index(0, index);
        }

        [Test]
        public void VADD()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "VALUES", "4", "1.0", "1.0", "1.0", "1.0", new byte[] { 1, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "VALUES", "4", "2.0", "2.0", "2.0", "2.0", new byte[] { 2, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);
        }

        [Test]
        public void VSIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "VALUES", "4", "1.0", "1.0", "1.0", "1.0", new byte[] { 1, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "VALUES", "4", "2.0", "2.0", "2.0", "2.0", new byte[] { 2, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res3 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "4", "0.0", "0.0", "0.0", "0.0", "COUNT", "5", "EF", "128"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 1, 0, 0, 0 })));
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 2, 0, 0, 0 })));

            var res4 = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 1, 0, 0, 0 }, "COUNT", "5", "EF", "128"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 1, 0, 0, 0 })));
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 2, 0, 0, 0 })));
        }
    }
}