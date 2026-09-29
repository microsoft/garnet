// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Pins <c>total_output_buffer_rentals</c>, the <c>INFO BPSTATS</c> counter that makes
    /// response-size overflow observable rather than inferred.
    /// <para>
    /// The fixture configures no metrics monitor, so a non-zero reading here also demonstrates that
    /// the counter is read live from its source rather than sampled, which is what the rest of the
    /// buffer pool section does and what STATS deliberately does not.
    /// </para>
    /// </summary>
    [TestFixture]
    public class OutputBufferRentalTests : TestBase
    {
        const int OversizedResponses = 10;
        const int OversizedValueBytes = 512 * 1024;

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        static long Rentals(ConnectionMultiplexer redis)
        {
            var info = redis.GetServer(TestUtils.EndPoint).Info("bpstats");
            var row = info.SelectMany(g => g)
                          .FirstOrDefault(kv => kv.Key == "total_output_buffer_rentals");
            ClassicAssert.IsNotNull(row.Key, "INFO BPSTATS did not carry total_output_buffer_rentals");
            return long.Parse(row.Value);
        }

        [Test]
        public void ResponsesThatFitTheirBufferRentNothing()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var before = Rentals(redis);

            for (var i = 0; i < 50; i++)
                _ = db.StringSet($"s{i}", "small-value");
            for (var i = 0; i < 50; i++)
                _ = db.StringGet($"s{i}");

            for (var i = 0; i < 50; i++)
                db.HashSet("h", [new HashEntry($"f{i}", "v")]);
            for (var i = 0; i < 50; i++)
                _ = db.HashGetAll("h");

            ClassicAssert.AreEqual(before, Rentals(redis),
                "an ordinary small response rented an overflow buffer, so the counter is measuring " +
                "something other than response-size overflow");
        }

        [Test]
        public void EachOversizedResponseRentsExactlyOneBuffer()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            db.HashSet("big", [new HashEntry("f", new string('x', OversizedValueBytes))]);

            var before = Rentals(redis);
            for (var i = 0; i < OversizedResponses; i++)
                _ = db.HashGetAll("big");
            var rented = Rentals(redis) - before;

            // Guards vacuity: if the response stopped overflowing, "at most one" would hold trivially.
            ClassicAssert.GreaterOrEqual(rented, OversizedResponses,
                $"{OversizedResponses} oversized responses rented only {rented} buffers, so this run " +
                "never exercised the overflow path the counter exists to observe");

            // The substantive claim: ReallocateOutput sizes from the length hint in a single jump,
            // so an oversized response costs one rental and a copy rather than a doubling ladder.
            ClassicAssert.LessOrEqual(rented, OversizedResponses,
                $"{OversizedResponses} oversized responses rented {rented} buffers, so the response " +
                "buffer is growing by repeated doubling instead of jumping straight to the hinted size");
        }
    }
}