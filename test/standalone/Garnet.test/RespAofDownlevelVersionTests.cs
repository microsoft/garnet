// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Verifies that a down-level (previous release) AOF can be recovered by the current build.
    ///
    /// This PR bumped <see cref="AofHeader.AofHeaderVersion"/> from 4 to 5. The bump only repurposed a
    /// previously-reserved flags bit (0b0100) as the high bit of the header-type mask and added new
    /// <em>chunked</em> header types for large values split across multiple entries. For every
    /// NON-chunked record the on-disk bytes are identical between v4 and v5: the 0b0100 bit is 0, so v5's
    /// 3-bit type mask reads the same header type as v4's 2-bit mask, and no other byte of the entry
    /// changed. A v5 build must therefore transparently recover a v4 AOF.
    ///
    /// There is no v4 build available in-process and the non-chunked writer is byte-for-byte unchanged,
    /// so the faithful way to produce a "v4 AOF" is to write real records with this build (which emits the
    /// exact production non-chunked serialization) and then rewrite each entry's version byte 5 -> 4 on
    /// disk. Recovery then exercises the real v4 -> v5 replay path. If a future version alters the
    /// non-chunked format relative to v4, this test would fail and correctly signal that a dedicated
    /// test-side v4 writer is required.
    /// </summary>
    [TestFixture]
    public class RespAofDownlevelVersionTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task DownlevelV4AofRecoversOnCurrentBuild()
        {
            var zentries = new[]
            {
                new SortedSetEntry("a", 1),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("c", 3),
            };

            // 1. Write a representative mix of non-chunked operations across the string, object, and
            //    unified (expire/delete) replay paths, then durably commit the AOF. Values are kept small
            //    so none of them chunk -- this test only covers the non-chunked v4-compatible format.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, lowMemory: true, failOnRecoveryError: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                db.StringSet("v4:str", "strval");                                             // string upsert
                db.StringSet("v4:ctr", 1);                                                    // string upsert
                _ = db.StringIncrement("v4:ctr", 41);                                         // string RMW -> 42

                db.HashSet("v4:hash", [new HashEntry("f1", "h1"), new HashEntry("f2", "h2")]); // object RMW
                db.SetAdd("v4:set", ["m1", "m2", "m3"]);                                       // object RMW
                db.SortedSetAdd("v4:zset", zentries);                                          // object RMW
                db.ListRightPush("v4:list", ["x", "y", "z"]);                                  // object RMW

                db.KeyExpire("v4:hash", TimeSpan.FromDays(1));                                 // expire (unified)

                db.StringSet("v4:delstr", "tmp");
                db.KeyDelete("v4:delstr");                                                     // string delete

                db.SetAdd("v4:delobj", ["only"]);
                db.KeyDelete("v4:delobj");                                                     // object delete
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);
            server = null;

            // 2. Downgrade every AOF entry's version byte from the current version to 4, simulating an AOF
            //    written by the previous (v4) release. Non-chunked v4 and v5 records are otherwise
            //    byte-identical, so the result is a faithful v4 AOF.
            var downgraded = RewriteAofHeaderVersion(TestUtils.MethodTestDir, from: AofHeader.AofHeaderVersion, to: 4);
            ClassicAssert.Greater(downgraded, 0, "expected to downgrade at least one AOF entry to version 4");

            // 3. Recover with the current build. failOnRecoveryError makes any mishandled v4 entry a crash.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, failOnRecoveryError: true);
            Assert.DoesNotThrow(() => server.Start(), "current build must recover a down-level (v4) AOF");

            // 4. Every committed operation must be replayed correctly.
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                ClassicAssert.AreEqual("strval", (string)db.StringGet("v4:str"));
                ClassicAssert.AreEqual(42, (long)db.StringGet("v4:ctr"));

                ClassicAssert.AreEqual("h1", (string)db.HashGet("v4:hash", "f1"));
                ClassicAssert.AreEqual("h2", (string)db.HashGet("v4:hash", "f2"));
                ClassicAssert.IsTrue(db.KeyExpireTime("v4:hash").HasValue, "expire must survive recovery");

                ClassicAssert.AreEqual(3, db.SetLength("v4:set"));
                ClassicAssert.IsTrue(db.SetContains("v4:set", "m2"));

                ClassicAssert.AreEqual(3, db.SortedSetLength("v4:zset"));
                ClassicAssert.AreEqual(2, db.SortedSetScore("v4:zset", "b"));

                var list = db.ListRange("v4:list");
                ClassicAssert.AreEqual(3, list.Length);
                ClassicAssert.AreEqual("x", (string)list[0]);
                ClassicAssert.AreEqual("y", (string)list[1]);
                ClassicAssert.AreEqual("z", (string)list[2]);

                ClassicAssert.IsFalse(db.KeyExists("v4:delstr"), "deleted string key must stay deleted");
                ClassicAssert.IsFalse(db.KeyExists("v4:delobj"), "deleted object key must stay deleted");
            }
        }

        [Test]
        public async Task UplevelAofVersionIsRejected()
        {
            // The version bump is designed to be fail-safe in the other direction too: a build must refuse
            // an AOF stamped with a header version it does not understand rather than mis-parsing it under
            // an older format. We cannot run a genuinely newer build in-process, so we stamp the AOF with
            // MaxSupportedAofHeaderVersion + 1 and assert recovery fails fast.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, lowMemory: true, failOnRecoveryError: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("uplevel:k1", "v1");
                db.StringSet("uplevel:k2", "v2");
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);
            server = null;

            var stamped = RewriteAofHeaderVersion(TestUtils.MethodTestDir, from: AofHeader.AofHeaderVersion, to: (byte)(AofHeader.MaxSupportedAofHeaderVersion + 1));
            ClassicAssert.Greater(stamped, 0, "expected to stamp at least one AOF entry with an unsupported version");

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, failOnRecoveryError: true);
            var ex = Assert.Catch(() => server.Start(), "recovery must reject an unsupported newer AOF header version");
            StringAssert.Contains("Unsupported AOF header version", GetInnermost(ex).Message);
        }

        private static Exception GetInnermost(Exception ex)
        {
            while (ex.InnerException != null)
                ex = ex.InnerException;
            return ex;
        }

        /// <summary>
        /// Rewrites the version byte of every non-chunked AOF data record from <paramref name="from"/> to
        /// <paramref name="to"/> across all <c>aof.log.*</c> segments, returning the number of records
        /// changed. Uses the same on-disk framing as the torn-tail recovery test: a 4-byte length-only
        /// record header (LogChecksumType.None), a positive length == a data record whose payload begins
        /// with an <see cref="AofHeader"/> (byte 0 == version), and a negative length == a commit record
        /// (left untouched).
        /// </summary>
        private static int RewriteAofHeaderVersion(string checkpointDir, byte from, byte to)
        {
            const int headerSize = 4;   // LogChecksumType.None => 4-byte length-only record header
            var aofDir = Path.Combine(checkpointDir, "AOF");

            var segments = Directory.GetFiles(aofDir, "aof.log.*")
                .OrderBy(f => int.Parse(f[(f.LastIndexOf('.') + 1)..]));

            var changed = 0;
            foreach (var seg in segments)
            {
                int segNum = int.Parse(seg[(seg.LastIndexOf('.') + 1)..]);
                byte[] data = File.ReadAllBytes(seg);

                // Segment 0 starts at LogAddress.FirstValidAddress; higher segments start at file offset 0.
                long offset = segNum == 0 ? Tsavorite.core.LogAddress.FirstValidAddress : 0;
                var dirty = false;

                while (offset + headerSize <= data.Length)
                {
                    int len = BitConverter.ToInt32(data, (int)offset);
                    if (len == 0) break; // zero padding / end of written data
                    int payload = len < 0 ? -len : len;
                    long recSize = headerSize + ((payload + 3) & ~3);
                    if (offset + recSize > data.Length) break;

                    // Positive length => data record; its payload starts with an AofHeader (byte 0 = version).
                    if (len > 0)
                    {
                        var versionPos = (int)offset + headerSize;
                        if (data[versionPos] == from)
                        {
                            data[versionPos] = to;
                            dirty = true;
                            changed++;
                        }
                    }

                    offset += recSize;
                }

                if (dirty)
                    File.WriteAllBytes(seg, data);
            }

            return changed;
        }
    }
}