// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Calls <see cref="IGarnetApi"/> methods with arguments they must reject, replying OK if each call reports the rejection
    /// through its error output, or an error naming the first call that does not.
    /// </summary>
    /// <remarks>
    /// Format: APIERRORS operation arg [arg ...], with the arguments of each operation listed on its check method.
    /// </remarks>
    sealed class ApiErrorsProc : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var operation = Encoding.ASCII.GetString(GetNextArg(ref procInput, ref offset).ReadOnlySpan);

            var args = new List<PinnedSpanByte>();
            PinnedSpanByte arg;
            while ((arg = GetNextArg(ref procInput, ref offset)).Length > 0)
                args.Add(arg);

            var failedCall = operation switch
            {
                "INCR" => CheckIncrement(api, args),
                "INCRBYFLOAT" => CheckIncrementByFloat(api, args),
                "ZINCRBY" => CheckSortedSetIncrement(api, args),
                "EXPIRE" => CheckExpire(api, args),
                "EXPIREMS" => CheckExpireInMilliseconds(api, args),
                "SETEX" => CheckSetWithExpiry(api, args),
                "APPEND" => CheckAppend(api, args),
                "APPENDNEW" => CheckAppendCreatesNoOversizedValue(api, args),
                _ => $"Unknown operation {operation}",
            };

            if (failedCall == null)
                return true;

            WriteError(ref output, $"ERR {failedCall}");
            return false;
        }

        /// <summary>
        /// Arguments: a key holding "abc", a key holding long.MaxValue, a key holding long.MinValue, two missing keys
        /// </summary>
        static string CheckIncrement<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            if (api.Increment(args[0], out _, out var error) != GarnetStatus.OK || !error)
                return "Increment of a value that is not an integer";
            if (api.Increment(args[1], out _, out error) != GarnetStatus.OK || !error)
                return "Increment past long.MaxValue";
            if (api.Decrement(args[2], out _, out error) != GarnetStatus.OK || !error)
                return "Decrement past long.MinValue";
            if (api.Decrement(args[3], out _, out error, long.MinValue) != GarnetStatus.OK || !error)
                return "Decrement by long.MinValue";
            if (api.Increment(args[4], out var value, out error, long.MinValue) != GarnetStatus.OK || error || value != long.MinValue)
                return "Increment by long.MinValue";
            return null;
        }

        /// <summary>
        /// Arguments: a key holding "abc", a key holding "1.5", a key holding "1e308", a missing key
        /// </summary>
        static string CheckIncrementByFloat<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            if (api.IncrementByFloat(args[0], out _, out var error, 1) != GarnetStatus.OK || !error)
                return "IncrementByFloat of a value that is not a float";
            if (api.IncrementByFloat(args[1], out var value, out error, 1) != GarnetStatus.OK || error || value != 2.5)
                return "IncrementByFloat of a float";
            if (api.IncrementByFloat(args[1], out _, out error, double.PositiveInfinity) != GarnetStatus.OK || !error)
                return "IncrementByFloat by infinity";
            if (api.IncrementByFloat(args[2], out _, out error, 1e308) != GarnetStatus.OK || !error)
                return "IncrementByFloat past double.MaxValue";
            if (api.IncrementByFloat(args[3], out _, out error, double.NaN) != GarnetStatus.OK || !error)
                return "IncrementByFloat by NaN";
            return null;
        }

        /// <summary>
        /// Arguments: a sorted set key, a member, a missing key
        /// </summary>
        static string CheckSortedSetIncrement<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            if (api.SortedSetIncrement(args[0], double.PositiveInfinity, args[1], out var score, out var error) != GarnetStatus.OK || error || score != double.PositiveInfinity)
                return "SortedSetIncrement to infinity";
            if (api.SortedSetIncrement(args[0], double.NegativeInfinity, args[1], out _, out error) != GarnetStatus.OK || !error)
                return "SortedSetIncrement to NaN";
            if (api.SortedSetIncrement(args[0], double.NaN, args[1], out _, out error) != GarnetStatus.OK || !error)
                return "SortedSetIncrement by NaN";
            if (api.SortedSetIncrement(args[2], double.NaN, args[1], out _, out error) != GarnetStatus.OK || !error)
                return "SortedSetIncrement of a missing key by NaN";
            return null;
        }

        /// <summary>
        /// Arguments: a key, "abc", long.MaxValue
        /// </summary>
        static string CheckExpire<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            if (api.EXPIRE(args[0], args[1], out var timeoutSet, out var error) != GarnetStatus.OK || !error || timeoutSet)
                return "EXPIRE by milliseconds that are not an integer";
            if (api.EXPIRE(args[0], args[2], out timeoutSet, out error) != GarnetStatus.OK || !error || timeoutSet)
                return "EXPIRE by long.MaxValue milliseconds";
            if (api.EXPIRE(args[0], TimeSpan.MaxValue, out timeoutSet, out error) != GarnetStatus.OK || !error || timeoutSet)
                return "EXPIRE by TimeSpan.MaxValue";
            if (api.EXPIREAT(args[0], long.MaxValue, out timeoutSet, out error) != GarnetStatus.OK || !error || timeoutSet)
                return "EXPIREAT long.MaxValue";
            if (api.PEXPIREAT(args[0], long.MaxValue, out timeoutSet, out error) != GarnetStatus.OK || !error || timeoutSet)
                return "PEXPIREAT long.MaxValue";
            return null;
        }

        /// <summary>
        /// Arguments: two keys, "500"
        /// </summary>
        static string CheckExpireInMilliseconds<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            if (api.EXPIRE(args[0], args[2], out var timeoutSet, out var error) != GarnetStatus.OK || error || !timeoutSet)
                return "EXPIRE by 500 milliseconds";
            if (api.EXPIRE(args[1], TimeSpan.FromMilliseconds(500), out timeoutSet, out error) != GarnetStatus.OK || error || !timeoutSet)
                return "EXPIRE by a TimeSpan of 500 milliseconds";
            return null;
        }

        /// <summary>
        /// Arguments: a missing key, a value, then expiries in milliseconds that SETEX must reject
        /// </summary>
        static string CheckSetWithExpiry<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            for (var i = 2; i < args.Count; i++)
            {
                if (api.SETEX(args[0], args[1], args[i], out var error) != GarnetStatus.OK || !error)
                    return $"SETEX by {Encoding.ASCII.GetString(args[i].ReadOnlySpan)} milliseconds";
            }

            foreach (var expiry in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), TimeSpan.MaxValue })
            {
                if (api.SETEX(args[0], args[1], expiry, out var error) != GarnetStatus.OK || !error)
                    return $"SETEX by a TimeSpan of {expiry}";
            }

            return null;
        }

        /// <summary>
        /// Arguments: a key holding a value of the maximum string size, a value to append
        /// </summary>
        static string CheckAppend<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            Span<byte> lengthBuffer = stackalloc byte[32];
            var length = PinnedSpanByte.FromPinnedSpan(lengthBuffer);
            if (api.APPEND(args[0], args[1], ref length, out var error) != GarnetStatus.OK || !error)
                return "APPEND past the maximum string size";
            return null;
        }

        /// <summary>
        /// Arguments: keys that are missing or expired
        /// </summary>
        static string CheckAppendCreatesNoOversizedValue<TGarnetApi>(TGarnetApi api, List<PinnedSpanByte> args)
            where TGarnetApi : IGarnetApi
        {
            // Only the length matters, so the contents are left uninitialized
            var value = GC.AllocateUninitializedArray<byte>(BitmapManager.MaxBitmapPayloadBytes + 1, pinned: true);
            Span<byte> lengthBuffer = stackalloc byte[32];
            foreach (var key in args)
            {
                var length = PinnedSpanByte.FromPinnedSpan(lengthBuffer);
                if (api.APPEND(key, PinnedSpanByte.FromPinnedSpan(value), ref length, out var error) != GarnetStatus.OK || !error)
                    return $"APPEND of a new value past the maximum string size to {Encoding.ASCII.GetString(key.ReadOnlySpan)}";
            }

            return null;
        }
    }

    /// <summary>
    /// Sets a key and then a timeout on it, replying with an error if the timeout is rejected; the set is not undone.
    /// </summary>
    /// <remarks>
    /// Format: SETTHENEXPIRE key value milliseconds
    /// </remarks>
    sealed class SetThenExpireTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput, ref offset);
            var value = GetNextArg(ref procInput, ref offset);
            var milliseconds = GetNextArg(ref procInput, ref offset);

            _ = api.SET(key, value);
            _ = api.EXPIRE(key, milliseconds, out _, out var error);
            if (error)
                WriteError(ref output, "ERR invalid expire time in 'setthenexpire' command");
            else
                WriteSimpleString(ref output, "OK");
        }
    }

    /// <summary>
    /// Tests that <see cref="IGarnetApi"/> methods report arguments and stored values they reject through an error output,
    /// leaving the store unchanged, instead of throwing or silently writing a wrong value.
    /// </summary>
    [TestFixture]
    public class GarnetApiErrorTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Register.NewProcedure("APIERRORS", () => new ApiErrorsProc());
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void IncrementApisReportErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("notAnInteger", "abc");
            _ = db.StringSet("int64Max", long.MaxValue);
            _ = db.StringSet("int64Min", long.MinValue);

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "INCR", "notAnInteger", "int64Max", "int64Min", "missing", "created"));

            ClassicAssert.AreEqual("abc", (string)db.StringGet("notAnInteger"));
            ClassicAssert.AreEqual(long.MaxValue, (long)db.StringGet("int64Max"));
            ClassicAssert.AreEqual(long.MinValue, (long)db.StringGet("int64Min"));
            ClassicAssert.IsFalse(db.KeyExists("missing"));
            ClassicAssert.AreEqual(long.MinValue, (long)db.StringGet("created"));
        }

        [Test]
        public void IncrementByFloatApiReportsErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("notAFloat", "abc");
            _ = db.StringSet("float", "1.5");
            _ = db.StringSet("large", "1e308");

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "INCRBYFLOAT", "notAFloat", "float", "large", "missing"));

            ClassicAssert.AreEqual("abc", (string)db.StringGet("notAFloat"));
            ClassicAssert.AreEqual("2.5", (string)db.StringGet("float"));
            ClassicAssert.AreEqual("1e308", (string)db.StringGet("large"));
            ClassicAssert.IsFalse(db.KeyExists("missing"));
        }

        [Test]
        public void SortedSetIncrementApiReportsErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.SortedSetAdd("zset", "other", 1);

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "ZINCRBY", "zset", "member", "missing"));

            ClassicAssert.AreEqual(double.PositiveInfinity, db.SortedSetScore("zset", "member"));
            ClassicAssert.AreEqual(2, db.SortedSetLength("zset"));
            ClassicAssert.IsFalse(db.KeyExists("missing"));
        }

        [Test]
        public void ExpireApisReportErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("key", "value");

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "EXPIRE", "key", "abc", long.MaxValue.ToString()));

            ClassicAssert.AreEqual("value", (string)db.StringGet("key"));
            ClassicAssert.IsNull(db.KeyTimeToLive("key"));
        }

        /// <summary>
        /// The expiry is set to the millisecond, so a timeout of less than a second does not expire the key immediately.
        /// </summary>
        [Test]
        public void ExpireApisKeepMilliseconds()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("key1", "value");
            _ = db.StringSet("key2", "value");

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "EXPIREMS", "key1", "key2", "500"));

            foreach (var key in new[] { "key1", "key2" })
            {
                var ttl = (long)db.Execute("PTTL", key);
                ClassicAssert.Greater(ttl, 0, key);
                ClassicAssert.LessOrEqual(ttl, 500, key);
            }
        }

        [Test]
        public void SetWithExpiryApisReportErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "SETEX", "missing", "value", "abc", "0", "-1", long.MaxValue.ToString()));

            ClassicAssert.IsFalse(db.KeyExists("missing"));
        }

        [Test]
        public void AppendApiReportsValueTooLarge()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Build a value of exactly the maximum size on the server, without sending it over the wire
            var maxSize = BitmapManager.MaxBitmapPayloadBytes;
            ClassicAssert.AreEqual(maxSize, (long)db.Execute("SETRANGE", "maxSized", maxSize - 1, "x"));

            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", "APPEND", "maxSized", "y"));

            ClassicAssert.AreEqual(maxSize, db.StringLength("maxSized"));
        }

        /// <summary>
        /// APPEND does not create a value past the maximum string size, for a missing key or an expired one.
        /// </summary>
        [Test]
        public void AppendApiRejectsOversizedNewValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;

            // One expired key is copy-updated from the read-only region, the other updated in place
            _ = db.StringSet("expiredReadOnly", "v", TimeSpan.FromMilliseconds(10));
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            _ = db.StringSet("expiredMutable", "v", TimeSpan.FromMilliseconds(10));
            Thread.Sleep(100);

            string[] keys = ["missing", "expiredMutable", "expiredReadOnly"];
            ClassicAssert.AreEqual("OK", (string)db.Execute("APIERRORS", ["APPENDNEW", .. keys]));

            foreach (var key in keys)
                ClassicAssert.IsFalse(db.KeyExists(key), key);
        }

        /// <summary>
        /// A transaction procedure that reports a rejected call still logs its earlier writes to the AOF, so they are recovered.
        /// </summary>
        [Test]
        public async Task TransactionProcWritesBeforeARejectedCallAreRecovered()
        {
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Register.NewTransactionProc("SETTHENEXPIRE", () => new SetThenExpireTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var ex = Assert.Throws<RedisServerException>(() => db.Execute("SETTHENEXPIRE", "key", "value", long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'setthenexpire' command", ex.Message);
                ClassicAssert.AreEqual("value", (string)db.StringGet("key"));
                ClassicAssert.IsNull(db.KeyTimeToLive("key"));
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Register.NewTransactionProc("SETTHENEXPIRE", () => new SetThenExpireTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("value", (string)db.StringGet("key"));
                ClassicAssert.IsNull(db.KeyTimeToLive("key"));
            }
        }
    }
}